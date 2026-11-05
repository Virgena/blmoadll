//! A plugin that does nothing but speak the wire protocol.
//!
//! It is the kernel integration tests' test double, built only with
//! `--features fixture`, so a default build never produces a binary.
//!
//!     blmoadll-fixture --provides demo.text [options]
//!
//! Options: `--injects <capability>` (repeatable), `--register <service> <capability>`,
//! `--host-call <capability>`, `--chunks <n>`,
//! `--exit-on-start`, `--exit-on-invoke`, `--exit-if <path>`.
//!
//! `--host` instead pretends to be the shared extension host for every plugin
//! on one pipe:
//!
//!     blmoadll-fixture --host --plugins a:demo.one,b:demo.two [options]
//!
//! Options: `--crash-on-load <id>`, `--crash-on-invoke <id>`,
//! `--exit-plugin-on-invoke <id>`, `--blame <id>`, `--crash-file <path>`.
//! A crash exits `9`; a blamed crash announces `$/host/loading` first, so the
//! kernel has a culprit to count a strike against.
//!
//! `--runtime` instead pretends to be the Runtime that owns every plugin:
//!
//!     blmoadll-fixture --runtime --plugins a:demo.one,b:demo.two [options]
//!
//! Options: `--quarantined <id>`, `--failed`, `--bad-version`, `--no-register`,
//! `--exit-after-ready`, `--chunks <n>`.

use std::collections::BTreeMap;
use std::process::ExitCode;

use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufReader};

use protocol as proto;
use protocol::Incoming;

const MAX_FRAME_BYTES: usize = 1 << 20;
const CRASH_CODE: u8 = 9;

#[derive(Default)]
struct Args {
    provides: Vec<String>,
    injects: Vec<Value>,
    registrations: Vec<Value>,
    host_calls: Vec<String>,
    chunks: usize,
    exit_on_start: bool,
    exit_on_invoke: bool,
    exit_if: Option<String>,
}

fn parse() -> Args {
    let mut args = Args::default();
    let mut rest = std::env::args().skip(1);
    while let Some(flag) = rest.next() {
        match flag.as_str() {
            "--provides" => {
                if let Some(capability) = rest.next() {
                    args.provides.push(capability);
                }
            }
            "--injects" => {
                if let Some(capability) = rest.next() {
                    args.injects.push(json!({
                        "capability": capability,
                        "optional": false,
                    }));
                }
            }
            "--register" => {
                if let (Some(service), Some(capability)) = (rest.next(), rest.next()) {
                    args.registrations.push(json!({
                        "service": service,
                        "capability": capability,
                    }));
                }
            }
            "--host-call" => {
                if let Some(capability) = rest.next() {
                    args.host_calls.push(capability);
                }
            }
            "--chunks" => args.chunks = rest.next().and_then(|n| n.parse().ok()).unwrap_or(0),
            "--exit-on-start" => args.exit_on_start = true,
            "--exit-on-invoke" => args.exit_on_invoke = true,
            "--exit-if" => args.exit_if = rest.next(),
            _ => {}
        }
    }
    args
}

#[derive(Default)]
struct HostArgs {
    provides: BTreeMap<String, Vec<String>>,
    crash_on_load: Option<String>,
    crash_on_invoke: Option<String>,
    exit_on_invoke: Option<String>,
    blame: Option<String>,
    crash_file: Option<String>,
}

fn parse_host(rest: Vec<String>) -> HostArgs {
    let mut args = HostArgs::default();
    let mut it = rest.into_iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--plugins" => {
                for item in it.next().unwrap_or_default().split(',') {
                    let mut parts = item.splitn(2, ':');
                    if let (Some(id), Some(capability)) = (parts.next(), parts.next()) {
                        args.provides
                            .insert(id.to_string(), vec![capability.to_string()]);
                    }
                }
            }
            "--crash-on-load" => args.crash_on_load = it.next(),
            "--crash-on-invoke" => args.crash_on_invoke = it.next(),
            "--exit-plugin-on-invoke" => args.exit_on_invoke = it.next(),
            "--blame" => args.blame = it.next(),
            "--crash-file" => args.crash_file = it.next(),
            _ => {}
        }
    }
    args
}

#[derive(Default)]
struct RuntimeArgs {
    plugins: BTreeMap<String, String>,
    state: String,
    plugin: Option<String>,
    bad_version: bool,
    no_register: bool,
    exit_after_ready: bool,
    chunks: usize,
}

fn parse_runtime(rest: Vec<String>) -> RuntimeArgs {
    let mut args = RuntimeArgs::default();
    let mut it = rest.into_iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--plugins" => {
                for item in it.next().unwrap_or_default().split(',') {
                    let mut parts = item.splitn(2, ':');
                    if let (Some(id), Some(capability)) = (parts.next(), parts.next()) {
                        if !id.is_empty() && !capability.is_empty() {
                            args.plugins.insert(id.to_string(), capability.to_string());
                        }
                    }
                }
            }
            "--quarantined" => {
                args.state = "quarantined".to_string();
                args.plugin = it.next();
            }
            "--failed" => args.state = "failed".to_string(),
            "--bad-version" => args.bad_version = true,
            "--no-register" => args.no_register = true,
            "--exit-after-ready" => args.exit_after_ready = true,
            "--chunks" => args.chunks = it.next().and_then(|n| n.parse().ok()).unwrap_or(1),
            _ => {}
        }
    }
    args
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let rest: Vec<String> = std::env::args().skip(1).collect();
    if rest.iter().any(|flag| flag == "--runtime") {
        let runtime = parse_runtime(
            rest.into_iter()
                .filter(|flag| flag != "--runtime")
                .collect(),
        );
        return run_runtime(runtime).await;
    }
    if rest.iter().any(|flag| flag == "--host") {
        let host = parse_host(rest.into_iter().filter(|flag| flag != "--host").collect());
        return run_host(host).await;
    }
    let args = parse();
    if args
        .exit_if
        .as_deref()
        .is_some_and(|path| std::path::Path::new(path).exists())
    {
        return ExitCode::from(9);
    }
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut out = tokio::io::stdout();

    loop {
        match proto::read_frame(&mut reader, MAX_FRAME_BYTES).await {
            // stdin closed: the contract's exit path, and the only thing this
            // test double needs to do to be a well-behaved plugin.
            Ok(None) | Err(_) => return ExitCode::SUCCESS,
            Ok(Some(payload)) => {
                let Ok(incoming) = proto::parse_frame(&payload) else {
                    continue;
                };
                // Notifications are not requests, so they cannot go through the
                // match below - but the test double still says what it heard, on
                // stderr, where the kernel's log picks it up: that is how the
                // process-level tests prove a cancel reached a provider.
                if let Incoming::Notification { method, params } = &incoming {
                    if method.as_str() == proto::method::CANCEL {
                        let id = params
                            .get("request_id")
                            .map(|value| value.to_string())
                            .unwrap_or_default();
                        eprintln!("fixture: cancelled request={id}");
                    }
                    continue;
                }
                let Incoming::Request { id, method, params } = incoming else {
                    continue;
                };
                match method.as_str() {
                    proto::method::INITIALIZE => {
                        let reply = json!({
                            "protocol": proto::PROTOCOL_VERSION,
                            "provides": args.provides,
                            "injects": args.injects,
                            "registrations": args.registrations,
                            "host_calls": args.host_calls,
                        });
                        send(&mut out, &proto::success(id, reply)).await;
                    }
                    proto::method::START => {
                        send(&mut out, &proto::success(id, json!({}))).await;
                        if args.exit_on_start {
                            return ExitCode::from(3);
                        }
                    }
                    proto::method::SHUTDOWN => {
                        // The reason the kernel was given, echoed on stderr: the
                        // same way the tests see which reason a host really sent.
                        let reason = params.get("reason").and_then(Value::as_str).unwrap_or("?");
                        eprintln!("fixture: shutdown reason={reason}");
                        send(&mut out, &proto::success(id, json!({}))).await;
                        return ExitCode::SUCCESS;
                    }
                    proto::method::INVOKE => {
                        if args.exit_on_invoke {
                            return ExitCode::from(7);
                        }
                        let inner = params.get("params").cloned().unwrap_or(Value::Null);
                        let streaming = params
                            .get("meta")
                            .and_then(|meta| meta.get("stream"))
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        if !streaming {
                            send(&mut out, &proto::success(id, json!({ "got": inner }))).await;
                            continue;
                        }
                        // The reply comes first: the kernel wires the mapping
                        // from it, so chunks before it would be orphans.
                        send(&mut out, &proto::success(id, json!({ "stream_id": "s-1" }))).await;
                        for n in 0..args.chunks {
                            let chunk = json!({
                                "stream_id": "s-1",
                                "seq": n,
                                "data": { "delta": format!("c{n}") },
                                "done": false,
                            });
                            send(&mut out, &proto::notify(proto::method::STREAM_CHUNK, chunk))
                                .await;
                        }
                        let terminal = json!({
                            "stream_id": "s-1",
                            "seq": args.chunks,
                            "data": null,
                            "done": true,
                        });
                        send(
                            &mut out,
                            &proto::notify(proto::method::STREAM_CHUNK, terminal),
                        )
                        .await;
                    }
                    other => {
                        let error = proto::RpcError::new(-32601, format!("no {other} here"));
                        send(&mut out, &proto::failure(id, &error)).await;
                    }
                }
            }
        }
    }
}

async fn send(out: &mut tokio::io::Stdout, frame: &Value) {
    let body = serde_json::to_vec(frame).unwrap_or_else(|_| b"null".to_vec());
    let _ = proto::write_frame(out, &body).await;
    let _ = out.flush().await;
}

async fn send_tagged(out: &mut tokio::io::Stdout, frame: &Value, plugin: Option<&str>) {
    let mut frame = frame.clone();
    if let Some(plugin) = plugin {
        frame["plugin"] = Value::String(plugin.to_string());
    }
    send(out, &frame).await;
}

/// The host half: every plugin on one pipe, tagged frames, and a crash on
/// demand so the kernel's supervision can be tested without Node.
async fn run_host(args: HostArgs) -> ExitCode {
    let mut reader = BufReader::new(tokio::io::stdin());
    let mut out = tokio::io::stdout();
    let mut gone: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    loop {
        match proto::read_frame(&mut reader, MAX_FRAME_BYTES).await {
            Ok(None) | Err(_) => return ExitCode::SUCCESS,
            Ok(Some(payload)) => {
                let Ok((plugin, incoming)) = proto::parse_tagged(&payload) else {
                    continue;
                };
                let crashes = |path: &Option<String>| {
                    path.as_deref()
                        .is_some_and(|path| std::path::Path::new(path).exists())
                };
                match (plugin, incoming) {
                    (Some(plugin), Incoming::Request { id, method, params }) => {
                        if gone.contains(&plugin) {
                            continue;
                        }
                        let provides = args.provides.get(&plugin).cloned().unwrap_or_default();
                        match method.as_str() {
                            proto::method::INITIALIZE => {
                                let reply = json!({
                                    "protocol": proto::PROTOCOL_VERSION,
                                    "provides": provides,
                                    "injects": [],
                                    "registrations": [],
                                    "host_calls": [],
                                });
                                send_tagged(&mut out, &proto::success(id, reply), Some(&plugin))
                                    .await;
                            }
                            proto::method::START => {
                                send_tagged(
                                    &mut out,
                                    &proto::success(id, json!({})),
                                    Some(&plugin),
                                )
                                .await;
                            }
                            proto::method::SHUTDOWN => {
                                send_tagged(
                                    &mut out,
                                    &proto::success(id, json!({})),
                                    Some(&plugin),
                                )
                                .await;
                                send_tagged(
                                    &mut out,
                                    &proto::notify(
                                        proto::method::HOST_EXITED,
                                        json!({ "plugin": plugin, "exit_code": 0, "signal": null }),
                                    ),
                                    None,
                                )
                                .await;
                            }
                            proto::method::INVOKE => {
                                if args.crash_on_invoke.as_deref() == Some(plugin.as_str()) {
                                    return ExitCode::from(CRASH_CODE);
                                }
                                if args.exit_on_invoke.as_deref() == Some(plugin.as_str()) {
                                    send_tagged(
                                        &mut out,
                                        &proto::notify(
                                            proto::method::HOST_EXITED,
                                            json!({ "plugin": plugin, "exit_code": 7, "signal": null }),
                                        ),
                                        None,
                                    )
                                    .await;
                                    gone.insert(plugin);
                                    continue;
                                }
                                let inner = params.get("params").cloned().unwrap_or(Value::Null);
                                send_tagged(
                                    &mut out,
                                    &proto::success(id, json!({ "got": inner })),
                                    Some(&plugin),
                                )
                                .await;
                            }
                            other => {
                                let error =
                                    proto::RpcError::new(-32601, format!("no {other} here"));
                                send_tagged(&mut out, &proto::failure(id, &error), Some(&plugin))
                                    .await;
                            }
                        }
                    }
                    (None, Incoming::Request { id, method, params }) => {
                        let plugin = params
                            .get("plugin")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        match method.as_str() {
                            proto::method::HOST_LOAD => {
                                if args.crash_on_load.as_deref() == Some(plugin.as_str()) {
                                    send_tagged(
                                        &mut out,
                                        &proto::notify(
                                            proto::method::HOST_LOADING,
                                            json!({ "plugin": plugin }),
                                        ),
                                        None,
                                    )
                                    .await;
                                    return ExitCode::from(CRASH_CODE);
                                }
                                send_tagged(&mut out, &proto::success(id, json!({})), None).await;
                            }
                            proto::method::HOST_UNLOAD => {
                                send_tagged(&mut out, &proto::success(id, json!({})), None).await;
                            }
                            proto::method::HOST_KILL => {
                                send_tagged(&mut out, &proto::success(id, json!({})), None).await;
                                send_tagged(
                                    &mut out,
                                    &proto::notify(
                                        proto::method::HOST_EXITED,
                                        json!({
                                            "plugin": plugin,
                                            "exit_code": null,
                                            "signal": null,
                                            "killed": true,
                                        }),
                                    ),
                                    None,
                                )
                                .await;
                            }
                            proto::method::HOST_PING => {
                                if crashes(&args.crash_file) {
                                    if let Some(blame) = &args.blame {
                                        send_tagged(
                                            &mut out,
                                            &proto::notify(
                                                proto::method::HOST_LOADING,
                                                json!({ "plugin": blame }),
                                            ),
                                            None,
                                        )
                                        .await;
                                    }
                                    return ExitCode::from(CRASH_CODE);
                                }
                                send_tagged(
                                    &mut out,
                                    &proto::success(id, json!({ "pong": true })),
                                    None,
                                )
                                .await;
                            }
                            other => {
                                let error =
                                    proto::RpcError::new(-32601, format!("no {other} here"));
                                send_tagged(&mut out, &proto::failure(id, &error), None).await;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// The runtime half: ready, one capability snapshot, and the front end's
/// calls answered without a plugin behind them.
async fn run_runtime(args: RuntimeArgs) -> ExitCode {
    let state = if args.state.is_empty() {
        "ready".to_string()
    } else {
        args.state.clone()
    };
    let total = args.plugins.len();
    let ready = if state == "ready" { total } else { 0 };
    let table: serde_json::Map<String, Value> = args
        .plugins
        .iter()
        .map(|(id, capability)| (capability.clone(), json!({ "plugin": id })))
        .collect();
    let plugins: Vec<Value> = args
        .plugins
        .iter()
        .map(|(id, _)| json!({ "id": id, "state": state.clone() }))
        .collect();
    let mut out = tokio::io::stdout();

    if !args.no_register {
        let version = if args.bad_version { 2 } else { 1 };
        send(
            &mut out,
            &proto::notify(proto::method::RUNTIME_READY, json!({ "v": version })),
        )
        .await;
        if args.bad_version {
            return ExitCode::from(5);
        }
        send(
            &mut out,
            &proto::notify(
                proto::method::RUNTIME_CAPABILITIES,
                json!({ "v": 1, "table": table.clone(), "plugins": plugins }),
            ),
        )
        .await;
        if args.exit_after_ready {
            return ExitCode::from(3);
        }
    }

    let mut reader = BufReader::new(tokio::io::stdin());
    loop {
        match proto::read_frame(&mut reader, MAX_FRAME_BYTES).await {
            Ok(None) | Err(_) => return ExitCode::SUCCESS,
            Ok(Some(payload)) => {
                let Ok(incoming) = proto::parse_frame(&payload) else {
                    continue;
                };
                match incoming {
                    Incoming::Notification { method, .. } => {
                        eprintln!("fixture runtime: {method}");
                    }
                    Incoming::Request { id, method, params } => match method.as_str() {
                        "capabilities" => {
                            let reply = Value::Object(table.clone());
                            send(&mut out, &proto::success(id, reply)).await;
                        }
                        "status" => {
                            let reply = json!({
                                "state": state,
                                "ready": ready,
                                "total": total,
                                "plugin": args.plugin.clone(),
                            });
                            send(&mut out, &proto::success(id, reply)).await;
                        }
                        "invoke" => {
                            let inner = params.get("params").cloned().unwrap_or(Value::Null);
                            let streaming = params
                                .get("meta")
                                .and_then(|meta| meta.get("stream"))
                                .and_then(Value::as_bool)
                                .unwrap_or(false);
                            if !streaming {
                                send(&mut out, &proto::success(id, json!({ "got": inner }))).await;
                                continue;
                            }
                            let stream_id = params
                                .get("meta")
                                .and_then(|meta| meta.get("stream_id"))
                                .and_then(Value::as_str)
                                .unwrap_or("f-1")
                                .to_string();
                            send(
                                &mut out,
                                &proto::success(id, json!({ "stream_id": stream_id })),
                            )
                            .await;
                            let count = args.chunks.max(1);
                            for n in 0..count {
                                let chunk = json!({
                                    "stream_id": stream_id,
                                    "seq": n,
                                    "data": { "delta": format!("c{n}") },
                                    "done": false,
                                });
                                send(&mut out, &proto::notify(proto::method::STREAM_CHUNK, chunk))
                                    .await;
                            }
                            let terminal = json!({
                                "stream_id": stream_id,
                                "seq": count,
                                "data": null,
                                "done": true,
                            });
                            send(
                                &mut out,
                                &proto::notify(proto::method::STREAM_CHUNK, terminal),
                            )
                            .await;
                        }
                        "restart" => {
                            let plugin = params.get("plugin").and_then(Value::as_str).unwrap_or("");
                            if args.plugins.contains_key(plugin) {
                                send(&mut out, &proto::success(id, json!({}))).await;
                            } else {
                                let error = proto::RpcError::with_data(
                                    -32011,
                                    "the plugin never comes back",
                                    json!({ "plugin": plugin }),
                                );
                                send(&mut out, &proto::failure(id, &error)).await;
                            }
                        }
                        "shutdown" => {
                            let reason =
                                params.get("reason").and_then(Value::as_str).unwrap_or("?");
                            eprintln!("fixture runtime: shutdown reason={reason}");
                            send(&mut out, &proto::success(id, json!({}))).await;
                            return ExitCode::SUCCESS;
                        }
                        other => {
                            let error = proto::RpcError::new(-32601, format!("no {other} here"));
                            send(&mut out, &proto::failure(id, &error)).await;
                        }
                    },
                    _ => {}
                }
            }
        }
    }
}
