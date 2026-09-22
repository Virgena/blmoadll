//! A plugin that does nothing but speak the wire protocol.
//!
//! It is the kernel integration tests' test double, built only with
//! `--features fixture`, so a default build never produces a binary.
//!
//!     eggshell-fixture --provides demo.text=1.0.0 [options]
//!
//! Options: `--requires <cap>=<range>` (repeatable), `--chunks <n>`,
//! `--exit-on-start`, `--exit-on-invoke`, `--exit-if <path>`.

use std::process::ExitCode;

use serde_json::{json, Value};
use tokio::io::{AsyncWriteExt, BufReader};

use protocol as proto;
use protocol::Incoming;

const MAX_FRAME_BYTES: usize = 1 << 20;

#[derive(Default)]
struct Args {
    provides: Vec<(String, String)>,
    requires: Vec<Value>,
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
                if let Some((capability, version)) = rest.next().as_deref().and_then(split_spec) {
                    args.provides.push((capability.to_string(), version.to_string()));
                }
            }
            "--requires" => {
                if let Some((capability, range)) = rest.next().as_deref().and_then(split_spec) {
                    args.requires.push(json!({
                        "capability": capability,
                        "version": range,
                        "optional": false,
                    }));
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

fn split_spec(spec: &str) -> Option<(&str, &str)> {
    spec.split_once('=')
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = parse();
    if args.exit_if.as_deref().is_some_and(|path| std::path::Path::new(path).exists()) {
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
                let Ok(incoming) = proto::parse_frame(&payload) else { continue };
                // Notifications are not requests, so they cannot go through the
                // match below - but the test double still says what it heard, on
                // stderr, where the kernel's log picks it up: that is how the
                // process-level tests prove a cancel reached a provider.
                if let Incoming::Notification { method, params } = &incoming {
                    if method.as_str() == proto::method::CANCEL {
                        let id = params.get("request_id").map(|value| value.to_string()).unwrap_or_default();
                        eprintln!("fixture: cancelled request={id}");
                    }
                    continue;
                }
                let Incoming::Request { id, method, params } = incoming else { continue };
                match method.as_str() {
                    proto::method::INITIALIZE => {
                        let provides: Vec<Value> = args
                            .provides
                            .iter()
                            .map(|(capability, version)| {
                                json!({ "capability": capability, "version": version })
                            })
                            .collect();
                        let reply = json!({
                            "protocol": proto::PROTOCOL_VERSION,
                            "provides": provides,
                            "requires": args.requires,
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
                            send(&mut out, &proto::notify(proto::method::STREAM_CHUNK, chunk)).await;
                        }
                        let terminal = json!({
                            "stream_id": "s-1",
                            "seq": args.chunks,
                            "data": null,
                            "done": true,
                        });
                        send(&mut out, &proto::notify(proto::method::STREAM_CHUNK, terminal)).await;
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
