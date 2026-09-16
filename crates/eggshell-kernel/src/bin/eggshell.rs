//! The kernel as a process: any language can host it over a pipe.
//!
//!     eggshell <config.toml> [--check] [--json]
//!
//! Without a flag, fd 0 carries the host's requests, fd 1 carries replies and
//! notifications, fd 2 stays the kernel's own log (one JSON object per line).
//! The plugin facing protocol is unchanged; the only difference is that the
//! host is a pipe instead of an in-process `Host`.
//!
//! `--check` instead runs a configuration check: it initializes every plugin,
//! validates the capability graph and prints the report on stdout, without ever
//! sending `start`. `--json` picks that report's format.
//!
//! A host is a pure caller: it provides no capability, it has no process and it
//! cannot be a routing target. The kernel labels its calls `meta.caller =
//! "host"`, and it will not let the host claim them.
//!
//! Built only with `--features host`, so a default build produces nothing.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use eggshell_kernel::{Host, Kernel};
use eggshell_loader::Config;
use eggshell_log as log;
use eggshell_protocol::{codes, failure, method, parse_frame, read_frame, reason, success, write_frame, Incoming, RpcError};

/// Frame cap for the host pipe. The plugin pipes keep their own
/// `max_frame_bytes`; this is the host's limit.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// How many outbound frames may wait for the host before the kernel stops
/// pulling. Small on purpose: the kernel's host queue already buffers, and
/// pausing *that* is what makes backpressure reach the providers.
const OUT_QUEUE: usize = 16;

/// How long the tail of that queue gets to reach the host after the kernel is
/// done shutting down.
const TAIL_GRACE: Duration = Duration::from_millis(250);

const USAGE: &str = "\
usage: eggshell <config.toml> [--check] [--json]

  (no flag)  boot the plugins and serve the host protocol on fd 0 / fd 1
  --check    initialize every plugin, validate the capability graph and print
             the report on stdout; never sends `start`, never touches io
  --json     print the --check report as one JSON object instead of text
";

#[tokio::main]
async fn main() -> ExitCode {
    let mut path: Option<String> = None;
    let mut check = false;
    let mut json = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--check" => check = true,
            "--json" => json = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return ExitCode::from(0);
            }
            other if other.starts_with('-') => {
                eprintln!("unknown option: {other}\n\n{USAGE}");
                return ExitCode::from(2);
            }
            other if path.is_none() => path = Some(other.to_string()),
            other => {
                eprintln!("unexpected argument: {other}\n\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let Some(path) = path else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };

    // A configuration check ends here: `run` loads the config itself, so a
    // config that cannot even be parsed still comes back as a report instead of
    // a bare exit code.
    if check {
        let report = eggshell_kernel::run(&PathBuf::from(&path), true).await;
        if json {
            println!("{}", report.to_json());
        } else {
            print!("{}", report.to_text());
        }
        return ExitCode::from(u8::try_from(report.code).unwrap_or(1));
    }

    let env = |name: &str| std::env::var(name).ok();
    let config = match Config::load(&PathBuf::from(&path), &env) {
        Ok(config) => config,
        Err(error) => {
            log::error("kernel", &format!("cannot load {path}: {error}"));
            return ExitCode::from(1);
        }
    };

    let kernel = match Kernel::boot_host(config).await {
        Ok(kernel) => kernel,
        Err(report) => {
            // One JSON object on stderr, same shape as `--json`, so a host can
            // tell "your config is wrong" from "your config cannot start".
            eprintln!("{}", report.to_json());
            return ExitCode::from(1);
        }
    };

    let Some(host) = kernel.host() else {
        log::error("kernel", "the host channel was already taken");
        return ExitCode::from(1);
    };

    // `serve` starts the shutdown flow, the stream sweeper and the reloader. Its
    // stdin reader stays dormant in host mode: nothing can attach to stdin, and
    // fd 0 is the protocol.
    kernel.clone().serve();

    let (out, queue) = mpsc::channel::<Value>(OUT_QUEUE);
    let writer = tokio::spawn(write_frames(queue));
    let pump = tokio::spawn(pump_host(host, out.clone()));
    tokio::spawn(read_host(Arc::clone(&kernel), out.clone()));

    let code = kernel.clone().wait_for_exit().await;
    drop(out);
    // Both waits are bounded. The kernel keeps its end of the host queue open
    // until the last `Arc<Kernel>` drops and `read_host` holds one, so the pump
    // never ends by itself; the writer ends only when every sender is gone and
    // the reader still holds one. The tail is small, so a grace period is enough.
    let _ = tokio::time::timeout(TAIL_GRACE, pump).await;
    let _ = tokio::time::timeout(TAIL_GRACE, writer).await;
    // Deliberately not `return ExitCode::from(code as u8)`: dropping the
    // runtime waits for the blocking thread parked on fd 0, and a host may
    // never close that pipe. Every frame is on its way out by now.
    std::process::exit(code)
}

/// Reads host requests until fd 0 closes. Requests are answered concurrently,
/// exactly like a plugin's pipe: a host waiting on one call must still be able
/// to send another.
async fn read_host(kernel: Arc<Kernel>, out: mpsc::Sender<Value>) {
    let mut reader = BufReader::new(tokio::io::stdin());
    loop {
        match read_frame(&mut reader, MAX_FRAME_BYTES).await {
            Ok(Some(payload)) => match parse_frame(&payload) {
                Ok(Incoming::Request { id, method: name, params }) => {
                    let kernel = Arc::clone(&kernel);
                    let out = out.clone();
                    tokio::spawn(async move { answer(&kernel, &out, id, &name, params).await });
                }
                Ok(Incoming::Notification { method: name, params }) => {
                    if name == method::CANCEL {
                        // The host gives up a stream (or a call): the kernel stops
                        // forwarding and tells the provider. Cancelling is
                        // cooperative, and it is silent - no terminal frame comes
                        // back, because the caller is the one who asked.
                        kernel.cancel(&params);
                    } else {
                        log::warn("kernel", &format!("host sent a notification the kernel does not take: {name}"));
                    }
                }
                Ok(Incoming::Response { .. }) | Ok(Incoming::ErrorResponse { .. }) => {
                    log::warn("kernel", "host sent a response; the kernel never calls the host");
                }
                Err(error) => log::warn(
                    "kernel",
                    &format!("host sent an unparsable frame ({})", codes::name(error.code)),
                ),
            },
            Ok(None) => break,
            Err(error) => {
                log::warn("kernel", &format!("host pipe: {error}"));
                break;
            }
        }
    }
    // The host closed fd 0. `stdin_eof` is the kernel's existing door for "the
    // pipe I read is gone": in host mode nothing is attached to stdin, so it
    // only starts the one shutdown flow, with reason `kernel_exit`.
    kernel.stdin_eof();
}

/// Answers one host request. `shutdown` is the exception: it replies before it
/// acts, because the host is waiting on that very reply.
async fn answer(kernel: &Arc<Kernel>, out: &mpsc::Sender<Value>, id: Value, name: &str, params: Value) {
    let reply = match name {
        "invoke" => {
            let capability = str_field(&params, "capability");
            let method_name = str_field(&params, "method");
            let inner = params.get("params").cloned().unwrap_or(Value::Null);
            let stream = params
                .get("meta")
                .and_then(|meta| meta.get("stream"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let outcome = if stream {
                kernel.invoke_stream(&capability, &method_name, inner).await
            } else {
                kernel.invoke(&capability, &method_name, inner).await
            };
            match outcome {
                Ok(value) => success(id, value),
                Err(error) => failure(id, &error),
            }
        }
        "capabilities" => success(id, kernel.capabilities()),
        "subscribe" => {
            let patterns = string_list(&params, "patterns");
            match kernel.subscribe(&patterns) {
                Ok(subscription_id) => success(id, json!({ "subscription_id": subscription_id })),
                Err(error) => failure(id, &error),
            }
        }
        "unsubscribe" => {
            let subscription_id = str_field(&params, "subscription_id");
            match kernel.unsubscribe(&subscription_id) {
                Ok(()) => success(id, json!({})),
                Err(error) => failure(id, &error),
            }
        }
        "shutdown" => {
            // The only two reasons a host can honestly give: the user quit, or the
            // host itself is going away. `reload` and `check` are the kernel's own
            // words - a host claiming one would lie to every plugin about why it is
            // being taken down.
            let reason = match params.get("reason").and_then(Value::as_str) {
                None | Some(reason::UI_QUIT) => reason::UI_QUIT,
                Some(reason::KERNEL_EXIT) => reason::KERNEL_EXIT,
                Some(other) => {
                    let error = RpcError::new(
                        codes::INVALID_PARAMS,
                        format!("shutdown reason must be \"ui_quit\" or \"kernel_exit\", not \"{other}\""),
                    );
                    let _ = out.send(failure(id, &error)).await;
                    return;
                }
            };
            // Reply first: the host is waiting on this very frame.
            let _ = out.send(success(id, json!({}))).await;
            kernel.shutdown_with(reason);
            return;
        }
        other => failure(
            id,
            &RpcError::new(codes::METHOD_NOT_FOUND, format!("unknown method {other}")),
        ),
    };
    let _ = out.send(reply).await;
}

/// The host's end of the kernel queue, on its way to fd 1.
///
/// `out.send().await` is the whole backpressure story: while the host is behind,
/// the pump stops calling `recv`, the kernel's host queue fills, and the
/// kernel's own watermarks pause the providers. Nothing is buffered here.
async fn pump_host(mut host: Host, out: mpsc::Sender<Value>) {
    while let Some(frame) = host.recv().await {
        if out.send(frame).await.is_err() {
            break;
        }
    }
}

/// Serialises every outbound frame. One writer means replies and notifications
/// can never interleave inside a frame.
async fn write_frames(mut queue: mpsc::Receiver<Value>) {
    let mut stdout = tokio::io::stdout();
    while let Some(frame) = queue.recv().await {
        let body = match serde_json::to_vec(&frame) {
            Ok(body) => body,
            Err(error) => {
                log::warn("kernel", &format!("cannot encode a frame for the host: {error}"));
                continue;
            }
        };
        if write_frame(&mut stdout, &body).await.is_err() {
            break;
        }
    }
    let _ = stdout.flush().await;
}

fn str_field(params: &Value, key: &str) -> String {
    params.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

fn string_list(params: &Value, key: &str) -> Vec<String> {
    params
        .get(key)
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(Value::as_str).map(str::to_string).collect())
        .unwrap_or_default()
}
