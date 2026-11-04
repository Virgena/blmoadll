use std::path::PathBuf;
use std::process::ExitCode;

use protocol::{read_frame, write_frame};
use serde_json::{json, Value};
use tokio::io::{stdin, stdout, BufReader};

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

fn split(value: &str) -> Vec<String> {
    value
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut provides: Vec<String> = Vec::new();
    let mut injects: Vec<String> = Vec::new();
    let mut exit_on_start = false;
    let mut exit_on_invoke = false;
    let mut exit_if: Option<PathBuf> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--provides" => provides.extend(split(&args.next().unwrap_or_default())),
            "--injects" => injects.extend(split(&args.next().unwrap_or_default())),
            "--exit-on-start" => exit_on_start = true,
            "--exit-on-invoke" => exit_on_invoke = true,
            "--exit-if" => exit_if = args.next().map(PathBuf::from),
            other => {
                eprintln!("blmoadll-fixture: unknown argument `{other}`");
                return ExitCode::from(2);
            }
        }
    }

    if let Some(path) = exit_if {
        tokio::spawn(async move {
            loop {
                if path.exists() {
                    std::process::exit(0);
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        });
    }

    let injects: Vec<Value> = injects
        .into_iter()
        .map(|capability| json!({ "capability": capability, "optional": false }))
        .collect();

    let mut input = BufReader::new(stdin());
    let mut output = stdout();

    let reply = |id: Value, result: Value| -> Value {
        json!({ "jsonrpc": "2.0", "id": id, "result": result })
    };

    loop {
        let frame = match read_frame(&mut input, MAX_FRAME_BYTES).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("blmoadll-fixture: bad frame: {error}");
                return ExitCode::from(1);
            }
        };

        let message: Value = match serde_json::from_slice(&frame) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("blmoadll-fixture: bad JSON: {error}");
                return ExitCode::from(1);
            }
        };

        let method = message["method"].as_str().unwrap_or_default().to_owned();
        let has_id = message.get("id").is_some();
        if !has_id {
            continue;
        }
        let id = message["id"].clone();

        let body = match method.as_str() {
            "initialize" => reply(
                id,
                json!({
                    "protocol": 3,
                    "provides": provides.clone(),
                    "injects": injects.clone(),
                    "registrations": [],
                    "host_calls": [],
                }),
            ),
            "start" => {
                if exit_on_start {
                    return ExitCode::SUCCESS;
                }
                reply(id, json!({}))
            }
            "invoke" => {
                if exit_on_invoke {
                    return ExitCode::SUCCESS;
                }
                let params = message["params"].clone();
                reply(
                    id,
                    json!({
                        "capability": params["capability"],
                        "method": params["method"],
                        "params": params["params"],
                    }),
                )
            }
            "shutdown" => {
                let encoded = serde_json::to_vec(&reply(id, json!({}))).unwrap_or_default();
                let _ = write_frame(&mut output, &encoded).await;
                return ExitCode::SUCCESS;
            }
            _ => reply(id, json!({})),
        };

        let encoded = serde_json::to_vec(&body).unwrap_or_default();
        if let Err(error) = write_frame(&mut output, &encoded).await {
            eprintln!("blmoadll-fixture: cannot write a frame: {error}");
            return ExitCode::from(1);
        }
    }
}
