//! Host-process tests: the kernel as a subprocess, driven the way a
//! TypeScript host drives it.
//!
//! `eggshell <config.toml>` speaks the same framing on fd 0/1 that a plugin
//! speaks. Nothing here touches the Rust API: spawn it, frame it, read it.
//!
//! Needs the test double and the host binary: `--features fixture,host`.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use eggshell_protocol::{read_frame, write_frame};

const EGGSHELL: &str = env!("CARGO_BIN_EXE_eggshell");
const FIXTURE: &str = env!("CARGO_BIN_EXE_eggshell-fixture");
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const PATIENCE: Duration = Duration::from_secs(15);

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("eggshell-pipe-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("scratch dir");
    path
}

/// The fixture path goes in a TOML literal string: on Windows it is full of
/// backslashes, and a basic string would treat them as escapes.
fn config(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("eggshell.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

fn one_provider(provides: &str, extra: &str) -> String {
    format!(
        r#"
[plugins.provider]
command = '{FIXTURE}'
args = ["--provides", "{provides}"{extra}]

[capability]
"demo.text" = "provider"
"#
    )
}

/// A running kernel. Killed on drop, so a failing test leaves nothing behind.
struct HostProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    /// Everything the kernel wrote to fd 2, drained in the background.
    logs: Arc<Mutex<String>>,
    next_id: u64,
}

impl Drop for HostProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl HostProcess {
    async fn spawn(path: &Path) -> HostProcess {
        let mut child = Command::new(EGGSHELL)
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the kernel");
        let stdin = child.stdin.take().expect("kernel stdin");
        let stdout = BufReader::new(child.stdout.take().expect("kernel stdout"));

        // fd 2 has to be read whatever the test does with it: a pipe nobody drains
        // is a kernel that blocks once the buffer fills.
        let logs = Arc::new(Mutex::new(String::new()));
        let sink = Arc::clone(&logs);
        let mut stderr = BufReader::new(child.stderr.take().expect("kernel stderr")).lines();
        tokio::spawn(async move {
            while let Ok(Some(line)) = stderr.next_line().await {
                let mut sink = sink.lock().unwrap();
                sink.push_str(&line);
                sink.push('\n');
            }
        });

        HostProcess {
            child,
            stdin: Some(stdin),
            stdout,
            logs,
            next_id: 0,
        }
    }

    fn logs(&self) -> String {
        self.logs.lock().unwrap().clone()
    }

    /// A plugin's stderr crosses two processes before it lands here, so a needle
    /// gets a moment to show up.
    async fn saw_log(&self, needle: &str) -> bool {
        for _ in 0..40 {
            if self.logs().contains(needle) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    async fn send(&mut self, value: &Value) {
        let body = serde_json::to_vec(value).expect("encode");
        let stdin = self.stdin.as_mut().expect("open stdin");
        write_frame(stdin, &body).await.expect("write a frame");
    }

    /// Sends one request. The ids are the host's to choose; the kernel echoes them.
    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = json!(self.next_id);
        self.send(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await;
        id
    }

    async fn recv(&mut self) -> Value {
        let payload = tokio::time::timeout(PATIENCE, read_frame(&mut self.stdout, MAX_FRAME_BYTES))
            .await
            .expect("the kernel went quiet")
            .expect("frame")
            .expect("the kernel closed its stdout");
        serde_json::from_slice(&payload).expect("a frame the host can parse")
    }

    /// The reply to one request, skipping whatever else is on the pipe.
    async fn reply(&mut self, id: &Value) -> Value {
        for _ in 0..64 {
            let frame = self.recv().await;
            if &frame["id"] == id {
                return frame;
            }
        }
        panic!("no reply for {id}");
    }

    /// The reply plus `count` notifications, in whichever order they arrive:
    /// a chunk can beat the reply that opened its stream.
    async fn reply_and(&mut self, id: &Value, method: &str, count: usize) -> (Value, Vec<Value>) {
        let (mut reply, mut notifications) = (None, Vec::new());
        while reply.is_none() || notifications.len() < count {
            let frame = self.recv().await;
            if &frame["id"] == id {
                reply = Some(frame);
            } else if frame["method"] == json!(method) {
                notifications.push(frame);
            }
        }
        (reply.unwrap(), notifications)
    }

    /// Closes the host's end of the pipe, the way a host that crashed would.
    async fn close_stdin(&mut self) {
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.shutdown().await;
        }
    }

    async fn exit(mut self) -> i32 {
        let status = tokio::time::timeout(PATIENCE, self.child.wait())
            .await
            .expect("the kernel did not exit")
            .expect("wait for the kernel");
        status.code().unwrap_or(-1)
    }

    /// The polite way out: shutdown, one reply, exit.
    async fn shutdown(mut self) -> i32 {
        let id = self.request("shutdown", json!({})).await;
        self.reply(&id).await;
        self.exit().await
    }
}
#[tokio::test]
async fn the_pipe_carries_a_capability_call() {
    let dir = scratch("call");
    let path = config(&dir, &one_provider("demo.text=1.0.0", ""));
    let mut kernel = HostProcess::spawn(&path).await;

    let id = kernel.request("capabilities", json!({})).await;
    let reply = kernel.reply(&id).await;
    assert_eq!(reply["result"]["demo.text"]["plugin"], json!("provider"));
    assert_eq!(reply["result"]["demo.text"]["version"], json!("1.0.0"));

    let id = kernel
        .request(
            "invoke",
            json!({"capability": "demo.text", "method": "echo", "params": {"hi": 1}}),
        )
        .await;
    let reply = kernel.reply(&id).await;
    assert_eq!(reply["result"]["got"]["hi"], json!(1));

    let id = kernel
        .request("invoke", json!({"capability": "demo.nope", "method": "echo"}))
        .await;
    assert_eq!(kernel.reply(&id).await["error"]["code"], json!(-32010));

    assert_eq!(kernel.shutdown().await, 0);
}

#[tokio::test]
async fn a_streamed_call_arrives_as_ordered_chunks() {
    let dir = scratch("stream");
    let path = config(&dir, &one_provider("demo.text=1.0.0", r#", "--chunks", "3""#));
    let mut kernel = HostProcess::spawn(&path).await;

    let id = kernel
        .request(
            "invoke",
            json!({
                "capability": "demo.text",
                "method": "chat",
                "params": {},
                "meta": {"stream": true},
            }),
        )
        .await;
    let (reply, frames) = kernel.reply_and(&id, "$/stream/chunk", 4).await;
    let stream_id = reply["result"]["stream_id"].as_str().expect("a stream id").to_string();

    let chunks: Vec<Value> = frames.iter().map(|frame| frame["params"].clone()).collect();
    for chunk in &chunks {
        assert_eq!(chunk["stream_id"], json!(stream_id));
    }
    assert_eq!(chunks[0]["seq"], json!(0));
    assert_eq!(chunks[2]["data"]["delta"], json!("c2"));
    assert_eq!(chunks[3]["done"], json!(true));
    assert!(chunks[3]["data"].is_null(), "the terminal block carries no data");

    assert_eq!(kernel.shutdown().await, 0);
}

#[tokio::test]
async fn events_reach_the_host_pipe() {
    let dir = scratch("events");
    let path = config(&dir, &format!(r#"
[plugins.doomed]
command = '{FIXTURE}'
args = ["--provides", "demo.text=1.0.0", "--exit-on-invoke"]

[capability]
"demo.text" = "doomed"
"#));
    let mut kernel = HostProcess::spawn(&path).await;

    let id = kernel.request("subscribe", json!({"patterns": ["kernel.plugin.*"]})).await;
    let reply = kernel.reply(&id).await;
    let subscription_id = reply["result"]["subscription_id"].clone();
    assert!(subscription_id.is_string(), "{reply}");

    let id = kernel
        .request("invoke", json!({"capability": "demo.text", "method": "echo"}))
        .await;
    let (reply, mut events) = kernel.reply_and(&id, "$/event", 1).await;
    assert_eq!(reply["error"]["code"], json!(-32011));
    let event = events.remove(0);
    assert_eq!(event["params"]["topic"], json!("kernel.plugin.degraded"));
    assert_eq!(event["params"]["payload"]["plugin"], json!("doomed"));

    let id = kernel.request("unsubscribe", json!({"subscription_id": subscription_id})).await;
    assert_eq!(kernel.reply(&id).await["result"], json!({}));

    assert_eq!(kernel.shutdown().await, 0);
}

#[tokio::test]
async fn the_host_cannot_claim_a_terminal() {
    let dir = scratch("io");
    let path = config(&dir, &one_provider("demo.text=1.0.0", ""));
    let mut kernel = HostProcess::spawn(&path).await;

    // fd 0 and fd 1 are the host protocol. The io primitives belong to plugins,
    // and there is no third pipe left to hand a plugin anyway.
    for method in ["kernel.attach", "kernel.detach", "kernel.write"] {
        let id = kernel
            .request(
                method,
                json!({"stream": "stdin", "plugin": "provider", "data": "hi"}),
            )
            .await;
        let reply = kernel.reply(&id).await;
        assert_eq!(reply["error"]["code"], json!(-32601), "{method}: {reply}");
    }

    let id = kernel.request("nonsense", json!({})).await;
    assert_eq!(kernel.reply(&id).await["error"]["code"], json!(-32601));

    assert_eq!(kernel.shutdown().await, 0);
}

#[tokio::test]
async fn closing_the_host_pipe_is_a_goodbye() {
    let dir = scratch("eof");
    let path = config(&dir, &one_provider("demo.text=1.0.0", ""));
    let mut kernel = HostProcess::spawn(&path).await;

    kernel.close_stdin().await;
    assert_eq!(kernel.exit().await, 0);
}

#[tokio::test]
async fn a_config_that_cannot_boot_exits_one_with_a_report() {
    let dir = scratch("bad-config");
    // "demo.text" is routed to a plugin the config never declared.
    let path = config(&dir, &format!(r#"
[plugins.provider]
command = '{FIXTURE}'

[capability]
"demo.other" = "nobody"
"#));
    let output = Command::new(EGGSHELL)
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .await
        .expect("run the kernel");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    let last = stderr.lines().filter(|line| !line.trim().is_empty()).next_back();
    let report: Value = serde_json::from_str(last.expect("a report on stderr")).expect("JSON");
    assert_eq!(report["ok"], json!(false), "{report}");
    assert!(!report["errors"].as_array().expect("errors").is_empty());
}
#[tokio::test]
async fn the_host_picks_the_reason_the_plugins_hear() {
    let dir = scratch("reason");
    let path = config(&dir, &one_provider("demo.text=1.0.0", ""));

    // No reason at all: the host is the UI, so the plugins hear `ui_quit`.
    let mut kernel = HostProcess::spawn(&path).await;
    let id = kernel.request("shutdown", json!({})).await;
    kernel.reply(&id).await;
    assert!(
        kernel.saw_log("fixture: shutdown reason=ui_quit").await,
        "{}",
        kernel.logs()
    );
    assert_eq!(kernel.exit().await, 0);

    // An explicit one is passed straight through.
    let mut kernel = HostProcess::spawn(&path).await;
    let id = kernel.request("shutdown", json!({"reason": "kernel_exit"})).await;
    kernel.reply(&id).await;
    assert!(
        kernel.saw_log("fixture: shutdown reason=kernel_exit").await,
        "{}",
        kernel.logs()
    );
    assert_eq!(kernel.exit().await, 0);

    // `reload` is the kernel's own word: a host claiming it would lie to every
    // plugin, and the kernel has to still be there afterwards.
    let mut kernel = HostProcess::spawn(&path).await;
    let id = kernel.request("shutdown", json!({"reason": "reload"})).await;
    let reply = kernel.reply(&id).await;
    assert_eq!(reply["error"]["code"], json!(-32602), "{reply}");
    let id = kernel.request("capabilities", json!({})).await;
    assert!(kernel.reply(&id).await["result"]["demo.text"].is_object());
    assert_eq!(kernel.shutdown().await, 0);
}

#[tokio::test]
async fn the_host_can_cancel_a_stream() {
    let dir = scratch("cancel");
    let path = config(&dir, &one_provider("demo.text=1.0.0", r#", "--chunks", "50""#));
    let mut kernel = HostProcess::spawn(&path).await;

    let id = kernel
        .request(
            "invoke",
            json!({
                "capability": "demo.text",
                "method": "chat",
                "params": {},
                "meta": {"stream": true},
            }),
        )
        .await;
    let (reply, chunks) = kernel.reply_and(&id, "$/stream/chunk", 1).await;
    assert_eq!(chunks.len(), 1);
    let stream_id = reply["result"]["stream_id"].as_str().expect("a stream id").to_string();

    // Cancelling is a notification, not a request: there is nothing to reply to,
    // and no terminal frame comes back either.
    kernel
        .send(&json!({
            "jsonrpc": "2.0",
            "method": "$/cancel",
            "params": {"stream_id": stream_id},
        }))
        .await;

    // Cooperative means the provider is told, and the test double says so.
    assert!(
        kernel.saw_log("fixture: cancelled request=").await,
        "{}",
        kernel.logs()
    );
    assert_eq!(kernel.shutdown().await, 0);
}