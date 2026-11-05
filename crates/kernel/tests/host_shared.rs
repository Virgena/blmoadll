//! The shared extension host, end to end.
//!
//! The fixture binary plays the host (one pipe, tagged frames) and the plugins
//! inside it, so the kernel's side of the contract is tested without Node:
//! load, initialize, start, invoke, one plugin exiting, the whole host crashing,
//! the restart, and the strike that quarantines a repeat offender.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use protocol::{read_frame, write_frame};

const BLMOADLL: &str = env!("CARGO_BIN_EXE_blmoadll");
const FIXTURE: &str = env!("CARGO_BIN_EXE_blmoadll-fixture");
const MAX_FRAME_BYTES: usize = 1 << 20;
const PATIENCE: Duration = Duration::from_secs(15);

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("blmoadll-shared-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("scratch dir");
    path
}

fn shared_config(dir: &Path, extra: &str) -> PathBuf {
    let path = dir.join("blmoadll.toml");
    let body = format!(
        r#"
[kernel]
host_ping_ms = 50
host_restarts = 3

[kernel.host]
command = '{FIXTURE}'
args = ["--host", "--plugins", "a:demo.one,b:demo.two"{extra}]

[plugins.a]
command = "node"
args = ["a.js"]

[plugins.b]
command = "node"
args = ["b.js"]

[capability]
"demo.one" = "a"
"demo.two" = "b"
"#
    );
    std::fs::write(&path, body).expect("write config");
    path
}

struct Kernel {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    logs: Arc<Mutex<String>>,
    events: Vec<Value>,
    next_id: u64,
}

impl Drop for Kernel {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl Kernel {
    async fn spawn(config: &Path) -> Kernel {
        let mut child = Command::new(BLMOADLL)
            .arg(config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the kernel");
        let stdin = child.stdin.take().expect("kernel stdin");
        let stdout = BufReader::new(child.stdout.take().expect("kernel stdout"));
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
        Kernel {
            child,
            stdin: Some(stdin),
            stdout,
            logs,
            events: Vec::new(),
            next_id: 1,
        }
    }

    async fn frame(&mut self) -> Value {
        let payload = tokio::time::timeout(PATIENCE, read_frame(&mut self.stdout, MAX_FRAME_BYTES))
            .await
            .expect("a frame before the deadline")
            .unwrap_or_else(|error| {
                panic!(
                    "the kernel stopped serving: {error}; logs:\n{}",
                    self.logs.lock().unwrap()
                )
            })
            .unwrap_or_else(|| {
                panic!(
                    "the kernel closed its pipe; logs:\n{}",
                    self.logs.lock().unwrap()
                )
            });
        serde_json::from_slice(&payload).expect("a JSON frame")
    }

    async fn send(&mut self, frame: &Value) {
        let body = serde_json::to_vec(frame).unwrap();
        let stdin = self.stdin.as_mut().expect("kernel stdin");
        write_frame(stdin, &body).await.expect("write a frame");
        stdin.flush().await.expect("flush");
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await;
        loop {
            let frame = self.frame().await;
            if frame.get("id").and_then(Value::as_u64) == Some(id) {
                return frame;
            }
            self.events.push(frame);
        }
    }

    async fn subscribe(&mut self, patterns: &[&str]) -> Value {
        self.request("subscribe", json!({ "patterns": patterns, "replay": true }))
            .await
    }

    /// Reads frames until one carries `topic`, keeping everything else.
    async fn wait_event(&mut self, topic: &str) -> Value {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        loop {
            if let Some(index) = self.events.iter().position(|frame| {
                frame.get("method").and_then(Value::as_str) == Some("$/event")
                    && frame["params"]["topic"].as_str() == Some(topic)
            }) {
                let frame = self.events.remove(index);
                return frame["params"]["payload"].clone();
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "no {topic} event; saw {:?}; logs:\n{}",
                    self.events,
                    self.logs.lock().unwrap()
                );
            }
            let frame = self.frame().await;
            self.events.push(frame);
        }
    }

    async fn shutdown(&mut self) {
        let _ = self
            .request("shutdown", json!({ "reason": "ui_quit" }))
            .await;
        let _ = tokio::time::timeout(Duration::from_secs(10), self.child.wait()).await;
    }
}

#[tokio::test]
async fn one_host_serves_both_plugins() {
    let dir = scratch("serves");
    let config = shared_config(&dir, "");
    let mut kernel = Kernel::spawn(&config).await;

    let capabilities = kernel.request("capabilities", json!({})).await;
    assert_eq!(capabilities["result"]["demo.one"]["plugin"], json!("a"));
    assert_eq!(capabilities["result"]["demo.two"]["plugin"], json!("b"));

    let reply = kernel
        .request(
            "invoke",
            json!({
                "capability": "demo.one",
                "method": "ping",
                "params": { "x": 1 },
            }),
        )
        .await;
    assert_eq!(reply["result"]["got"], json!({ "x": 1 }));

    let reply = kernel
        .request(
            "invoke",
            json!({
                "capability": "demo.two",
                "method": "ping",
                "params": { "y": 2 },
            }),
        )
        .await;
    assert_eq!(reply["result"]["got"], json!({ "y": 2 }));

    kernel.shutdown().await;
    let logs = kernel.logs.lock().unwrap().clone();
    assert!(
        logs.contains("boot: 2 plugins, 1 host, 0 processes"),
        "{logs}"
    );
}

#[tokio::test]
async fn one_plugin_exiting_leaves_the_other_serving() {
    let dir = scratch("degrade");
    let config = shared_config(&dir, r#", "--exit-plugin-on-invoke", "a""#);
    let mut kernel = Kernel::spawn(&config).await;
    kernel.request("capabilities", json!({})).await;
    kernel
        .subscribe(&["kernel.plugin.degraded", "kernel.plugin.stopped"])
        .await;

    let reply = kernel
        .request(
            "invoke",
            json!({ "capability": "demo.one", "method": "ping", "params": {} }),
        )
        .await;
    assert_eq!(reply["error"]["code"], json!(-32011));
    let degraded = kernel.wait_event("kernel.plugin.degraded").await;
    assert_eq!(degraded["plugin"], json!("a"));

    let reply = kernel
        .request(
            "invoke",
            json!({ "capability": "demo.two", "method": "ping", "params": { "ok": true } }),
        )
        .await;
    assert_eq!(
        reply["result"]["got"],
        json!({ "ok": true }),
        "the other plugin keeps serving"
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn a_host_crash_is_restarted_and_a_repeat_offender_is_quarantined() {
    let dir = scratch("crash");
    let crash = dir.join("crash");
    let crash_arg = crash.display().to_string().replace('\\', "/");
    let config = shared_config(
        &dir,
        &format!(r#", "--crash-file", "{}", "--blame", "a""#, crash_arg),
    );
    let mut kernel = Kernel::spawn(&config).await;
    kernel.request("capabilities", json!({})).await;
    kernel
        .subscribe(&["kernel.host.*", "kernel.plugin.degraded"])
        .await;

    std::fs::write(&crash, "now").unwrap();
    let exited = kernel.wait_event("kernel.host.exited").await;
    assert_eq!(exited["plugin"], json!("a"), "the blamed plugin is named");
    std::fs::remove_file(&crash).unwrap();
    let restarted = kernel.wait_event("kernel.host.restarted").await;
    assert_eq!(restarted["ready"], json!(2), "{restarted}");
    let reply = kernel
        .request(
            "invoke",
            json!({ "capability": "demo.one", "method": "ping", "params": {} }),
        )
        .await;
    assert_eq!(reply["result"]["got"], json!({}), "the plugin works again");

    std::fs::write(&crash, "again").unwrap();
    kernel.wait_event("kernel.host.exited").await;
    std::fs::remove_file(&crash).unwrap();
    let quarantined = kernel.wait_event("kernel.host.quarantined").await;
    assert_eq!(quarantined["plugin"], json!("a"));
    let restarted = kernel.wait_event("kernel.host.restarted").await;
    assert_eq!(
        restarted["ready"],
        json!(1),
        "the offender is not loaded again"
    );
    let reply = kernel
        .request(
            "invoke",
            json!({ "capability": "demo.one", "method": "ping", "params": {} }),
        )
        .await;
    assert_eq!(
        reply["error"]["code"],
        json!(-32011),
        "the quarantined plugin stays unavailable"
    );
    let reply = kernel
        .request(
            "invoke",
            json!({ "capability": "demo.two", "method": "ping", "params": {} }),
        )
        .await;
    assert_eq!(
        reply["result"]["got"],
        json!({}),
        "the innocent plugin serves"
    );

    kernel.shutdown().await;
}

#[tokio::test]
async fn check_reports_each_plugin_as_shared() {
    let dir = scratch("check");
    let config = shared_config(&dir, "");
    let run = Command::new(BLMOADLL)
        .arg(&config)
        .arg("--check")
        .arg("--json")
        .output()
        .await
        .expect("run --check");
    assert!(
        run.status.success(),
        "check failed: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    let report: Value = serde_json::from_slice(&run.stdout).expect("a JSON report");
    let plugins = report["plugins"].as_array().expect("plugins");
    assert_eq!(plugins.len(), 2);
    for plugin in plugins {
        assert_eq!(plugin["host"], json!("shared"), "{plugin}");
    }
}
