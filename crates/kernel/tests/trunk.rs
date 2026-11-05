//! Trunk tests: the kernel driven with a `[kernel.runtime]` row.
//!
//! The Runtime is a child process here too, so these tests spawn the kernel
//! binary and speak the front-end protocol over its fd 0/1. The test double
//! stands in for the Runtime: `blmoadll-fixture --runtime`.
//!
//! Needs the test double and the host binary: `--features fixture,host`.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use protocol::{read_frame, write_frame};

const BLMOADLL: &str = env!("CARGO_BIN_EXE_blmoadll");
const FIXTURE: &str = env!("CARGO_BIN_EXE_blmoadll-fixture");
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const PATIENCE: Duration = Duration::from_secs(15);

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("blmoadll-trunk-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("scratch dir");
    path
}

fn runtime_config(dir: &Path, ready_ms: u64, flags: &[&str]) -> PathBuf {
    let mut args = vec!["--runtime", "--plugins", "a:demo.one,b:demo.two"];
    args.extend_from_slice(flags);
    let rendered: Vec<String> = args.iter().map(|flag| format!("\"{flag}\"")).collect();
    let path = dir.join("blmoadll.toml");
    std::fs::write(
        &path,
        format!(
            "[kernel]\nruntime_ready_ms = {ready_ms}\n\n\
             [kernel.runtime]\ncommand = '{FIXTURE}'\nargs = [{}]\n",
            rendered.join(", ")
        ),
    )
    .expect("write config");
    path
}

struct KernelProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    logs: Arc<Mutex<String>>,
    next_id: u64,
}

impl Drop for KernelProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl KernelProcess {
    async fn spawn(path: &Path) -> KernelProcess {
        let mut child = Command::new(BLMOADLL)
            .arg(path)
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

        KernelProcess {
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

    async fn saw_log(&self, needle: &str) -> bool {
        for _ in 0..40 {
            if self.logs().contains(needle) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = json!(self.next_id);
        let stdin = self.stdin.as_mut().expect("open stdin");
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .expect("encode");
        write_frame(stdin, &body).await.expect("write a frame");
        id
    }

    async fn recv(&mut self) -> Value {
        let outcome =
            tokio::time::timeout(PATIENCE, read_frame(&mut self.stdout, MAX_FRAME_BYTES)).await;
        let payload = match outcome {
            Ok(Ok(Some(payload))) => payload,
            Ok(Err(error)) => panic!("bad frame {error:?}; logs: {}", self.logs()),
            Ok(Ok(None)) => panic!("the kernel closed its stdout; logs: {}", self.logs()),
            Err(_) => panic!("the kernel went quiet; logs: {}", self.logs()),
        };
        serde_json::from_slice(&payload).expect("a frame the front end can parse")
    }

    async fn reply(&mut self, id: &Value) -> Value {
        for _ in 0..64 {
            let frame = self.recv().await;
            if &frame["id"] == id {
                return frame;
            }
        }
        panic!("no reply for {id}");
    }

    async fn reply_and(&mut self, id: &Value, method: &str, count: usize) -> (Value, Vec<Value>) {
        let (mut reply, mut notes) = (None, Vec::new());
        while reply.is_none() || notes.len() < count {
            let frame = self.recv().await;
            if &frame["id"] == id {
                reply = Some(frame);
            } else if frame["method"] == json!(method) {
                notes.push(frame);
            }
        }
        (reply.unwrap(), notes)
    }

    async fn shutdown(mut self) -> i32 {
        let id = self.request("shutdown", json!({})).await;
        self.reply(&id).await;
        self.exit().await
    }

    async fn exit(mut self) -> i32 {
        let status = tokio::time::timeout(PATIENCE, self.child.wait())
            .await
            .expect("the kernel did not exit")
            .expect("wait for the kernel");
        status.code().unwrap_or(-1)
    }
}

#[tokio::test]
async fn a_runtime_config_carries_calls_and_streams() {
    let dir = scratch("carry");
    let path = runtime_config(&dir, 10_000, &["--chunks", "2"]);
    let mut kernel = KernelProcess::spawn(&path).await;

    let id = kernel.request("capabilities", json!({})).await;
    let reply = kernel.reply(&id).await;
    assert_eq!(reply["result"]["demo.one"]["plugin"], json!("a"));
    assert_eq!(reply["result"]["demo.two"]["plugin"], json!("b"));

    let id = kernel
        .request(
            "invoke",
            json!({"capability": "demo.one", "method": "echo", "params": {"hi": 1}}),
        )
        .await;
    let reply = kernel.reply(&id).await;
    assert_eq!(reply["result"]["got"]["hi"], json!(1));

    let id = kernel
        .request(
            "invoke",
            json!({
                "capability": "demo.two",
                "method": "chat",
                "params": {},
                "meta": {"stream": true},
            }),
        )
        .await;
    let (reply, chunks) = kernel.reply_and(&id, "$/stream/chunk", 3).await;
    let stream_id = reply["result"]["stream_id"].clone();
    assert!(stream_id.is_string(), "a stream opens with an id: {reply}");
    assert_eq!(chunks[0]["params"]["stream_id"], stream_id);
    assert_eq!(chunks[2]["params"]["done"], json!(true));

    let id = kernel.request("status", json!({})).await;
    let reply = kernel.reply(&id).await;
    assert_eq!(reply["result"]["state"], json!("ready"));
    assert_eq!(reply["result"]["total"], json!(2));

    assert_eq!(kernel.shutdown().await, 0);
}

#[tokio::test]
async fn a_runtime_with_another_version_fails_the_launch() {
    let dir = scratch("version");
    let path = runtime_config(&dir, 5_000, &["--bad-version"]);
    let kernel = KernelProcess::spawn(&path).await;
    assert!(
        kernel.saw_log("protocol 2").await,
        "logs: {}",
        kernel.logs()
    );
    assert_eq!(kernel.exit().await, 1);
}

#[tokio::test]
async fn a_runtime_that_never_registers_times_out() {
    let dir = scratch("quiet");
    let path = runtime_config(&dir, 300, &["--no-register"]);
    let kernel = KernelProcess::spawn(&path).await;
    assert!(
        kernel.saw_log("did not register").await,
        "logs: {}",
        kernel.logs()
    );
    assert_eq!(kernel.exit().await, 1);
}

#[tokio::test]
async fn a_runtime_that_dies_ends_the_run() {
    let dir = scratch("dies");
    let path = runtime_config(&dir, 5_000, &["--exit-after-ready"]);
    let kernel = KernelProcess::spawn(&path).await;
    assert!(
        kernel.saw_log("runtime exited").await,
        "logs: {}",
        kernel.logs()
    );
    assert_ne!(kernel.exit().await, 0);
}
