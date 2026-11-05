//! Spawning, supervising and talking to one plugin process.
//!
//! One process, two directions: a writer task owns the plugin's stdin, a reader
//! task owns its stdout and hands parsed frames to the kernel, and a third task
//! forwards stderr into the kernel log.

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

use protocol as proto;
use protocol::{Incoming, RpcError, codes};

use loader::{Limits, PluginSpec};
use logger as log;

/// Serializes one frame: `Content-Length: N\r\n\r\n<body>`.
pub fn encode(value: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"null".to_vec());
    let mut out = Vec::with_capacity(body.len() + 32);
    out.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
    out.extend_from_slice(&body);
    out
}

/// One encoded frame waiting for a writer, plus the counter it charges.
///
/// A shared host has one writer for every plugin's frames, so the frame, not the
/// sink, is what remembers whose `queued_bytes` this byte count belongs to.
pub struct Frame {
    pub bytes: Vec<u8>,
    pub counter: Option<Arc<AtomicUsize>>,
}

impl Frame {
    pub fn untracked(bytes: Vec<u8>) -> Frame {
        Frame {
            bytes,
            counter: None,
        }
    }
}

/// The kernel -> plugin direction: a queue of encoded frames plus the number of
/// bytes still waiting to be written. The watermarks read that counter.
#[derive(Clone)]
pub struct Sink {
    tx: mpsc::UnboundedSender<Frame>,
    bytes: Arc<AtomicUsize>,
    /// Set on a shared host, where every frame carries the plugin it is for.
    tag: Option<Arc<str>>,
}

impl Sink {
    /// A sink that goes nowhere, used before a process exists.
    pub fn closed() -> Sink {
        let (tx, _rx) = mpsc::unbounded_channel();
        Sink {
            tx,
            bytes: Arc::new(AtomicUsize::new(0)),
            tag: None,
        }
    }

    /// A sink over `tx`, optionally tagging every frame with a plugin id. The
    /// queue may be shared by many sinks; each sink counts only its own bytes.
    pub fn over(tx: mpsc::UnboundedSender<Frame>, tag: Option<String>) -> Sink {
        Sink {
            tx,
            bytes: Arc::new(AtomicUsize::new(0)),
            tag: tag.map(Arc::from),
        }
    }

    pub fn queued_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    fn encoded(&self, frame: &Value) -> Vec<u8> {
        let tagged = self.tag.as_ref().map(|tag| {
            let mut owned = frame.clone();
            owned["plugin"] = Value::String(tag.to_string());
            owned
        });
        match &tagged {
            Some(owned) => encode(owned),
            None => encode(frame),
        }
    }

    fn push(&self, frame: &Value) {
        let encoded = self.encoded(frame);
        let len = encoded.len();
        self.bytes.fetch_add(len, Ordering::Relaxed);
        let queued = Frame {
            bytes: encoded,
            counter: Some(self.bytes.clone()),
        };
        if self.tx.send(queued).is_err() {
            self.bytes.fetch_sub(len, Ordering::Relaxed);
        }
    }

    /// Enqueues unconditionally. Events use this: dropping a subscriber's
    /// backlog is the event bus's problem, not the kernel's.
    pub fn send(&self, frame: &Value) {
        self.push(frame);
    }

    /// Enqueues only while the queue stays under `hard_limit`. `false` means the
    /// caller has to shed load.
    pub fn try_send(&self, frame: &Value, hard_limit: usize) -> bool {
        let encoded = self.encoded(frame);
        if self.queued_bytes() + encoded.len() > hard_limit {
            return false;
        }
        let len = encoded.len();
        self.bytes.fetch_add(len, Ordering::Relaxed);
        let queued = Frame {
            bytes: encoded,
            counter: Some(self.bytes.clone()),
        };
        if self.tx.send(queued).is_err() {
            self.bytes.fetch_sub(len, Ordering::Relaxed);
            return false;
        }
        true
    }
}

pub struct Process {
    pub child: Option<Child>,
    pub pid: Option<u32>,
    pub sink: Sink,
}

impl Process {
    pub fn force_kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

/// The plugin -> kernel direction: parsed frames until the pipe closes.
pub struct Frames {
    pub rx: mpsc::UnboundedReceiver<Incoming>,
    pub closed: oneshot::Receiver<()>,
}

pub(crate) fn signal_of(status: &std::process::ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        None
    }
}

/// `{plugin, exit_code, signal}` as carried by `-32011` and `degraded`.
pub fn exit_payload(plugin: &str, status: Option<std::process::ExitStatus>) -> Value {
    match status {
        Some(status) => json!({
            "plugin": plugin,
            "exit_code": status.code(),
            "signal": signal_of(&status),
        }),
        None => json!({ "plugin": plugin, "exit_code": null, "signal": null }),
    }
}

/// Spawns one plugin. Never blocks on the plugin's own startup work.
pub fn spawn(id: &str, spec: &PluginSpec, limits: &Limits) -> std::io::Result<(Process, Frames)> {
    let mut command = Command::new(&spec.command);
    command.args(&spec.args);
    command.current_dir(&spec.cwd);
    if spec.clear_env {
        command.env_clear();
    }
    for (key, value) in &spec.env {
        command.env(key, value);
    }
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.kill_on_drop(true);
    #[cfg(target_os = "linux")]
    unsafe {
        // Hardening only: the contract's guarantee is that a plugin exits on its
        // own once stdin closes. Nothing here promises reaping a killed kernel.
        // prctl is Linux only, so macOS gets the plain spawn.
        use std::os::unix::process::CommandExt;
        command.as_std_mut().pre_exec(|| {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            Ok(())
        });
    }

    let mut child = command.spawn()?;
    let pid = child.id();
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let (tx, rx) = mpsc::unbounded_channel::<Frame>();
    if let Some(stdin) = stdin {
        tokio::spawn(writer(stdin, rx));
    }

    let (frame_tx, frame_rx) = mpsc::unbounded_channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let max_frame_bytes = limits.max_frame_bytes;
    if let Some(stdout) = stdout {
        let id = id.to_string();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                match proto::read_frame(&mut reader, max_frame_bytes).await {
                    Ok(Some(payload)) => match proto::parse_frame(&payload) {
                        Ok(incoming) => {
                            if frame_tx.send(incoming).is_err() {
                                break;
                            }
                        }
                        Err(e) => log::warn(
                            "kernel",
                            &format!("plugin={id} unparsable frame ({})", codes::name(e.code)),
                        ),
                    },
                    Ok(None) => break,
                    Err(proto::FrameError::TooLarge(n)) => {
                        log::warn(
                            "kernel",
                            &format!(
                                "plugin={id} frame of {n} bytes exceeds max_frame_bytes ({max_frame_bytes}); \
                                 killing (code {})",
                                codes::FRAME_TOO_LARGE
                            ),
                        );
                        break;
                    }
                    Err(e) => {
                        log::warn("kernel", &format!("plugin={id} read error: {e}"));
                        break;
                    }
                }
            }
            let _ = closed_tx.send(());
        });
    }

    if let Some(stderr) = stderr {
        let id = id.to_string();
        let limit = limits.log_line_bytes;
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                log::plugin_stderr(&id, &line, limit);
            }
        });
    }

    Ok((
        Process {
            child: Some(child),
            pid,
            sink: Sink::over(tx, None),
        },
        Frames {
            rx: frame_rx,
            closed: closed_rx,
        },
    ))
}

/// Writes every queued frame, in order, and releases its byte charge only once
/// it is actually written. One writer per pipe: frames can never interleave.
pub(crate) async fn writer<W>(mut out: W, mut rx: mpsc::UnboundedReceiver<Frame>)
where
    W: AsyncWrite + Unpin,
{
    while let Some(frame) = rx.recv().await {
        if out.write_all(&frame.bytes).await.is_err() || out.flush().await.is_err() {
            break;
        }
        if let Some(counter) = &frame.counter {
            counter.fetch_sub(frame.bytes.len(), Ordering::Relaxed);
        }
    }
}

/// The `-32011` body for a plugin that is gone.
pub fn gone(plugin: &str, detail: &Value) -> RpcError {
    RpcError {
        code: codes::PROVIDER_UNAVAILABLE,
        message: format!("provider `{plugin}` is not available"),
        data: Some(detail.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn encoded_frames_round_trip_through_the_reader() {
        let frame = proto::request(3, "invoke", json!({"capability": "demo.text"}));
        let bytes = encode(&frame);
        let mut reader = BufReader::new(bytes.as_slice());
        let payload = proto::read_frame(&mut reader, 1024).await.unwrap().unwrap();
        match proto::parse_frame(&payload).unwrap() {
            Incoming::Request { id, method, params } => {
                assert_eq!(id, json!(3));
                assert_eq!(method, "invoke");
                assert_eq!(params["capability"], json!("demo.text"));
            }
            other => panic!("expected a request, got {other:?}"),
        }
        // A second frame straight after the first, still on one pipe.
        let mut both = encode(&frame);
        both.extend_from_slice(&encode(&proto::success(json!(3), json!({}))));
        let mut reader = BufReader::new(both.as_slice());
        assert!(
            proto::read_frame(&mut reader, 1024)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            proto::read_frame(&mut reader, 1024)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn too_large_frames_are_reported_not_delivered() {
        let frame = proto::request(1, "invoke", json!({"pad": "x".repeat(4096)}));
        let bytes = encode(&frame);
        let mut reader = BufReader::new(bytes.as_slice());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let err = rt.block_on(proto::read_frame(&mut reader, 64)).unwrap_err();
        assert!(matches!(err, proto::FrameError::TooLarge(_)));
    }
}
