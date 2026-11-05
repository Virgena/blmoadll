//! One process, many plugins: the extension host.
//!
//! The kernel speaks the unchanged plugin protocol on one pipe. Every frame
//! carries a `plugin` member so the host can hand it to the right plugin, and
//! every frame the host writes back carries it so the kernel can route it. The
//! host itself is a child process, so a crash takes the plugins down and leaves
//! the kernel alive.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, watch};

use loader::{Limits, PluginSpec};
use logger as log;
use protocol as proto;
use protocol::{Incoming, RpcError, codes, method};

use crate::process::{self, Frame, Frames, Sink};

/// One hosted plugin: its channel, its byte counter, and the host it lives in.
pub struct Member {
    pub host: Arc<SharedHost>,
    pub plugin: String,
    pub sink: Sink,
}

/// How the host process ended. `exit_code` is `None` when the platform gave no
/// code (killed, or a signal on unix).
#[derive(Debug, Clone, Copy)]
pub struct HostDeath {
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
}

struct MemberLink {
    frames: mpsc::UnboundedSender<Incoming>,
    closed: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct SharedState {
    members: HashMap<String, MemberLink>,
    calls: HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>,
    exits: HashMap<String, Value>,
    /// The plugin whose module was being imported when the host last reported
    /// progress, so a crash can name a culprit.
    loading: Option<String>,
    crashed: Option<String>,
}

pub struct SharedHost {
    child: tokio::sync::Mutex<Option<Child>>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    input: mpsc::UnboundedSender<Frame>,
    pid: Option<u32>,
    next_id: AtomicU64,
    state: Mutex<SharedState>,
    deaths: watch::Sender<Option<HostDeath>>,
}

impl SharedHost {
    /// Starts the host exactly like a plugin process: same stdio, same
    /// hardening, one pipe.
    pub fn spawn(spec: &PluginSpec, limits: &Limits) -> std::io::Result<Arc<SharedHost>> {
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
            use std::os::unix::process::CommandExt;
            command.as_std_mut().pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
                Ok(())
            });
        }

        let mut child = command.spawn()?;
        let pid = child.id();
        let stdin = child.stdin.take().expect("host stdin");
        let stdout = child.stdout.take().expect("host stdout");
        let stderr = child.stderr.take().expect("host stderr");

        let (input, output) = mpsc::unbounded_channel::<Frame>();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        tokio::spawn(host_writer(stdin, output, stop_rx));

        let (deaths, _) = watch::channel(None);
        let host = Arc::new(SharedHost {
            child: tokio::sync::Mutex::new(Some(child)),
            stop: Mutex::new(Some(stop_tx)),
            input,
            pid,
            next_id: AtomicU64::new(1),
            state: Mutex::new(SharedState::default()),
            deaths,
        });

        let log_line_bytes = limits.log_line_bytes;
        let max_frame_bytes = limits.max_frame_bytes;
        {
            let id = "host".to_string();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    log::plugin_stderr(&id, &line, log_line_bytes);
                }
            });
        }
        {
            let host = host.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout);
                loop {
                    match proto::read_frame(&mut reader, max_frame_bytes).await {
                        Ok(Some(payload)) => match proto::parse_tagged(&payload) {
                            Ok((Some(plugin), incoming)) => host.route(plugin, incoming),
                            Ok((None, incoming)) => host.route_control(incoming),
                            Err(error) => log::warn(
                                "host",
                                &format!("unparsable frame ({})", codes::name(error.code)),
                            ),
                        },
                        Ok(None) => break,
                        Err(error) => {
                            log::warn("host", &format!("host pipe: {error}"));
                            break;
                        }
                    }
                }
                host.dead().await;
            });
        }
        Ok(host)
    }

    /// Registers one plugin on the shared pipe. The kernel keeps the `Frames` and
    /// dispatches it exactly as it dispatches a plugin process.
    pub fn join(self: &Arc<Self>, plugin: &str) -> (Member, Frames) {
        let (frame_tx, frame_rx) = mpsc::unbounded_channel();
        let (closed_tx, closed_rx) = oneshot::channel();
        self.state.lock().unwrap().members.insert(
            plugin.to_string(),
            MemberLink {
                frames: frame_tx,
                closed: Some(closed_tx),
            },
        );
        let sink = Sink::over(self.input.clone(), Some(plugin.to_string()));
        (
            Member {
                host: self.clone(),
                plugin: plugin.to_string(),
                sink,
            },
            Frames {
                rx: frame_rx,
                closed: closed_rx,
            },
        )
    }

    pub async fn load(&self, plugin: &str, entry: &str, timeout_ms: u64) -> Result<(), RpcError> {
        self.call(
            method::HOST_LOAD,
            json!({ "plugin": plugin, "entry": entry }),
            timeout_ms,
        )
        .await
        .map(|_| ())
    }

    pub async fn unload(
        &self,
        plugin: &str,
        reason: &str,
        timeout_ms: u64,
    ) -> Result<(), RpcError> {
        self.call(
            method::HOST_UNLOAD,
            json!({ "plugin": plugin, "reason": reason }),
            timeout_ms,
        )
        .await
        .map(|_| ())
    }

    /// Fire and forget: a stuck host cannot answer a kill anyway, and the
    /// kernel's next step is to kill the process.
    pub fn kill(&self, plugin: &str) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = proto::request(id, method::HOST_KILL, json!({ "plugin": plugin }));
        let _ = self.input.send(Frame::untracked(process::encode(&frame)));
    }

    pub async fn ping(&self, timeout_ms: u64) -> bool {
        self.call(method::HOST_PING, json!({}), timeout_ms)
            .await
            .is_ok()
    }

    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    pub fn died(&self) -> watch::Receiver<Option<HostDeath>> {
        self.deaths.subscribe()
    }

    pub fn exit_detail(&self, plugin: &str) -> Option<Value> {
        self.state.lock().unwrap().exits.get(plugin).cloned()
    }

    pub fn last_loading(&self) -> Option<String> {
        self.state.lock().unwrap().loading.clone()
    }

    pub fn last_crash(&self) -> Option<String> {
        self.state.lock().unwrap().crashed.clone()
    }

    /// Closes the host's stdin, then makes sure the process is gone. A host that
    /// ignores the closed pipe is killed rather than left behind.
    pub async fn shutdown(&self) {
        if let Some(stop) = self.stop.lock().unwrap().take() {
            let _ = stop.send(());
        }
        let mut guard = self.child.lock().await;
        if let Some(child) = guard.as_mut() {
            if tokio::time::timeout(Duration::from_millis(300), child.wait())
                .await
                .is_err()
            {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
        }
        *guard = None;
    }

    async fn call(&self, name: &str, params: Value, timeout_ms: u64) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.state.lock().unwrap().calls.insert(id, tx);
        let frame = proto::request(id, name, params);
        if self
            .input
            .send(Frame::untracked(process::encode(&frame)))
            .is_err()
        {
            self.state.lock().unwrap().calls.remove(&id);
            return Err(RpcError::new(
                codes::HOST_FAILED,
                "the extension host is gone",
            ));
        }
        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(RpcError::new(
                codes::HOST_FAILED,
                "the extension host dropped the call",
            )),
            Err(_) => {
                self.state.lock().unwrap().calls.remove(&id);
                Err(RpcError::new(
                    codes::REQUEST_TIMEOUT,
                    format!("{name} timed out"),
                ))
            }
        }
    }

    fn route(&self, plugin: String, incoming: Incoming) {
        let state = self.state.lock().unwrap();
        match state.members.get(&plugin) {
            Some(link) => {
                let _ = link.frames.send(incoming);
            }
            None => log::warn(
                "host",
                &format!("plugin={plugin} sent a frame with no member"),
            ),
        }
    }

    fn route_control(&self, incoming: Incoming) {
        match incoming {
            Incoming::Response { id, result } => self.resolve(id, Ok(result)),
            Incoming::ErrorResponse { id, error } => self.resolve(id, Err(error)),
            Incoming::Request {
                id,
                method: name,
                params,
            } => {
                let error = RpcError::new(
                    codes::METHOD_NOT_FOUND,
                    format!("{name} is not a kernel method on the host pipe"),
                );
                let _ = params;
                let _ = self
                    .input
                    .send(Frame::untracked(process::encode(&proto::failure(
                        id, &error,
                    ))));
            }
            Incoming::Notification { method, params } => self.control_notice(&method, &params),
        }
    }

    fn control_notice(&self, name: &str, params: &Value) {
        let plugin = string_of(params, "plugin");
        match name {
            method::HOST_LOADING => {
                if !plugin.is_empty() {
                    self.state.lock().unwrap().loading = Some(plugin);
                }
            }
            method::HOST_EXITED => {
                let detail = json!({
                    "plugin": plugin,
                    "exit_code": params.get("exit_code").cloned().unwrap_or(Value::Null),
                    "signal": params.get("signal").cloned().unwrap_or(Value::Null),
                    "host": true,
                });
                log::debug("host", &format!("plugin={plugin} exited {detail}"));
                let mut state = self.state.lock().unwrap();
                state.exits.insert(plugin.clone(), detail);
                if let Some(mut link) = state.members.remove(&plugin) {
                    if let Some(closed) = link.closed.take() {
                        let _ = closed.send(());
                    }
                    // Dropping the frame sender ends this member's dispatch loop.
                    drop(link.frames);
                }
            }
            method::HOST_FAILED => log::warn(
                "host",
                &format!("plugin={plugin} failed: {}", string_of(params, "message")),
            ),
            method::HOST_CRASH => {
                log::warn(
                    "host",
                    &format!("plugin={plugin} crashed: {}", string_of(params, "error")),
                );
                if !plugin.is_empty() {
                    self.state.lock().unwrap().crashed = Some(plugin);
                }
            }
            method::HOST_STUCK => log::error(
                "host",
                &format!("plugin={plugin} is stuck at {}", string_of(params, "stage")),
            ),
            method::HOST_FATAL => log::error(
                "host",
                &format!(
                    "the extension host is gone: {}",
                    string_of(params, "message")
                ),
            ),
            other => log::debug("host", &format!("host notice {other}")),
        }
    }

    fn resolve(&self, id: Value, outcome: Result<Value, RpcError>) {
        let Some(numeric) = id.as_u64() else { return };
        if let Some(tx) = self.state.lock().unwrap().calls.remove(&numeric) {
            let _ = tx.send(outcome);
        }
    }

    /// The pipe closed. Every member's channel closes, every waiting caller is
    /// failed, and the supervisor is told, with the exit code when there is one.
    async fn dead(self: &Arc<Self>) {
        let closed: Vec<oneshot::Sender<()>> = {
            let mut state = self.state.lock().unwrap();
            let mut closed = Vec::new();
            for (_, mut link) in state.members.drain() {
                if let Some(tx) = link.closed.take() {
                    closed.push(tx);
                }
            }
            for (_, call) in state.calls.drain() {
                let _ = call.send(Err(RpcError::new(
                    codes::HOST_FAILED,
                    "the extension host exited",
                )));
            }
            closed
        };
        for tx in closed {
            let _ = tx.send(());
        }
        let status = {
            let mut guard = self.child.lock().await;
            let status = match guard.as_mut() {
                Some(child) => child.wait().await.ok(),
                None => None,
            };
            *guard = None;
            status
        };
        let _ = self.deaths.send(Some(HostDeath {
            exit_code: status.as_ref().and_then(|status| status.code()),
            signal: status.as_ref().and_then(process::signal_of),
        }));
    }
}

fn string_of(params: &Value, key: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// The host pipe's writer: every member's frames, one at a time, until a stop
/// signal or the last sender goes. Leaving the loop drops stdin, which is the
/// host's own exit path.
async fn host_writer(
    mut out: ChildStdin,
    mut rx: mpsc::UnboundedReceiver<Frame>,
    mut stop: oneshot::Receiver<()>,
) {
    use tokio::io::AsyncWriteExt;
    loop {
        tokio::select! {
            frame = rx.recv() => {
                let Some(frame) = frame else { break };
                if out.write_all(&frame.bytes).await.is_err() || out.flush().await.is_err() {
                    break;
                }
                if let Some(counter) = &frame.counter {
                    counter.fetch_sub(frame.bytes.len(), Ordering::Relaxed);
                }
            }
            _ = &mut stop => break,
        }
    }
    let _ = out.shutdown().await;
}
