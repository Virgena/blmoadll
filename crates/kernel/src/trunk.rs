//! The Runtime trunk: the kernel's one child.
//!
//! A runtime config names `[kernel.runtime]` instead of plugin rows. The kernel
//! then spawns exactly one process, waits for its capability snapshot, and
//! forwards the front-end protocol to it. Plugin lifecycle lives on the other
//! side of this pipe; the kernel keeps capability ids and routing.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::{mpsc, oneshot};

use loader::{Limits, PluginSpec};
use logger as log;
use protocol::{self, Incoming, RpcError, codes, failure, method, notify, request, success};

use crate::events::{Budget, EventBus};
use crate::process::{self, Frames, Process, Sink};

const OUT_QUEUE: usize = 16;
const TAIL_GRACE: Duration = Duration::from_millis(250);
const RUNTIME_PROTOCOL: u64 = 1;

const STATES: [&str; 5] = ["starting", "ready", "recovering", "quarantined", "failed"];

type Pending = Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>;

pub(crate) struct Inner {
    sink: Sink,
    out: mpsc::Sender<Value>,
    limits: Limits,
    bus: EventBus,
    child: Mutex<Option<Child>>,
    pending: Pending,
    table: Mutex<BTreeMap<String, Value>>,
    status: Mutex<Value>,
    ready: Mutex<Option<oneshot::Sender<Result<(), String>>>>,
    exited: tokio::sync::Notify,
    next_id: AtomicU64,
    next_stream: AtomicU64,
    shutting: AtomicBool,
    code: AtomicI32,
}

pub(crate) type Trunk = Arc<Inner>;

impl Inner {
    fn table_json(&self) -> Value {
        Value::Object(self.table.lock().unwrap().clone().into_iter().collect())
    }

    fn status_json(&self) -> Value {
        self.status.lock().unwrap().clone()
    }

    fn publish(&self, topic: &str, payload: Value) {
        let budget = Budget {
            len: self.limits.event_queue_len,
            bytes: self.limits.event_queue_bytes,
        };
        let payload_len = serde_json::to_vec(&payload)
            .map(|body| body.len())
            .unwrap_or(0);
        let (deliveries, notices) = self.bus.deliver(topic, &payload, payload_len, &budget);
        for delivery in deliveries.into_iter().chain(notices) {
            if delivery.plugin == protocol::HOST {
                let _ = self.out.try_send(delivery.frame);
            }
        }
    }

    fn finish_ready(&self, outcome: Result<(), String>) {
        if let Some(tx) = self.ready.lock().unwrap().take() {
            let _ = tx.send(outcome);
        }
    }

    fn fail_pending(&self) {
        let mut pending = self.pending.lock().unwrap();
        for (_, tx) in pending.drain() {
            let _ = tx.send(Err(RpcError::with_data(
                codes::PROVIDER_UNAVAILABLE,
                "the runtime is gone",
                json!({ "plugin": protocol::RUNTIME }),
            )));
        }
    }

    fn kill(&self) {
        if let Some(child) = self.child.lock().unwrap().as_mut() {
            let _ = child.start_kill();
        }
    }

    async fn call(&self, name: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        if !self
            .sink
            .try_send(&request(id, name, params), self.limits.outbound_queue_bytes)
        {
            self.pending.lock().unwrap().remove(&id);
            return Err(RpcError::new(
                codes::OVERLOADED,
                "the runtime is behind; the frame would not fit",
            ));
        }
        match tokio::time::timeout(Duration::from_millis(self.limits.request_timeout_ms), rx).await
        {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => Err(RpcError::with_data(
                codes::PROVIDER_UNAVAILABLE,
                "the runtime is gone",
                json!({ "plugin": protocol::RUNTIME }),
            )),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(RpcError::new(
                    codes::REQUEST_TIMEOUT,
                    "the runtime did not answer in time",
                ))
            }
        }
    }

    async fn answer(&self, id: Value, name: &str, params: Value) {
        let reply = match name {
            "capabilities" => success(id, self.table_json()),
            "status" => success(id, self.status_json()),
            "subscribe" => {
                let patterns = string_list(&params, "patterns");
                let replay = params
                    .get("replay")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let subscription = self.bus.subscribe(protocol::HOST, patterns.clone());
                let _ = self.sink.try_send(
                    &notify(
                        method::RUNTIME_SUBSCRIBE,
                        json!({ "patterns": patterns, "replay": replay }),
                    ),
                    self.limits.outbound_queue_bytes,
                );
                success(id, json!({ "subscription_id": subscription }))
            }
            "unsubscribe" => {
                let raw = params
                    .get("subscription_id")
                    .cloned()
                    .unwrap_or(Value::Null);
                let subscription = raw
                    .as_u64()
                    .or_else(|| raw.as_str().and_then(|text| text.parse().ok()));
                let found = subscription
                    .map(|id| self.bus.unsubscribe(id))
                    .unwrap_or(false);
                if found {
                    let _ = self.sink.try_send(
                        &notify(
                            method::RUNTIME_UNSUBSCRIBE,
                            json!({ "subscription_id": subscription }),
                        ),
                        self.limits.outbound_queue_bytes,
                    );
                    success(id, json!({}))
                } else {
                    failure(
                        id,
                        &RpcError::new(codes::UNKNOWN_SUBSCRIPTION, "no such subscription"),
                    )
                }
            }
            "invoke" => self.invoke(id.clone(), params).await,
            "restart" => match self.call("restart", params).await {
                Ok(value) => success(id, value),
                Err(error) => failure(id, &error),
            },
            "shutdown" => {
                let reason = params
                    .get("reason")
                    .cloned()
                    .unwrap_or(json!(protocol::reason::UI_QUIT));
                self.shutting.store(true, Ordering::SeqCst);
                let _ = self.call("shutdown", json!({ "reason": reason })).await;
                success(id, json!({}))
            }
            other => failure(
                id,
                &RpcError::new(codes::METHOD_NOT_FOUND, format!("unknown method {other}")),
            ),
        };
        self.reply(reply).await;
    }

    async fn invoke(&self, id: Value, params: Value) -> Value {
        let streaming = params
            .get("meta")
            .and_then(|meta| meta.get("stream"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !streaming {
            return match self.call("invoke", params).await {
                Ok(value) => success(id, value),
                Err(error) => failure(id, &error),
            };
        }
        let stream_id = format!("f-{}", self.next_stream.fetch_add(1, Ordering::SeqCst) + 1);
        let mut downstream = params;
        downstream["meta"]["stream_id"] = json!(stream_id);
        match self.call("invoke", downstream).await {
            Ok(_) => success(id, json!({ "stream_id": stream_id })),
            Err(error) => failure(id, &error),
        }
    }

    async fn reply(&self, frame: Value) {
        if self.out.send(frame).await.is_err() {
            log::warn("kernel", "the front end is gone; a reply was dropped");
        }
    }

    fn forward(&self, frame: Value) {
        if !self.sink.try_send(&frame, self.limits.outbound_queue_bytes) {
            log::warn("kernel", "a frame for the runtime did not fit its queue");
        }
    }

    fn handle_capabilities(&self, params: &Value) {
        if let Some(v) = params.get("v").and_then(Value::as_u64) {
            if v != RUNTIME_PROTOCOL {
                let message = format!(
                    "the runtime speaks protocol {v}, this kernel speaks {RUNTIME_PROTOCOL}"
                );
                log::error("kernel", &message);
                self.finish_ready(Err(message));
                self.kill();
                return;
            }
        }

        let Some(table) = params.get("table").and_then(Value::as_object) else {
            log::warn("kernel", "ignored a capability snapshot without a table");
            return;
        };
        let plugins = params
            .get("plugins")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for plugin in &plugins {
            let state = plugin.get("state").and_then(Value::as_str).unwrap_or("");
            if !STATES.contains(&state) {
                log::warn(
                    "kernel",
                    &format!("ignored a capability snapshot with state {state:?}"),
                );
                return;
            }
        }

        let mut resolved = BTreeMap::new();
        for (capability, provider) in table {
            let Some(plugin) = provider.get("plugin").and_then(Value::as_str) else {
                log::warn(
                    "kernel",
                    &format!("ignored a capability snapshot: {capability} names no plugin"),
                );
                return;
            };
            resolved.insert(capability.clone(), json!({ "plugin": plugin }));
        }

        let ready = plugins
            .iter()
            .filter(|plugin| plugin.get("state").and_then(Value::as_str) == Some("ready"))
            .count();
        let state = if plugins.is_empty() {
            "ready"
        } else {
            plugins
                .iter()
                .find_map(|plugin| {
                    let state = plugin.get("state").and_then(Value::as_str)?;
                    (state != "ready").then_some(state)
                })
                .unwrap_or("ready")
        };
        *self.status.lock().unwrap() = json!({
            "state": state,
            "ready": ready,
            "total": plugins.len(),
        });
        *self.table.lock().unwrap() = resolved.clone();
        self.publish(
            "kernel.capabilities.changed",
            json!({ "capabilities": Value::Object(resolved.into_iter().collect()) }),
        );
        self.finish_ready(Ok(()));
    }

    fn handle_event(&self, params: &Value) {
        let topic = params.get("topic").and_then(Value::as_str).unwrap_or("");
        // A plugin's own publish carries the topic it chose and names the plugin;
        // the Runtime's lifecycle facts are the only untagged ones, and those are
        // restricted to the kernel namespaces.
        let from_plugin = params
            .get("plugin")
            .and_then(Value::as_str)
            .is_some_and(|plugin| !plugin.is_empty());
        if !from_plugin
            && !(topic.starts_with("kernel.plugin.") || topic.starts_with("kernel.runtime."))
        {
            log::warn(
                "kernel",
                &format!("dropped the topic {topic:?} from the runtime"),
            );
            return;
        }
        let payload = params.get("payload").cloned().unwrap_or(Value::Null);
        if topic == "kernel.runtime.status" {
            *self.status.lock().unwrap() = payload.clone();
        }
        self.publish(topic, payload);
    }
}

async fn read_runtime(inner: Trunk, mut frames: Frames) {
    while let Some(incoming) = frames.rx.recv().await {
        match incoming {
            Incoming::Response { id, result } => {
                if let Some(id) = id.as_u64() {
                    if let Some(tx) = inner.pending.lock().unwrap().remove(&id) {
                        let _ = tx.send(Ok(result));
                    }
                }
            }
            Incoming::ErrorResponse { id, error } => {
                if let Some(id) = id.as_u64() {
                    if let Some(tx) = inner.pending.lock().unwrap().remove(&id) {
                        let _ = tx.send(Err(error));
                    }
                }
            }
            Incoming::Notification {
                method: name,
                params,
            } => match name.as_str() {
                method::RUNTIME_READY => {
                    let v = params.get("v").and_then(Value::as_u64).unwrap_or(0);
                    if v != RUNTIME_PROTOCOL {
                        let message = format!(
                            "the runtime speaks protocol {v}, this kernel speaks {RUNTIME_PROTOCOL}"
                        );
                        log::error("kernel", &message);
                        inner.finish_ready(Err(message));
                        inner.kill();
                    }
                }
                method::RUNTIME_CAPABILITIES => inner.handle_capabilities(&params),
                method::RUNTIME_EVENT => inner.handle_event(&params),
                method::STREAM_CHUNK | method::STREAM_ERROR => {
                    let _ = inner.out.try_send(notify(&name, params));
                }
                other => log::warn(
                    "kernel",
                    &format!("the runtime sent a notification the kernel does not take: {other}"),
                ),
            },
            Incoming::Request {
                id,
                method: name,
                params,
            } => {
                log::warn(
                    "kernel",
                    &format!("the runtime sent a request ({name}), which the kernel does not take"),
                );
                inner.forward(failure(
                    id,
                    &RpcError::new(
                        codes::METHOD_NOT_FOUND,
                        "the kernel never calls the runtime",
                    ),
                ));
                let _ = params;
            }
        }
    }
}

async fn watch_exit(inner: Trunk) {
    let child = inner.child.lock().unwrap().take();
    let Some(mut child) = child else {
        return;
    };
    let status = child.wait().await;
    let code = match &status {
        Ok(status) => status.code().unwrap_or(-1),
        Err(_) => -1,
    };
    inner.code.store(code, Ordering::SeqCst);
    inner.fail_pending();
    if !inner.shutting.load(Ordering::SeqCst) {
        log::error("kernel", &format!("runtime exited (code {code})"));
        let payload = match &status {
            Ok(status) => json!({
                "code": code,
                "signal": process::signal_of(status),
                "message": format!("the runtime exited (code {code})"),
            }),
            Err(error) => json!({
                "code": code,
                "signal": Value::Null,
                "message": format!("waiting for the runtime failed: {error}"),
            }),
        };
        inner.publish("kernel.runtime.failed", payload);
    }
    if let Some(tx) = inner.ready.lock().unwrap().take() {
        let _ = tx.send(Err(format!("the runtime exited (code {code})")));
    }
    inner.exited.notify_one();
}

/// Spawns the Runtime and waits for its first capability snapshot.
pub(crate) async fn boot(
    mut spec: PluginSpec,
    limits: Limits,
    out: mpsc::Sender<Value>,
) -> Result<Trunk, String> {
    spec.id = protocol::RUNTIME.to_string();
    let (started, frames) = process::spawn(protocol::RUNTIME, &spec, &limits).map_err(|error| {
        format!(
            "cannot start the runtime {}: {error}",
            spec.command.display()
        )
    })?;
    let Process { child, pid, sink } = started;
    if let Some(pid) = pid {
        log::info("kernel", &format!("runtime={pid} started"));
    }
    let (ready_tx, ready_rx) = oneshot::channel();
    let inner: Trunk = Arc::new(Inner {
        sink,
        out,
        limits,
        bus: EventBus::new(),
        child: Mutex::new(child),
        pending: Mutex::new(HashMap::new()),
        table: Mutex::new(BTreeMap::new()),
        status: Mutex::new(json!({ "state": "starting", "ready": 0, "total": 0 })),
        ready: Mutex::new(Some(ready_tx)),
        exited: tokio::sync::Notify::new(),
        next_id: AtomicU64::new(1),
        next_stream: AtomicU64::new(0),
        shutting: AtomicBool::new(false),
        code: AtomicI32::new(0),
    });
    tokio::spawn(read_runtime(inner.clone(), frames));
    tokio::spawn(watch_exit(inner.clone()));

    let waiting = Duration::from_millis(inner.limits.runtime_ready_ms);
    match tokio::time::timeout(waiting, ready_rx).await {
        Ok(Ok(Ok(()))) => Ok(inner),
        Ok(Ok(Err(message))) => Err(message),
        Ok(Err(_)) => Err("the runtime closed before it was ready".to_string()),
        Err(_) => {
            inner.kill();
            Err(format!(
                "the runtime did not register within {} ms",
                inner.limits.runtime_ready_ms
            ))
        }
    }
}

/// Reads the front end's frames and forwards them to the Runtime.
async fn read_front_end(inner: Trunk) {
    let mut reader = BufReader::new(tokio::io::stdin());
    loop {
        match protocol::read_frame(&mut reader, inner.limits.max_frame_bytes).await {
            Ok(Some(payload)) => match protocol::parse_frame(&payload) {
                Ok(Incoming::Request {
                    id,
                    method: name,
                    params,
                }) => {
                    let inner = inner.clone();
                    tokio::spawn(async move {
                        inner.answer(id, &name, params).await;
                    });
                }
                Ok(Incoming::Notification {
                    method: name,
                    params,
                }) => {
                    if name == method::CANCEL {
                        inner.forward(notify(&name, params));
                    } else {
                        log::warn(
                            "kernel",
                            &format!(
                                "the front end sent a notification the kernel does not take: {name}"
                            ),
                        );
                    }
                }
                Ok(Incoming::Response { .. }) | Ok(Incoming::ErrorResponse { .. }) => {
                    log::warn(
                        "kernel",
                        "the front end sent a response; the kernel never calls it",
                    );
                }
                Err(error) => log::warn(
                    "kernel",
                    &format!(
                        "the front end sent an unparsable frame ({})",
                        codes::name(error.code)
                    ),
                ),
            },
            Ok(None) => {
                inner.shutting.store(true, Ordering::SeqCst);
                let _ = inner
                    .call(
                        "shutdown",
                        json!({ "reason": protocol::reason::KERNEL_EXIT }),
                    )
                    .await;
                return;
            }
            Err(error) => {
                log::warn("kernel", &format!("front end pipe: {error}"));
                return;
            }
        }
    }
}

async fn write_frames(mut queue: mpsc::Receiver<Value>) {
    let mut stdout = tokio::io::stdout();
    while let Some(frame) = queue.recv().await {
        let body = match serde_json::to_vec(&frame) {
            Ok(body) => body,
            Err(error) => {
                log::warn(
                    "kernel",
                    &format!("cannot encode a frame for the front end: {error}"),
                );
                continue;
            }
        };
        if protocol::write_frame(&mut stdout, &body).await.is_err() {
            break;
        }
    }
    let _ = stdout.flush().await;
}

/// Runs one kernel in runtime mode and returns its exit code.
pub async fn run(mut spec: PluginSpec, limits: Limits, config_path: &str) -> i32 {
    spec.env
        .insert("BLMOADLL_CONFIG".to_string(), config_path.to_string());
    let (out, queue) = mpsc::channel::<Value>(OUT_QUEUE);
    let writer = tokio::spawn(write_frames(queue));
    let trunk = match boot(spec, limits, out).await {
        Ok(trunk) => trunk,
        Err(message) => {
            log::error("kernel", &message);
            drop(writer);
            return 1;
        }
    };
    log::info("kernel", "runtime ready");
    tokio::spawn(read_front_end(trunk.clone()));

    trunk.exited.notified().await;
    let expected = trunk.shutting.load(Ordering::SeqCst);
    let code = trunk.code.load(Ordering::SeqCst);
    trunk.kill();
    let _ = tokio::time::timeout(TAIL_GRACE, writer).await;
    if !expected && code == 0 { 1 } else { code }
}

fn string_list(params: &Value, key: &str) -> Vec<String> {
    params
        .get(key)
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}
