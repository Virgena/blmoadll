//! The kernel: boot, route, relay, reload, shut down.
//!
//! It knows capability ids and which process serves one. It does not know what
//! a "model", a "session" or a "UI" is. The one place it exceeds that brief on
//! purpose is `io`: multiplexing stdin/stdout is a transport job, and the docs
//! say so out loud.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};

use protocol as proto;
use protocol::{HOST, Incoming, RpcError, codes, method};

use crate::events::{Budget, EventBus};
use crate::io::{self, Stdout};
use crate::process::{self, Frames};
use loader::graph::{self, Decl, Issue};
use loader::{Config, ConfigError, Limits, PluginSpec};
use loader::{RoutingTable, to_json};
use logger as log;

/// Interval between polls of the shutdown flow, the sweeper and the reloader.
const POLL: Duration = Duration::from_millis(20);

struct Instance {
    cfg: PluginSpec,
    process: process::Process,
    decl: Decl,
    /// Whole-table snapshot; reload replaces the `Arc`, never a single `Route`.
    view: Arc<RoutingTable>,
    waiting: HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>,
    next_id: u64,
    inflight: usize,
    started: bool,
    /// Set once the process is gone, holding `{plugin, exit_code, signal}`.
    gone: Option<Value>,
    /// We asked it to stop, so its exit is expected rather than a crash.
    shutting_down: bool,
    stop_trigger: Option<&'static str>,
    start_trigger: &'static str,
}

impl Instance {
    fn sink(&self) -> &process::Sink {
        &self.process.sink
    }
}

#[derive(Clone)]
struct Waiting {
    missing: Vec<String>,
    trigger: &'static str,
}

struct StreamState {
    caller: String,
    provider: String,
    provider_request_id: u64,
    seq: u64,
    dead: bool,
    last: Instant,
    /// Chunks held back while the caller's outbound queue is above the high
    /// watermark. This is the backpressure that applies to *every* consumer.
    pending: Vec<Value>,
    pending_bytes: usize,
}

#[derive(Default)]
struct IoState {
    stdin_owner: Option<String>,
    stdin_mode: String,
    stdout_owner: Option<String>,
}

struct State {
    config: Config,
    plugins: BTreeMap<String, Instance>,
    table: RoutingTable,
    blocked: BTreeMap<String, Waiting>,
    bus: EventBus,
    streams: HashMap<String, StreamState>,
    /// `(provider, provider request id)` for calls that asked for a stream.
    stream_calls: HashMap<(String, u64), String>,
    /// `(provider, provider-side stream id)` -> kernel-side stream id.
    upstream: HashMap<(String, String), String>,
    /// `(caller, caller request id)` -> `(provider, provider request id)`.
    calls: HashMap<(String, String), (String, u64)>,
    next_stream: u64,
    io: IoState,
    stdout: Stdout,
    /// The embedder's queue: chunks, stream errors and subscribed events.
    host: HostQueue,
    host_inflight: usize,
    next_host_call: u64,
    last_activity: Instant,
    inflight_total: usize,
    /// `--check` runs a lifecycle without lifecycle events and without `start`.
    check_mode: bool,
    /// The embedder talks to the kernel over a pipe, so the terminal - and with
    /// it the io primitives - belongs to the embedder. Claiming fd 0/1 here
    /// would corrupt the host protocol.
    host_mode: bool,
}

pub struct Kernel {
    state: Mutex<State>,
    attached: watch::Sender<bool>,
    shutdown_tx: mpsc::UnboundedSender<&'static str>,
    shutdown_rx: Mutex<Option<mpsc::UnboundedReceiver<&'static str>>>,
    exit_tx: Mutex<Option<oneshot::Sender<i32>>>,
    exit_rx: Mutex<Option<oneshot::Receiver<i32>>>,
    report: Mutex<Report>,
    host: Mutex<Option<Host>>,
    stopping: AtomicBool,
    /// Set by SIGHUP: reload once even though the bytes on disk are unchanged.
    force_reload: AtomicBool,
    restart_lock: tokio::sync::Mutex<()>,
    path: PathBuf,
}

/// What the CLI prints, in text or as JSON.
#[derive(Debug, Default, Clone)]
pub struct Report {
    pub code: i32,
    pub errors: Vec<Issue>,
    pub warnings: Vec<Issue>,
    pub plugins: Vec<Value>,
    pub capabilities: BTreeMap<String, Value>,
    pub start_order: Vec<String>,
    pub disabled: Vec<String>,
    pub blocked: BTreeMap<String, Vec<String>>,
    /// The config files this run was composed of, base first. A layered config
    /// is then visible in the report instead of assumed.
    pub sources: Vec<String>,
}

impl Report {
    pub fn to_json(&self) -> Value {
        let issues = |list: &[Issue]| -> Vec<Value> {
            list.iter()
                .map(|issue| {
                    let mut object = json!({ "code": issue.code, "message": issue.message });
                    if let Some(field) = &issue.field {
                        object["field"] = Value::String(field.clone());
                    }
                    object
                })
                .collect()
        };
        json!({
            "ok": self.errors.is_empty() && self.code == 0,
            "errors": issues(&self.errors),
            "warnings": issues(&self.warnings),
            "plugins": self.plugins,
            "capabilities": self.capabilities,
            "planned_start_order": self.start_order,
            "config_sources": self.sources,
            "disabled": self.disabled,
            "blocked": self.blocked,
        })
    }

    /// Human-readable summary, used for `--check` without `--json`.
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        for error in &self.errors {
            out.push_str(&format!(
                "error [{}] {}\n",
                codes::name(error.code),
                error.message
            ));
        }
        for warning in &self.warnings {
            out.push_str(&format!(
                "warning [{}] {}\n",
                codes::name(warning.code),
                warning.message
            ));
        }
        if !self.sources.is_empty() {
            out.push_str(&format!("config: {}\n", self.sources.join(" + ")));
        }
        if !self.disabled.is_empty() {
            out.push_str(&format!("disabled: {}\n", self.disabled.join(", ")));
        }
        for (id, missing) in &self.blocked {
            out.push_str(&format!("waiting: {id} needs {}\n", missing.join(", ")));
        }
        let order: Vec<String> = self.start_order.clone();
        out.push_str(&format!(
            "{} capabilities, {} plugins\nstart order: {}\n",
            self.capabilities.len(),
            self.plugins.len(),
            order.join(", ")
        ));
        for (capability, route) in &self.capabilities {
            out.push_str(&format!(
                "  {capability} = {}\n",
                route["plugin"].as_str().unwrap_or("?")
            ));
        }
        out
    }
}

fn from_config_error(error: ConfigError) -> Issue {
    Issue {
        code: codes::INVALID_CONFIG,
        message: error.message,
        field: error.field,
    }
}

fn str_field(params: &Value, key: &str) -> String {
    params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Runs one whole kernel lifetime: boot, serve, shut down. Returns the report.
pub async fn run(path: &Path, check: bool) -> Report {
    let env = |name: &str| std::env::var(name).ok();
    let config = match Config::load(path, &env) {
        Ok(config) => config,
        Err(error) => {
            let mut report = Report::default();
            report.errors.push(from_config_error(error));
            report.code = 1;
            return report;
        }
    };
    let kernel = match Kernel::boot(config, check).await {
        Ok(kernel) => kernel,
        Err(report) => return report,
    };
    if check {
        let mut report = kernel.report.lock().unwrap().clone();
        report.code = if report.errors.is_empty() { 0 } else { 1 };
        return report;
    }
    kernel.clone().serve();
    let code = kernel.wait_for_exit().await;
    let mut report = kernel.report.lock().unwrap().clone();
    report.code = code;
    report
}

impl Kernel {
    // ------------------------------------------------------------ small bits

    fn cfg(&self) -> Limits {
        self.state.lock().unwrap().config.limits.clone()
    }

    fn host_mode(&self) -> bool {
        self.state.lock().unwrap().host_mode
    }

    /// This kernel is somebody's subprocess: fd 0 and fd 1 are the host
    /// protocol, so there is no terminal left for a plugin to claim.
    fn no_terminal(name: &str) -> RpcError {
        RpcError::new(
            codes::METHOD_NOT_FOUND,
            format!(
                "{name} is not available: the kernel runs as a host subprocess, so its terminal belongs to the host"
            ),
        )
    }

    fn budget(&self) -> Budget {
        let kernel = self.cfg();
        Budget {
            len: kernel.event_queue_len,
            bytes: kernel.event_queue_bytes,
        }
    }

    fn ids(&self) -> Vec<String> {
        self.state.lock().unwrap().plugins.keys().cloned().collect()
    }

    fn send(&self, plugin: &str, frame: &Value) {
        let state = self.state.lock().unwrap();
        Self::deliver(&state, plugin, frame);
    }

    fn reply(&self, plugin: &str, id: Value, outcome: Result<Value, RpcError>) {
        let frame = match outcome {
            Ok(result) => proto::success(id, result),
            Err(error) => proto::failure(id, &error),
        };
        self.send(plugin, &frame);
    }

    fn touch(&self) {
        self.state.lock().unwrap().last_activity = Instant::now();
    }

    /// `-32011` when a capability's provider is known to be unusable.
    fn availability(&self, provider: &str) -> Option<RpcError> {
        let state = self.state.lock().unwrap();
        match state.plugins.get(provider) {
            Some(instance) if instance.gone.is_some() => {
                Some(process::gone(provider, instance.gone.as_ref().unwrap()))
            }
            Some(instance) if !instance.started => Some(RpcError::new(
                codes::NOT_STARTED,
                format!("`{provider}` has not started"),
            )),
            Some(_) => None,
            None => Some(process::gone(provider, &json!({ "plugin": provider }))),
        }
    }

    fn provider_timeout(&self, provider: &str) -> u64 {
        let state = self.state.lock().unwrap();
        let kernel = &state.config.limits;
        state
            .plugins
            .get(provider)
            .map(|instance| instance.cfg.timeouts.request(&kernel))
            .unwrap_or(kernel.request_timeout_ms)
    }

    fn plugin_report(&self) -> Vec<Value> {
        let state = self.state.lock().unwrap();
        state
            .plugins
            .iter()
            .map(|(id, instance)| {
                json!({
                    "id": id,
                    "command": instance.cfg.command.display().to_string(),
                    "provides": instance.decl.provides,
                    "injects": instance.decl.injects.iter()
                        .map(|r| json!({"capability": r.capability, "optional": r.optional}))
                        .collect::<Vec<_>>(),
                    "registrations": instance.decl.registrations.iter()
                        .map(|r| json!({"service": r.service, "capability": r.capability}))
                        .collect::<Vec<_>>(),
                    "host_calls": instance.decl.host_calls,
                })
            })
            .collect()
    }

    // ----------------------------------------------------------------- boot

    /// Boots a kernel for an embedder that lives in this process: it keeps the
    /// terminal, so `kernel.attach` and `kernel.write` work.
    pub async fn boot(config: Config, check: bool) -> Result<Arc<Kernel>, Report> {
        Kernel::boot_in(config, check, false).await
    }

    /// Boots a kernel for an embedder that talks to it over a pipe. The io
    /// primitives are refused there: fd 0 and fd 1 carry the host protocol.
    pub async fn boot_host(config: Config) -> Result<Arc<Kernel>, Report> {
        Kernel::boot_in(config, false, true).await
    }

    async fn boot_in(config: Config, check: bool, host: bool) -> Result<Arc<Kernel>, Report> {
        let kernel = Arc::new(Kernel::assemble(config, check, host));
        kernel.clone().start_processes().await?;

        // Seeded rather than blank: the report the kernel already carries names
        // the config layers this boot came from, and every failure below is
        // handed back to a caller who wants to know them.
        let mut report = kernel.report.lock().unwrap().clone();
        let mut decls: BTreeMap<String, Decl> = BTreeMap::new();
        for id in kernel.ids() {
            let (timeout, plugin_config) = {
                let state = kernel.state.lock().unwrap();
                let instance = state.plugins.get(&id).unwrap();
                (
                    instance.cfg.timeouts.initialize(&state.config.limits),
                    instance.cfg.config.clone(),
                )
            };
            let params = json!({
                "protocol": proto::PROTOCOL_VERSION,
                "plugin_id": id,
                "kernel_version": env!("CARGO_PKG_VERSION"),
                "config": plugin_config,
            });
            match kernel.call(&id, method::INITIALIZE, params, timeout).await {
                Ok(reply) => {
                    let protocol = reply.get("protocol").and_then(Value::as_u64).unwrap_or(0);
                    if protocol != proto::PROTOCOL_VERSION as u64 {
                        report.errors.push(
                            Issue::error(
                                codes::PROTOCOL_VERSION_MISMATCH,
                                format!(
                                    "`{id}` speaks protocol {protocol}, the kernel speaks {}",
                                    proto::PROTOCOL_VERSION
                                ),
                            )
                            .at(format!("plugins.{id}")),
                        );
                        return Err(kernel.cleanup(report).await);
                    }
                    match graph::decl_from_initialize(&id, &reply) {
                        Ok(decl) => {
                            decls.insert(id.clone(), decl);
                        }
                        Err(issue) => {
                            report.errors.push(issue.at(format!("plugins.{id}")));
                            return Err(kernel.cleanup(report).await);
                        }
                    }
                }
                Err(error) => {
                    report.errors.push(
                        Issue::error(
                            error.code,
                            format!("`{id}` failed to initialize: {}", error.message),
                        )
                        .at(format!("plugins.{id}")),
                    );
                    return Err(kernel.cleanup(report).await);
                }
            }
        }

        let (capability, disabled) = {
            let state = kernel.state.lock().unwrap();
            (
                state.config.capability.clone(),
                state.config.disabled.clone(),
            )
        };
        let declarations: Vec<Decl> = decls.values().cloned().collect();
        let outcome = graph::validate(&declarations, &capability, &disabled);
        report.warnings.extend(outcome.warnings.iter().cloned());
        if !outcome.errors.is_empty() {
            report.errors.extend(outcome.errors.iter().cloned());
            return Err(kernel.cleanup(report).await);
        }
        for warning in &report.warnings {
            log::warn("kernel", &warning.message);
        }
        let runnable: Vec<Decl> = declarations
            .iter()
            .filter(|decl| !outcome.blocked.contains_key(&decl.id))
            .cloned()
            .collect();
        let order = match graph::start_order(&runnable, &outcome.table) {
            Ok(order) => order,
            Err(issue) => {
                report.errors.push(issue);
                return Err(kernel.cleanup(report).await);
            }
        };

        // The table everything starts life with. It is a reference, not a
        // promise: reload swaps whole views later.
        let table = Arc::new(outcome.table.clone());
        {
            let mut state = kernel.state.lock().unwrap();
            state.table = outcome.table.clone();
            state.blocked = outcome
                .blocked
                .iter()
                .map(|(id, missing)| {
                    (
                        id.clone(),
                        Waiting {
                            missing: missing.clone(),
                            trigger: proto::trigger::BOOT,
                        },
                    )
                })
                .collect();
            for (id, instance) in state.plugins.iter_mut() {
                instance.view = table.clone();
                if let Some(decl) = decls.get(id) {
                    instance.decl = decl.clone();
                }
            }
        }

        report.capabilities = outcome
            .table
            .iter()
            .map(|(c, r)| (c.clone(), json!({ "plugin": r.plugin })))
            .collect();
        report.start_order = order.clone();
        report.disabled = disabled.iter().cloned().collect();
        report.blocked = outcome.blocked.clone();
        report.plugins = kernel.plugin_report();
        *kernel.report.lock().unwrap() = report.clone();

        if check {
            // `--check` never sends `start`, never touches io and never publishes
            // a lifecycle event. It still cleans up after itself.
            let gone = kernel.first_gone();
            kernel.shutdown_all(proto::reason::CHECK).await;
            if let Some(detail) = gone {
                let name = detail["plugin"].as_str().unwrap_or("?").to_string();
                report.errors.push(Issue::error(
                    codes::PROVIDER_UNAVAILABLE,
                    format!("`{name}` exited after initialize: in --check this means the plugin quit unexpectedly"),
                ));
            }
            report.plugins = kernel.plugin_report();
            report.code = if report.errors.is_empty() { 0 } else { 1 };
            *kernel.report.lock().unwrap() = report.clone();
            return Err(report);
        }

        for (id, missing) in &report.blocked {
            let payload = json!({
                "plugin": id,
                "missing": missing,
                "trigger": proto::trigger::BOOT,
            });
            let size = payload.to_string().len();
            kernel.publish_event("kernel.plugin.blocked", payload, size);
        }
        for id in &order {
            if let Err(error) = kernel.start_plugin(id, proto::trigger::BOOT).await {
                report.errors.push(Issue::error(
                    error.code,
                    format!("`{id}` failed to start: {}", error.message),
                ));
                return Err(kernel.cleanup(report).await);
            }
        }

        report.plugins = kernel.plugin_report();
        *kernel.report.lock().unwrap() = report;
        Ok(kernel)
    }

    fn assemble(config: Config, check: bool, host: bool) -> Kernel {
        let sources: Vec<String> = config
            .sources
            .iter()
            .map(|path| path.display().to_string())
            .collect();
        let report = Report {
            sources,
            ..Report::default()
        };
        let mut plugins = BTreeMap::new();
        for (id, cfg) in &config.plugins {
            plugins.insert(
                id.clone(),
                Instance {
                    cfg: cfg.clone(),
                    process: process::Process {
                        child: None,
                        pid: None,
                        sink: process::Sink::closed(),
                    },
                    decl: Decl {
                        id: id.clone(),
                        provides: Vec::new(),
                        injects: Vec::new(),
                        registrations: Vec::new(),
                        host_calls: Vec::new(),
                    },
                    view: Arc::new(RoutingTable::new()),
                    waiting: HashMap::new(),
                    next_id: 1,
                    inflight: 0,
                    started: false,
                    gone: None,
                    shutting_down: false,
                    stop_trigger: None,
                    start_trigger: proto::trigger::BOOT,
                },
            );
        }
        let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel();
        let (exit_tx, exit_rx) = oneshot::channel();
        let (attached, _) = watch::channel(false);
        // The embedder is a caller without a pipe: a bounded queue, counted in
        // bytes like a plugin's, so the same watermarks pause it.
        let capacity = config.limits.stream_buffer_chunks.max(1);
        let (host_tx, host_rx) = mpsc::channel::<Value>(capacity);
        let host_bytes = Arc::new(AtomicUsize::new(0));
        let path = config.path.clone();
        Kernel {
            state: Mutex::new(State {
                config,
                plugins,
                table: RoutingTable::new(),
                blocked: BTreeMap::new(),
                bus: EventBus::new(),
                streams: HashMap::new(),
                stream_calls: HashMap::new(),
                upstream: HashMap::new(),
                calls: HashMap::new(),
                next_stream: 1,
                io: IoState::default(),
                stdout: Stdout::new(),
                host: HostQueue {
                    tx: host_tx,
                    bytes: host_bytes.clone(),
                },
                host_inflight: 0,
                next_host_call: 0,
                last_activity: Instant::now(),
                inflight_total: 0,
                check_mode: check,
                host_mode: host,
            }),
            attached,
            shutdown_tx,
            shutdown_rx: Mutex::new(Some(shutdown_rx)),
            exit_tx: Mutex::new(Some(exit_tx)),
            exit_rx: Mutex::new(Some(exit_rx)),
            report: Mutex::new(report),
            host: Mutex::new(Some(Host {
                rx: host_rx,
                bytes: host_bytes,
            })),
            stopping: AtomicBool::new(false),
            force_reload: AtomicBool::new(false),
            restart_lock: tokio::sync::Mutex::new(()),
            path,
        }
    }

    async fn start_processes(self: Arc<Self>) -> Result<(), Report> {
        let plugs: Vec<(String, PluginSpec)> = {
            let state = self.state.lock().unwrap();
            state
                .plugins
                .iter()
                .map(|(id, i)| (id.clone(), i.cfg.clone()))
                .collect()
        };
        let config = self.state.lock().unwrap().config.clone();
        let mut frames: Vec<(String, Frames)> = Vec::new();
        for (id, cfg) in plugs {
            match process::spawn(&id, &cfg, &config.limits) {
                Ok((process, stream)) => {
                    self.state
                        .lock()
                        .unwrap()
                        .plugins
                        .get_mut(&id)
                        .unwrap()
                        .process = process;
                    frames.push((id, stream));
                }
                Err(error) => {
                    let mut report = self.report.lock().unwrap().clone();
                    report.errors.push(
                        Issue::error(
                            codes::INVALID_CONFIG,
                            format!("cannot start `{id}`: {error}"),
                        )
                        .at(format!("plugins.{id}")),
                    );
                    return Err(self.cleanup(report).await);
                }
            }
        }
        for (id, stream) in frames {
            self.clone().dispatch(id, stream);
        }
        Ok(())
    }

    fn first_gone(&self) -> Option<Value> {
        let state = self.state.lock().unwrap();
        state
            .plugins
            .values()
            .find_map(|instance| instance.gone.clone())
    }

    // -------------------------------------------------------------- dispatch

    fn dispatch(self: Arc<Self>, id: String, mut frames: Frames) {
        tokio::spawn(async move {
            while let Some(incoming) = frames.rx.recv().await {
                self.touch();
                match incoming {
                    Incoming::Request {
                        id: call_id,
                        method,
                        params,
                    } => {
                        let plugin = id.clone();
                        if method == method::KERNEL_PUBLISH {
                            // An event's meaning is its position in the sequence, and a
                            // spawn is scheduled by the runtime, not by the sender: two
                            // broadcasts from one plugin would race and could arrive
                            // inverted. The handler only queues frames, so it runs here,
                            // in the order the reader saw them.
                            self.clone()
                                .on_request(plugin, call_id, method, params)
                                .await;
                            continue;
                        }
                        let kernel = self.clone();
                        // Never block the reader: a plugin awaiting one call must
                        // still be able to receive the answer to another.
                        tokio::spawn(async move {
                            kernel.on_request(plugin, call_id, method, params).await
                        });
                    }
                    Incoming::Notification { method, params } => {
                        self.on_notification(&id, &method, &params)
                    }
                    Incoming::Response {
                        id: call_id,
                        result,
                    } => self.resolve(&id, &call_id, Ok(result)),
                    Incoming::ErrorResponse { id: call_id, error } => {
                        self.resolve(&id, &call_id, Err(error))
                    }
                }
            }
            let _ = frames.closed.await;
            self.clone().on_closed(&id).await;
        });
    }

    fn resolve(&self, plugin: &str, id: &Value, outcome: Result<Value, RpcError>) {
        let Some(numeric) = id.as_u64() else { return };
        let mut state = self.state.lock().unwrap();
        // A streaming provider names its own stream id in this reply. Wire the
        // mapping up here, synchronously, before the waiting task can be woken:
        // otherwise the very next chunk on this pipe could arrive orphaned.
        if let Ok(result) = &outcome {
            if let Some(caller_sid) = state.stream_calls.remove(&(plugin.to_string(), numeric)) {
                if let Some(provider_sid) = result.get("stream_id").and_then(Value::as_str) {
                    state
                        .upstream
                        .insert((plugin.to_string(), provider_sid.to_string()), caller_sid);
                }
            }
        }
        if let Some(instance) = state.plugins.get_mut(plugin) {
            if let Some(tx) = instance.waiting.remove(&numeric) {
                let _ = tx.send(outcome);
            }
        }
    }

    async fn on_request(self: Arc<Self>, plugin: String, id: Value, name: String, params: Value) {
        match name.as_str() {
            method::KERNEL_INVOKE => self.plugin_invoke(&plugin, id, params).await,
            method::KERNEL_PUBLISH => {
                let outcome = self.publish(&params);
                self.reply(&plugin, id, outcome);
            }
            method::KERNEL_SUBSCRIBE => {
                let outcome = self.subscribe_as(&plugin, &params);
                self.reply(&plugin, id, outcome);
            }
            method::KERNEL_UNSUBSCRIBE => {
                let outcome = self.unsubscribe_as(&params);
                self.reply(&plugin, id, outcome);
            }
            method::KERNEL_ATTACH => {
                let outcome = self.attach(&plugin, &params);
                self.reply(&plugin, id, outcome);
            }
            method::KERNEL_DETACH => {
                let outcome = self.detach(&plugin, &params);
                self.reply(&plugin, id, outcome);
            }
            method::KERNEL_WRITE => {
                let outcome = self.write(&params);
                self.reply(&plugin, id, outcome);
            }
            method::KERNEL_SHUTDOWN => {
                // Answer first: the caller is waiting on this very reply.
                self.reply(&plugin, id, Ok(json!({})));
                let _ = self.shutdown_tx.send(proto::reason::UI_QUIT);
            }
            other => self.reply(
                &plugin,
                id,
                Err(RpcError::new(
                    codes::METHOD_NOT_FOUND,
                    format!("unknown method {other}"),
                )),
            ),
        }
    }

    fn on_notification(&self, plugin: &str, name: &str, params: &Value) {
        match name {
            method::STREAM_CHUNK => self.on_provider_chunk(plugin, params),
            method::STREAM_ERROR => self.on_provider_error(plugin, params),
            method::CANCEL => self.on_cancel(plugin, params),
            method::KERNEL_LOG => self.on_log(plugin, params),
            _ => {}
        }
    }

    // ---------------------------------------------------------------- calls

    fn send_request(
        &self,
        plugin: &str,
        name: &str,
        params: Value,
    ) -> Option<(u64, oneshot::Receiver<Result<Value, RpcError>>)> {
        let mut state = self.state.lock().unwrap();
        let instance = state.plugins.get_mut(plugin)?;
        let id = instance.next_id;
        instance.next_id += 1;
        let (tx, rx) = oneshot::channel();
        instance.waiting.insert(id, tx);
        let frame = proto::request(id, name, params);
        instance.sink().send(&frame);
        Some((id, rx))
    }

    async fn call(
        &self,
        plugin: &str,
        name: &str,
        params: Value,
        timeout_ms: u64,
    ) -> Result<Value, RpcError> {
        let Some((id, rx)) = self.send_request(plugin, name, params) else {
            return Err(process::gone(plugin, &json!({ "plugin": plugin })));
        };
        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(RpcError::new(
                codes::INTERNAL_ERROR,
                "kernel dropped the response",
            )),
            Err(_) => {
                self.cancel_provider(plugin, id);
                Err(RpcError::new(
                    codes::REQUEST_TIMEOUT,
                    format!("{name} timed out"),
                ))
            }
        }
    }

    fn cancel_provider(&self, plugin: &str, request_id: u64) {
        let frame = proto::notify(method::CANCEL, json!({ "request_id": request_id }));
        let mut state = self.state.lock().unwrap();
        if let Some(instance) = state.plugins.get_mut(plugin) {
            instance.waiting.remove(&request_id);
        }
        if let Some(instance) = state.plugins.get(plugin) {
            instance.sink().send(&frame);
        }
    }

    /// Admission control, then the call itself.
    async fn plugin_invoke(self: &Arc<Self>, plugin: &str, id: Value, params: Value) {
        let kernel = self.cfg();
        let admitted = {
            let mut state = self.state.lock().unwrap();
            let limit = state
                .plugins
                .get(plugin)
                .map(|instance| instance.cfg.timeouts.max_inflight(&state.config.limits))
                .unwrap_or(kernel.max_inflight);
            let mine = state.plugins.get(plugin).map(|i| i.inflight).unwrap_or(0);
            if mine >= limit || state.inflight_total >= kernel.max_inflight_total {
                false
            } else {
                if let Some(instance) = state.plugins.get_mut(plugin) {
                    instance.inflight += 1;
                }
                state.inflight_total += 1;
                true
            }
        };
        if !admitted {
            let message =
                format!("`{plugin}` already has as many calls in flight as it is allowed");
            return self.reply(plugin, id, Err(RpcError::new(codes::OVERLOADED, message)));
        }
        self.plugin_invoke_inner(plugin, id, params).await;
        let mut state = self.state.lock().unwrap();
        if let Some(instance) = state.plugins.get_mut(plugin) {
            instance.inflight = instance.inflight.saturating_sub(1);
        }
        state.inflight_total = state.inflight_total.saturating_sub(1);
    }

    async fn plugin_invoke_inner(self: &Arc<Self>, plugin: &str, id: Value, params: Value) {
        let meta = params.get("meta").cloned().unwrap_or_else(|| json!({}));
        // Outbound calls must name their own request_id, or the caller could
        // never cancel them.
        let Some(request_id) = meta
            .get("request_id")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return self.reply(
                plugin,
                id,
                Err(RpcError::new(
                    codes::INVALID_PARAMS,
                    "meta.request_id is required",
                )),
            );
        };
        let capability = str_field(&params, "capability");
        let method_name = str_field(&params, "method");
        let inner = params.get("params").cloned().unwrap_or(Value::Null);
        let stream = meta.get("stream").and_then(Value::as_bool).unwrap_or(false);
        let timeout = meta.get("timeout_ms").and_then(Value::as_u64);

        // One snapshot per call: a call never sees half of one table.
        let route = {
            let state = self.state.lock().unwrap();
            state
                .plugins
                .get(plugin)
                .and_then(|i| i.view.get(&capability).cloned())
        };
        let Some(route) = route else {
            return self.reply(
                plugin,
                id,
                Err(RpcError::new(
                    codes::UNKNOWN_CAPABILITY,
                    format!("no provider is configured for `{capability}`"),
                )),
            );
        };
        let provider = route.plugin;
        if let Some(error) = self.availability(&provider) {
            return self.reply(plugin, id, Err(error));
        }

        if !stream {
            let outcome = self
                .forward(
                    plugin,
                    &request_id,
                    &provider,
                    &capability,
                    &method_name,
                    inner,
                    None,
                    timeout,
                )
                .await;
            return self.reply(plugin, id, outcome);
        }

        let caller_sid = self.start_stream(
            plugin,
            &provider,
            &capability,
            &method_name,
            inner,
            &request_id,
            timeout,
        );
        self.reply(plugin, id, Ok(json!({ "stream_id": caller_sid })));
    }

    /// Registers a caller-side stream, then fires the provider's call in the
    /// background. Shared by plugin callers and the embedder: only the caller
    /// label and the destination queue differ.
    #[allow(clippy::too_many_arguments)]
    fn start_stream(
        self: &Arc<Self>,
        caller: &str,
        provider: &str,
        capability: &str,
        method_name: &str,
        params: Value,
        caller_request_id: &str,
        timeout: Option<u64>,
    ) -> String {
        let caller_sid = {
            let mut state = self.state.lock().unwrap();
            let n = state.next_stream;
            state.next_stream += 1;
            format!("f-{n}")
        };
        let timeout_ms = timeout.unwrap_or_else(|| self.provider_timeout(provider));
        {
            let mut state = self.state.lock().unwrap();
            state.streams.insert(
                caller_sid.clone(),
                StreamState {
                    caller: caller.to_string(),
                    provider: provider.to_string(),
                    provider_request_id: 0,
                    seq: 0,
                    dead: false,
                    last: Instant::now(),
                    pending: Vec::new(),
                    pending_bytes: 0,
                },
            );
        }
        let kernel = self.clone();
        let sid = caller_sid.clone();
        let caller = caller.to_string();
        let provider = provider.to_string();
        let capability = capability.to_string();
        let method_name = method_name.to_string();
        let request_id = caller_request_id.to_string();
        tokio::spawn(async move {
            let outcome = kernel
                .forward(
                    &caller,
                    &request_id,
                    &provider,
                    &capability,
                    &method_name,
                    params,
                    Some(&sid),
                    Some(timeout_ms),
                )
                .await;
            match outcome {
                Ok(result) if result.get("stream_id").is_some() => {}
                Ok(_) => kernel.stream_error(
                    &sid,
                    &RpcError::new(
                        codes::INVALID_PARAMS,
                        "provider answered a stream request without a stream_id",
                    ),
                ),
                Err(error) => kernel.stream_error(&sid, &error),
            }
        });
        caller_sid
    }
    #[allow(clippy::too_many_arguments)]
    async fn forward(
        &self,
        caller: &str,
        caller_request_id: &str,
        provider: &str,
        capability: &str,
        method_name: &str,
        params: Value,
        stream_sid: Option<&str>,
        timeout: Option<u64>,
    ) -> Result<Value, RpcError> {
        let timeout_ms = timeout.unwrap_or_else(|| self.provider_timeout(provider));
        let registration = {
            let requested = params.get("capability").and_then(Value::as_str);
            let state = self.state.lock().unwrap();
            requested.and_then(|requested| {
                state.plugins.get(caller).and_then(|instance| {
                    instance
                        .decl
                        .registrations
                        .iter()
                        .find(|registration| {
                            registration.service == capability
                                && registration.capability == requested
                        })
                        .cloned()
                })
            })
        };
        let (provider_request_id, rx) = {
            let mut state = self.state.lock().unwrap();
            let Some(instance) = state.plugins.get_mut(provider) else {
                return Err(process::gone(provider, &json!({ "plugin": provider })));
            };
            if let Some(detail) = instance.gone.clone() {
                return Err(process::gone(provider, &detail));
            }
            if !instance.started {
                return Err(RpcError::new(
                    codes::NOT_STARTED,
                    format!("`{provider}` has not started"),
                ));
            }
            let id = instance.next_id;
            instance.next_id += 1;
            let (tx, rx) = oneshot::channel();
            instance.waiting.insert(id, tx);
            let mut meta = json!({
                "caller": caller,
                // Only the kernel's own fields are written here; the caller's
                // request identity and stream flag remain kernel-controlled.
                "request_id": id,
                "timeout_ms": timeout_ms,
                "stream": stream_sid.is_some(),
            });
            if let Some(registration) = &registration {
                meta["authorized_registration"] = json!({
                    "service": registration.service,
                    "capability": registration.capability,
                });
            }
            let frame = proto::request(
                id,
                method::INVOKE,
                json!({
                    "capability": capability,
                    "method": method_name,
                    "params": params,
                    "meta": meta,
                }),
            );
            instance.sink().send(&frame);
            state.calls.insert(
                (caller.to_string(), caller_request_id.to_string()),
                (provider.to_string(), id),
            );
            if let Some(sid) = stream_sid {
                state
                    .stream_calls
                    .insert((provider.to_string(), id), sid.to_string());
                if let Some(stream) = state.streams.get_mut(sid) {
                    stream.provider_request_id = id;
                }
            }
            (id, rx)
        };

        let outcome = match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(RpcError::new(
                codes::INTERNAL_ERROR,
                "kernel dropped the response",
            )),
            Err(_) => {
                self.cancel_provider(provider, provider_request_id);
                Err(RpcError::new(
                    codes::REQUEST_TIMEOUT,
                    format!("`{capability}/{method_name}` timed out"),
                ))
            }
        };
        {
            let mut state = self.state.lock().unwrap();
            state
                .calls
                .remove(&(caller.to_string(), caller_request_id.to_string()));
            state
                .stream_calls
                .remove(&(provider.to_string(), provider_request_id));
        }
        outcome
    }

    // -------------------------------------------------------------- streams

    /// True when this plugin is above a watermark, so a chunk must be held back
    /// rather than queued. Applies to every streaming consumer, not only the
    /// one that attached stdout.
    fn blocked(state: &State, plugin: &str, high: usize) -> bool {
        if plugin == HOST {
            return state.host.queued_bytes() >= high;
        }
        match state.plugins.get(plugin) {
            Some(instance) => {
                instance.sink().queued_bytes() >= high
                    || (state.io.stdout_owner.as_deref() == Some(plugin)
                        && state.stdout.queued_bytes() >= high)
            }
            None => false,
        }
    }

    fn on_provider_chunk(&self, provider: &str, params: &Value) {
        let Some(provider_sid) = params
            .get("stream_id")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return;
        };
        let done = params.get("done").and_then(Value::as_bool).unwrap_or(false);
        let data = params.get("data").cloned().unwrap_or(Value::Null);
        if done && !data.is_null() {
            log::warn(
                "kernel",
                &format!(
                    "plugin={provider} ended stream {provider_sid} with a payload; the terminal chunk carries null"
                ),
            );
        }
        let caller_sid = {
            let mut state = self.state.lock().unwrap();
            let key = (provider.to_string(), provider_sid.clone());
            let sid = state.upstream.get(&key).cloned();
            if done {
                state.upstream.remove(&key);
            }
            sid
        };
        let Some(caller_sid) = caller_sid else {
            log::warn(
                "kernel",
                &format!("plugin={provider} sent a chunk for unknown stream {provider_sid}"),
            );
            return;
        };
        self.push_chunk(&caller_sid, data, done);
    }

    fn on_provider_error(&self, provider: &str, params: &Value) {
        let Some(provider_sid) = params
            .get("stream_id")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return;
        };
        let error = RpcError {
            code: params
                .get("code")
                .and_then(Value::as_i64)
                .unwrap_or(codes::INTERNAL_ERROR),
            message: params
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("stream failed")
                .to_string(),
            data: params.get("data").cloned(),
        };
        let caller_sid = {
            let mut state = self.state.lock().unwrap();
            state.upstream.remove(&(provider.to_string(), provider_sid))
        };
        if let Some(caller_sid) = caller_sid {
            self.terminate_stream(&caller_sid, Some(error));
        }
    }

    /// Forwards one chunk, or buffers it when the caller is behind.
    fn push_chunk(&self, caller_sid: &str, data: Value, done: bool) {
        let kernel = self.cfg();
        let mut state = self.state.lock().unwrap();
        let Some(stream) = state.streams.get_mut(caller_sid) else {
            return;
        };
        if stream.dead {
            log::debug(
                "kernel",
                &format!("dropping a late chunk for the finished stream {caller_sid}"),
            );
            return;
        }
        let seq = stream.seq;
        stream.seq += 1;
        stream.last = Instant::now();
        stream.dead = done;
        let caller = stream.caller.clone();
        let frame = proto::notify(
            method::STREAM_CHUNK,
            json!({ "stream_id": caller_sid, "seq": seq, "data": data, "done": done }),
        );

        if Self::blocked(&state, &caller, kernel.queue_high_water_bytes) {
            let size = frame.to_string().len();
            let mut overflowed = false;
            if let Some(stream) = state.streams.get_mut(caller_sid) {
                stream.pending.push(frame);
                stream.pending_bytes += size;
                overflowed = stream.pending.len() > kernel.stream_buffer_chunks
                    || stream.pending_bytes > kernel.stream_buffer_bytes;
            }
            if overflowed {
                drop(state);
                log::warn(
                    "kernel",
                    &format!("stream {caller_sid} overflowed its buffer; terminating"),
                );
                self.terminate_stream(
                    caller_sid,
                    Some(RpcError::new(codes::OVERLOADED, "stream buffer overflowed")),
                );
            }
            return;
        }
        if !Self::deliver(&state, &caller, &frame) {
            drop(state);
            log::warn(
                "kernel",
                &format!("stream {caller_sid} outran its caller; terminating"),
            );
            self.terminate_stream(
                caller_sid,
                Some(RpcError::new(codes::OVERLOADED, "the caller queue is full")),
            );
        }
    }

    /// Ends a stream, cancelling the provider and (optionally) telling the caller.
    fn terminate_stream(&self, caller_sid: &str, error: Option<RpcError>) {
        let mut state = self.state.lock().unwrap();
        let Some(stream) = state.streams.remove(caller_sid) else {
            return;
        };
        state.upstream.retain(|_, sid| sid != caller_sid);
        state.stream_calls.retain(|(provider, id), _| {
            !(provider == &stream.provider && *id == stream.provider_request_id)
        });
        if stream.provider_request_id != 0 {
            let frame = proto::notify(
                method::CANCEL,
                json!({ "request_id": stream.provider_request_id }),
            );
            if let Some(instance) = state.plugins.get(&stream.provider) {
                instance.sink().send(&frame);
            }
        }
        if let Some(error) = error {
            let frame = proto::notify(
                method::STREAM_ERROR,
                json!({
                    "stream_id": caller_sid,
                    "code": error.code,
                    "message": error.message,
                    "data": error.data,
                }),
            );
            Self::deliver(&state, &stream.caller, &frame);
        }
    }

    fn stream_error(&self, caller_sid: &str, error: &RpcError) {
        self.terminate_stream(caller_sid, Some(error.clone()));
    }

    /// Backpressure release and idle timeouts. ponytail: one shared ticker
    /// instead of an event per queue; the cost is up to 100 ms of latency.
    fn sweep_streams(&self) {
        let kernel = self.cfg();
        let mut state = self.state.lock().unwrap();
        let ids: Vec<String> = state.streams.keys().cloned().collect();
        for id in ids {
            let Some(stream) = state.streams.get(&id) else {
                continue;
            };
            let caller = stream.caller.clone();
            let provider = stream.provider.clone();
            let provider_request_id = stream.provider_request_id;
            let idle_limit = state
                .plugins
                .get(&provider)
                .map(|i| i.cfg.timeouts.stream_idle(&kernel))
                .unwrap_or(kernel.stream_idle_timeout_ms);
            let idle = !stream.dead && stream.last.elapsed() > Duration::from_millis(idle_limit);
            let ready = !stream.pending.is_empty()
                && !Self::blocked(&state, &caller, kernel.queue_low_water_bytes);
            let finished = stream.dead && stream.pending.is_empty();
            let mut shed = false;

            if idle {
                drop(state);
                log::warn("kernel", &format!("stream {id} went idle; cancelling"));
                self.terminate_stream(
                    &id,
                    Some(RpcError::new(codes::REQUEST_TIMEOUT, "stream went idle")),
                );
                state = self.state.lock().unwrap();
                continue;
            }
            if ready {
                let frames: Vec<Value> = {
                    let stream = state.streams.get_mut(&id).unwrap();
                    stream.pending_bytes = 0;
                    std::mem::take(&mut stream.pending)
                };
                for frame in frames {
                    if !Self::deliver(&state, &caller, &frame) {
                        shed = true;
                        break;
                    }
                }
            }
            if shed {
                drop(state);
                log::warn(
                    "kernel",
                    &format!("stream {id} outran its caller; terminating"),
                );
                self.terminate_stream(
                    &id,
                    Some(RpcError::new(codes::OVERLOADED, "the caller queue is full")),
                );
                state = self.state.lock().unwrap();
                continue;
            }
            if finished {
                state.streams.remove(&id);
                state.stream_calls.retain(|(provider_id, request_id), _| {
                    !(provider_id == &provider && *request_id == provider_request_id)
                });
            }
        }
    }

    fn on_cancel(&self, plugin: &str, params: &Value) {
        let target = {
            let mut state = self.state.lock().unwrap();
            if let Some(sid) = params.get("stream_id").and_then(Value::as_str) {
                state
                    .streams
                    .remove(sid)
                    .map(|stream| (stream.provider, stream.provider_request_id))
            } else if let Some(request_id) = params.get("request_id").and_then(Value::as_str) {
                state
                    .calls
                    .remove(&(plugin.to_string(), request_id.to_string()))
            } else {
                None
            }
        };
        // Cancellation is a cooperative signal: the kernel stops forwarding and
        // tells the provider, but it cannot force the provider to stop working.
        if let Some((provider, request_id)) = target {
            if request_id != 0 {
                let frame = proto::notify(method::CANCEL, json!({ "request_id": request_id }));
                self.send(&provider, &frame);
            }
        }
    }

    // --------------------------------------------------------------- events

    fn publish_event(&self, topic: &str, payload: Value, size: usize) {
        let limits = self.budget();
        let state = self.state.lock().unwrap();
        let (events, notices) = state.bus.deliver(topic, &payload, size, &limits);
        for delivery in events.iter().chain(notices.iter()) {
            Self::deliver(&state, &delivery.plugin, &delivery.frame);
        }
        for delivery in &events {
            if let Some(sub_id) = delivery.sub_id {
                state.bus.ack(sub_id, delivery.size);
            }
        }
    }

    fn publish(&self, params: &Value) -> Result<Value, RpcError> {
        let topic = str_field(params, "topic");
        if topic.starts_with("kernel.") {
            return Err(RpcError {
                code: codes::INVALID_PARAMS,
                message: format!("`{topic}` is reserved for the kernel"),
                data: Some(json!({ "reason": "reserved_topic" })),
            });
        }
        let payload = params.get("payload").cloned().unwrap_or(Value::Null);
        let size = serde_json::to_string(&payload)
            .map(|text| text.len())
            .unwrap_or(0);
        let limit = self.cfg().event_payload_bytes;
        if size > limit {
            return Err(RpcError::new(
                codes::PAYLOAD_TOO_LARGE,
                format!("payload of {size} bytes exceeds event_payload_bytes ({limit})"),
            ));
        }
        self.publish_event(&topic, payload, size);
        Ok(json!({}))
    }

    fn subscribe_as(&self, plugin: &str, params: &Value) -> Result<Value, RpcError> {
        let patterns: Vec<String> = params
            .get("patterns")
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if patterns.is_empty() {
            return Err(RpcError::new(
                codes::INVALID_PARAMS,
                "patterns must not be empty",
            ));
        }
        let id = self.state.lock().unwrap().bus.subscribe(plugin, patterns);
        Ok(json!({ "subscription_id": format!("sub-{id}") }))
    }

    fn unsubscribe_as(&self, params: &Value) -> Result<Value, RpcError> {
        let raw = str_field(params, "subscription_id");
        let id: u64 = raw
            .strip_prefix("sub-")
            .unwrap_or(&raw)
            .parse()
            .map_err(|_| {
                RpcError::new(
                    codes::INVALID_PARAMS,
                    format!("bad subscription_id {raw:?}"),
                )
            })?;
        if self.state.lock().unwrap().bus.unsubscribe(id) {
            Ok(json!({}))
        } else {
            Err(RpcError::new(
                codes::UNKNOWN_SUBSCRIPTION,
                format!("no subscription {raw}"),
            ))
        }
    }

    fn on_log(&self, plugin: &str, params: &Value) {
        let level = params
            .get("level")
            .and_then(Value::as_str)
            .unwrap_or("info");
        let message = params.get("message").and_then(Value::as_str).unwrap_or("");
        let mut fields = params.get("fields").cloned().unwrap_or_else(|| json!({}));
        if let Some(object) = fields.as_object_mut() {
            object.insert("plugin".to_string(), Value::String(plugin.to_string()));
        }
        let rendered = log::format_line(level, "plugin", message, &fields);
        let limit = self.cfg().event_payload_bytes;
        if rendered.len() > limit {
            log::warn(
                "kernel",
                &format!("plugin={plugin} log dropped size={}", rendered.len()),
            );
            return;
        }
        log::emit(level, "plugin", message, fields);
    }

    // ------------------------------------------------------------------- io

    fn attach(&self, plugin: &str, params: &Value) -> Result<Value, RpcError> {
        if self.host_mode() {
            return Err(Self::no_terminal(method::KERNEL_ATTACH));
        }
        let stream = str_field(params, "stream");
        let mode = params
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("line")
            .to_string();
        {
            let mut state = self.state.lock().unwrap();
            match stream.as_str() {
                "stdin" => {
                    // Takeover is atomic: this happens in one lock, and the swap
                    // point is unique inside the kernel's single event demo.
                    if let Some(previous) = state.io.stdin_owner.clone() {
                        if previous != plugin {
                            let frame = proto::notify(
                                method::IO_DETACHED,
                                json!({ "stream": "stdin", "reason": "taken_over" }),
                            );
                            if let Some(instance) = state.plugins.get(&previous) {
                                instance.sink().send(&frame);
                            }
                        }
                    }
                    state.io.stdin_owner = Some(plugin.to_string());
                    state.io.stdin_mode = mode;
                }
                "stdout" => state.io.stdout_owner = Some(plugin.to_string()),
                other => {
                    return Err(RpcError::new(
                        codes::INVALID_PARAMS,
                        format!("unknown stream {other:?}"),
                    ));
                }
            }
        }
        self.sync_stdin();
        Ok(json!({}))
    }

    fn detach(&self, plugin: &str, params: &Value) -> Result<Value, RpcError> {
        if self.host_mode() {
            return Err(Self::no_terminal(method::KERNEL_DETACH));
        }
        {
            let mut state = self.state.lock().unwrap();
            match str_field(params, "stream").as_str() {
                "stdin" => {
                    if state.io.stdin_owner.as_deref() == Some(plugin) {
                        state.io.stdin_owner = None;
                    }
                }
                "stdout" => {
                    if state.io.stdout_owner.as_deref() == Some(plugin) {
                        state.io.stdout_owner = None;
                    }
                }
                other => {
                    return Err(RpcError::new(
                        codes::INVALID_PARAMS,
                        format!("unknown stream {other:?}"),
                    ));
                }
            }
        }
        // Detaching is explicitly not a shutdown trigger.
        self.sync_stdin();
        Ok(json!({}))
    }

    fn sync_stdin(&self) {
        let reading = self.state.lock().unwrap().io.stdin_owner.is_some();
        let _ = self.attached.send(reading);
    }

    fn write(&self, params: &Value) -> Result<Value, RpcError> {
        if self.host_mode() {
            return Err(Self::no_terminal(method::KERNEL_WRITE));
        }
        if str_field(params, "stream") != "stdout" {
            return Err(RpcError::new(
                codes::INVALID_PARAMS,
                "kernel.write only writes to stdout",
            ));
        }
        let data = str_field(params, "data");
        let kernel = self.cfg();
        let state = self.state.lock().unwrap();
        state
            .stdout
            .write(
                &data,
                kernel.io_write_queue_bytes,
                kernel.event_payload_bytes,
            )
            .map_err(|code| {
                RpcError::new(
                    code,
                    format!(
                        "stdout is not accepting writes right now ({})",
                        codes::name(code)
                    ),
                )
            })?;
        Ok(json!({}))
    }

    /// Called by the stdin reader; never before the first `attach{stdin}`.
    pub fn stdin_bytes(&self, bytes: &[u8]) {
        let owner = {
            let state = self.state.lock().unwrap();
            match state.io.stdin_owner.clone() {
                Some(owner) => owner,
                None => {
                    log::warn(
                        "kernel",
                        "stdin data arrived with no attach owner; discarded",
                    );
                    return;
                }
            }
        };
        let text = String::from_utf8_lossy(bytes);
        let frame = proto::notify(
            method::IO_DATA,
            json!({ "stream": "stdin", "data": text, "eof": false }),
        );
        self.send(&owner, &frame);
    }

    /// End of the kernel's stdin: hand over the sentinel, then begin shutdown.
    pub fn stdin_eof(&self) {
        let owner = self.state.lock().unwrap().io.stdin_owner.clone();
        if let Some(owner) = owner {
            let frame = proto::notify(
                method::IO_DATA,
                json!({ "stream": "stdin", "data": "", "eof": true }),
            );
            self.send(&owner, &frame);
        }
        let _ = self.shutdown_tx.send(proto::reason::KERNEL_EXIT);
    }

    // ------------------------------------------------------------ lifecycle

    async fn start_plugin(&self, id: &str, trigger: &'static str) -> Result<(), RpcError> {
        let (caps, timeout, pid, check, cwd, command, args) = {
            let state = self.state.lock().unwrap();
            let instance = match state.plugins.get(id) {
                Some(instance) => instance,
                None => {
                    return Err(RpcError::new(
                        codes::INVALID_PARAMS,
                        format!("no plugin `{id}`"),
                    ));
                }
            };
            (
                to_json(&state.table),
                instance.cfg.timeouts.start(&state.config.limits),
                instance.process.pid,
                state.check_mode,
                instance.cfg.cwd.display().to_string(),
                instance.cfg.command.display().to_string(),
                instance.cfg.args.clone(),
            )
        };
        // The snapshot handed over here is a starting reference; changes arrive
        // as `kernel.capabilities.changed`.
        self.call(id, method::START, json!({ "capabilities": caps }), timeout)
            .await?;
        {
            let mut state = self.state.lock().unwrap();
            let instance = state.plugins.get_mut(id).unwrap();
            instance.started = true;
            instance.start_trigger = trigger;
        }
        if !check {
            let payload = json!({
                "plugin": id,
                "pid": pid,
                "trigger": trigger,
                "cwd": cwd,
                "command": command,
                "args": args,
            });
            let size = payload.to_string().len();
            self.publish_event("kernel.plugin.started", payload, size);
        }
        Ok(())
    }

    fn send_shutdown(&self, plugin: &str, reason: &str, trigger: Option<&'static str>) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(instance) = state.plugins.get_mut(plugin) else {
            return false;
        };
        if instance.gone.is_some() || instance.shutting_down {
            return false;
        }
        instance.shutting_down = true;
        instance.stop_trigger = trigger;
        let id = instance.next_id;
        instance.next_id += 1;
        let (tx, _rx) = oneshot::channel();
        instance.waiting.insert(id, tx);
        let frame = proto::request(id, method::SHUTDOWN, json!({ "reason": reason }));
        instance.sink().send(&frame);
        true
    }

    fn grace_deadline(&self) -> Instant {
        let grace = {
            let state = self.state.lock().unwrap();
            state
                .plugins
                .values()
                .map(|i| i.cfg.timeouts.shutdown_grace(&state.config.limits))
                .max()
                .unwrap_or(5000)
        };
        Instant::now() + Duration::from_millis(grace)
    }

    async fn wait_until_gone(&self) -> usize {
        let deadline = self.grace_deadline();
        loop {
            let alive = {
                let state = self.state.lock().unwrap();
                state.plugins.values().filter(|i| i.gone.is_none()).count()
            };
            if alive == 0 || Instant::now() >= deadline {
                return alive;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    async fn force_kill_remaining(&self) -> usize {
        let mut killed = 0;
        {
            let mut state = self.state.lock().unwrap();
            for instance in state.plugins.values_mut() {
                if instance.gone.is_none() {
                    instance.process.force_kill();
                    killed += 1;
                }
            }
        }
        tokio::time::sleep(POLL).await;
        killed
    }

    async fn shutdown_all(&self, reason: &str) {
        for id in self.ids() {
            self.send_shutdown(&id, reason, None);
        }
        self.wait_until_gone().await;
        self.force_kill_remaining().await;
    }

    async fn cleanup(&self, mut report: Report) -> Report {
        self.shutdown_all(proto::reason::KERNEL_EXIT).await;
        report.code = 1;
        *self.report.lock().unwrap() = report.clone();
        report
    }

    pub async fn wait_for_exit(&self) -> i32 {
        let rx = self.exit_rx.lock().unwrap().take();
        match rx {
            Some(rx) => rx.await.unwrap_or(2),
            None => 0,
        }
    }

    fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    pub fn serve(self: Arc<Self>) {
        // stdin comes up only when somebody attaches; until then the OS keeps the
        // pipe's bytes, so `echo hi | eggshell run` loses nothing.
        let chunk = self.cfg().io_line_bytes;
        io::start_stdin(self.clone(), self.attached.subscribe(), chunk);

        if let Some(rx) = self.shutdown_rx.lock().unwrap().take() {
            let kernel = self.clone();
            tokio::spawn(async move {
                let code = kernel.clone().shutdown_flow(rx).await;
                kernel.stopping.store(true, Ordering::SeqCst);
                if let Some(tx) = kernel.exit_tx.lock().unwrap().take() {
                    let _ = tx.send(code);
                }
            });
        }

        {
            let kernel = self.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(POLL * 5);
                loop {
                    ticker.tick().await;
                    if kernel.is_stopping() {
                        return;
                    }
                    kernel.sweep_streams();
                }
            });
        }

        {
            let kernel = self.clone();
            tokio::spawn(async move { kernel.reload_loop().await });
        }

        #[cfg(unix)]
        {
            let kernel = self.clone();
            tokio::spawn(async move {
                use tokio::signal::unix::{SignalKind, signal};
                let mut hup = match signal(SignalKind::hangup()) {
                    Ok(stream) => stream,
                    Err(_) => return,
                };
                while hup.recv().await.is_some() {
                    kernel.force_reload.store(true, Ordering::SeqCst);
                }
            });
        }

        let kernel = self.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = kernel.shutdown_tx.send(proto::reason::UI_QUIT);
            }
        });
    }

    /// The one shutdown path: `kernel.shutdown`, stdin EOF and signals share it.
    async fn shutdown_flow(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<&'static str>) -> i32 {
        let reason = rx.recv().await.unwrap_or(proto::reason::KERNEL_EXIT);
        let kernel = self.cfg();
        let deadline = Instant::now() + Duration::from_millis(kernel.drain_ms);
        let idle = Duration::from_millis(kernel.io_eof_idle_ms);
        let mut truncated = false;
        loop {
            let (inflight, quiet) = {
                let state = self.state.lock().unwrap();
                (state.inflight_total, state.last_activity.elapsed())
            };
            if inflight == 0 && quiet >= idle {
                break;
            }
            if Instant::now() >= deadline {
                // Known cost, documented: a long streamed reply can still be cut
                // off here, and that is an exit code 2.
                truncated = inflight > 0;
                if truncated {
                    log::warn(
                        "kernel",
                        &format!(
                            "drain budget of {} ms expired with {inflight} call(s) in flight",
                            kernel.drain_ms
                        ),
                    );
                }
                break;
            }
            tokio::time::sleep(POLL).await;
        }

        for id in self.ids() {
            self.send_shutdown(&id, reason, None);
        }
        self.wait_until_gone().await;
        let killed = self.force_kill_remaining().await;
        if killed > 0 || truncated { 2 } else { 0 }
    }

    // ---------------------------------------------------------- hot reload

    async fn reload_loop(self: Arc<Self>) {
        let mut last = Config::fingerprint(&self.path);
        let mut ticker = tokio::time::interval(Duration::from_millis(1000));
        loop {
            ticker.tick().await;
            if self.is_stopping() {
                return;
            }
            let forced = self.force_reload.swap(false, Ordering::SeqCst);
            // Every layer counts, not just the entry file: editing the base a
            // local overlay extends is as much a config change as editing the
            // overlay itself.
            let hash = Config::fingerprint(&self.path);
            if !forced && hash == last {
                continue;
            }
            // Advance the hash before trying: a config that fails stays failed
            // until the file actually changes again, instead of retrying forever.
            last = hash;
            self.clone().reload().await;
        }
    }

    async fn reload(self: Arc<Self>) {
        let env = |name: &str| std::env::var(name).ok();
        let fresh = match Config::load(&self.path, &env) {
            Ok(config) => config,
            Err(error) => {
                log::error(
                    "kernel",
                    &format!("reload rejected: {error}; keeping the running config"),
                );
                return;
            }
        };

        let old = self.state.lock().unwrap().config.clone();
        let ids: Vec<String> = old.plugins.keys().cloned().collect();
        let added: Vec<String> = fresh
            .plugins
            .keys()
            .filter(|id| !ids.contains(id))
            .cloned()
            .collect();
        let removed: Vec<String> = ids
            .iter()
            .filter(|id| !fresh.plugins.contains_key(*id))
            .cloned()
            .collect();
        let changed: Vec<String> = ids
            .iter()
            .filter(|id| match fresh.plugins.get(*id) {
                Some(cfg) => old.plugins.get(*id) != Some(cfg),
                None => false,
            })
            .cloned()
            .collect();

        if added.is_empty() && changed.is_empty() && removed.is_empty() {
            // `[capability]` only: no process is spawned, started or drained.
            let declarations: Vec<Decl> = {
                let state = self.state.lock().unwrap();
                state.plugins.values().map(|i| i.decl.clone()).collect()
            };
            let outcome = graph::validate(&declarations, &fresh.capability, &fresh.disabled);
            if !outcome.errors.is_empty() {
                log::error(
                    "kernel",
                    &format!(
                        "reload rejected: {}; keeping the running config",
                        outcome.errors[0].message
                    ),
                );
                return;
            }
            let table = Arc::new(outcome.table.clone());
            {
                let mut state = self.state.lock().unwrap();
                state.table = outcome.table.clone();
                state.config = fresh.clone();
                for instance in state.plugins.values_mut() {
                    instance.view = table.clone();
                }
            }
            self.clone()
                .settle(&fresh, &[], proto::trigger::CONFIG)
                .await;
            self.publish_event(
                "kernel.capabilities.changed",
                json!({ "capabilities": to_json(&outcome.table) }),
                128,
            );
            self.publish_event(
                "kernel.config.reloaded",
                json!({ "path": self.path.display().to_string() }),
                64,
            );
            log::info(
                "kernel",
                "reloaded the routing table only; no process was restarted",
            );
            return;
        }

        log::info(
            "kernel",
            &format!(
                "reload: +{} ~{} -{}",
                added.len(),
                changed.len(),
                removed.len()
            ),
        );

        let new_ids: Vec<String> = added.iter().chain(changed.iter()).cloned().collect();
        for id in &changed {
            if let Err(error) = self.stop_plugin(id, proto::trigger::CONFIG).await {
                log::error(
                    "kernel",
                    &format!(
                        "reload rejected: {}; keeping the running config",
                        error.message
                    ),
                );
                return;
            }
        }
        if let Err(error) = self
            .clone()
            .bring_up(&new_ids, &fresh, &removed, proto::trigger::CONFIG)
            .await
        {
            log::error(
                "kernel",
                &format!(
                    "reload rejected: {}; keeping the running config",
                    error.message
                ),
            );
            return;
        }
        self.publish_event(
            "kernel.config.reloaded",
            json!({ "path": self.path.display().to_string() }),
            64,
        );
        log::info("kernel", "reload applied");

        self.clone()
            .settle(&fresh, &removed, proto::trigger::CONFIG)
            .await;
        if !removed.is_empty() {
            let kernel = self.clone();
            let removed = removed.clone();
            tokio::spawn(async move { kernel.retire(removed).await });
        }
    }

    async fn bring_up(
        self: Arc<Self>,
        new_ids: &[String],
        fresh: &Config,
        removed: &[String],
        trigger: &'static str,
    ) -> Result<(), RpcError> {
        let mut frames: Vec<(String, Frames)> = Vec::new();
        for id in new_ids {
            let cfg = fresh.plugins.get(id).unwrap().clone();
            match process::spawn(id, &cfg, &fresh.limits) {
                Ok((process, stream)) => {
                    self.state.lock().unwrap().plugins.insert(
                        id.clone(),
                        Instance {
                            cfg,
                            process,
                            decl: Decl {
                                id: id.clone(),
                                provides: Vec::new(),
                                injects: Vec::new(),
                                registrations: Vec::new(),
                                host_calls: Vec::new(),
                            },
                            view: Arc::new(RoutingTable::new()),
                            waiting: HashMap::new(),
                            next_id: 1,
                            inflight: 0,
                            started: false,
                            gone: None,
                            shutting_down: false,
                            stop_trigger: None,
                            start_trigger: proto::trigger::BOOT,
                        },
                    );
                    frames.push((id.clone(), stream));
                }
                Err(error) => {
                    self.abort_reload(&new_ids, trigger).await;
                    return Err(RpcError::new(
                        codes::INVALID_CONFIG,
                        format!("cannot start `{id}`: {error}"),
                    ));
                }
            }
        }
        for (id, stream) in frames {
            self.clone().dispatch(id, stream);
        }

        // 2. initialize the new instances, then check the whole new graph.
        let mut decls: BTreeMap<String, Decl> = {
            let state = self.state.lock().unwrap();
            state
                .plugins
                .iter()
                .filter(|(id, _)| !new_ids.contains(id) && !removed.contains(id))
                .map(|(id, instance)| (id.clone(), instance.decl.clone()))
                .collect()
        };
        for id in new_ids {
            let (timeout, plugin_config) = {
                let state = self.state.lock().unwrap();
                let instance = state.plugins.get(id).unwrap();
                (
                    instance.cfg.timeouts.initialize(&state.config.limits),
                    instance.cfg.config.clone(),
                )
            };
            let params = json!({
                "protocol": proto::PROTOCOL_VERSION,
                "plugin_id": id,
                "kernel_version": env!("CARGO_PKG_VERSION"),
                "config": plugin_config,
            });
            match self.call(id, method::INITIALIZE, params, timeout).await {
                Ok(reply) => match graph::decl_from_initialize(id, &reply) {
                    Ok(decl) => {
                        decls.insert(id.clone(), decl);
                    }
                    Err(issue) => {
                        self.abort_reload(&new_ids, trigger).await;
                        return Err(RpcError::new(
                            issue.code,
                            format!("`{id}`: {}", issue.message),
                        ));
                    }
                },
                Err(error) => {
                    self.abort_reload(&new_ids, trigger).await;
                    return Err(RpcError::new(
                        error.code,
                        format!("`{id}` failed to initialize: {}", error.message),
                    ));
                }
            }
        }

        let declarations: Vec<Decl> = decls.values().cloned().collect();
        let outcome = graph::validate(&declarations, &fresh.capability, &fresh.disabled);
        if !outcome.errors.is_empty() {
            self.abort_reload(&new_ids, trigger).await;
            return Err(RpcError::new(
                outcome.errors[0].code,
                outcome.errors[0].message.clone(),
            ));
        }
        let order = match graph::start_order(&declarations, &outcome.table) {
            Ok(order) => order,
            Err(issue) => {
                self.abort_reload(&new_ids, trigger).await;
                return Err(RpcError::new(issue.code, issue.message));
            }
        };

        // New instances see the new table; untouched ones keep the old one, so
        // nothing ever sees an empty routing slot.
        let new_table = Arc::new(outcome.table.clone());
        {
            let mut state = self.state.lock().unwrap();
            for id in new_ids {
                if let Some(decl) = decls.get(id) {
                    if let Some(instance) = state.plugins.get_mut(id) {
                        instance.decl = decl.clone();
                    }
                }
                if let Some(instance) = state.plugins.get_mut(id) {
                    instance.view = new_table.clone();
                }
            }
        }
        let blocked = outcome.blocked.clone();
        for id in order
            .iter()
            .filter(|id| new_ids.contains(id) && !blocked.contains_key(*id))
        {
            if let Err(error) = self.start_plugin(id, trigger).await {
                self.abort_reload(&new_ids, trigger).await;
                return Err(RpcError::new(
                    error.code,
                    format!("`{id}` failed to start: {}", error.message),
                ));
            }
        }

        {
            let mut state = self.state.lock().unwrap();
            state.table = outcome.table.clone();
            state.config = fresh.clone();
            for instance in state.plugins.values_mut() {
                instance.view = new_table.clone();
            }
        }
        self.publish_event(
            "kernel.capabilities.changed",
            json!({ "capabilities": to_json(&outcome.table) }),
            128,
        );
        Ok(())
    }

    /// Undo a failed reload: drop the new instances, keep the old world serving.
    async fn abort_reload(&self, ids: &[String], trigger: &'static str) {
        for id in ids {
            self.send_shutdown(id, proto::reason::RELOAD, Some(trigger));
        }
        tokio::time::sleep(POLL).await;
        let mut state = self.state.lock().unwrap();
        for id in ids {
            if let Some(instance) = state.plugins.get_mut(id) {
                if instance.gone.is_none() {
                    instance.process.force_kill();
                }
            }
            state.plugins.remove(id);
        }
    }

    async fn retire(&self, ids: Vec<String>) {
        let deadline = Instant::now() + Duration::from_millis(self.cfg().drain_ms);
        loop {
            let busy = {
                let state = self.state.lock().unwrap();
                ids.iter()
                    .filter_map(|id| state.plugins.get(id))
                    .any(|instance| instance.inflight > 0)
            };
            if !busy || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
        for id in &ids {
            self.send_shutdown(id, proto::reason::RELOAD, Some(proto::trigger::CONFIG));
        }
        let deadline = Instant::now() + Duration::from_millis(self.cfg().shutdown_grace_ms);
        loop {
            let alive = {
                let state = self.state.lock().unwrap();
                ids.iter()
                    .filter_map(|id| state.plugins.get(id))
                    .filter(|instance| instance.gone.is_none())
                    .count()
            };
            if alive == 0 || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
        let mut state = self.state.lock().unwrap();
        for id in &ids {
            if let Some(instance) = state.plugins.get_mut(id) {
                if instance.gone.is_none() {
                    instance.process.force_kill();
                }
            }
            state.plugins.remove(id);
        }
    }

    /// Waiting plugins stop when their dependencies leave and start again when
    /// those come back. `leaving` names the ids this reload retires anyway.
    async fn settle(
        self: Arc<Self>,
        fresh: &Config,
        leaving: &[String],
        trigger: &'static str,
    ) -> BTreeSet<String> {
        let (declarations, before) = {
            let state = self.state.lock().unwrap();
            let declarations: Vec<Decl> = state
                .plugins
                .iter()
                .filter(|(id, _)| !leaving.contains(id))
                .map(|(_, instance)| instance.decl.clone())
                .collect();
            (declarations, state.blocked.clone())
        };
        let outcome = graph::validate(&declarations, &fresh.capability, &fresh.disabled);
        if !outcome.errors.is_empty() {
            log::error(
                "kernel",
                &format!("waiting pass rejected: {}", outcome.errors[0].message),
            );
            return BTreeSet::new();
        }
        let blocked = outcome.blocked.clone();
        let mut stop: Vec<String> = Vec::new();
        let mut wake: Vec<String> = Vec::new();
        let mut respawn: Vec<String> = Vec::new();
        {
            let state = self.state.lock().unwrap();
            for (id, instance) in &state.plugins {
                if leaving.contains(id) {
                    continue;
                }
                let waited_before = before.contains_key(id);
                if blocked.contains_key(id) {
                    if !waited_before && instance.started && instance.gone.is_none() {
                        stop.push(id.clone());
                    }
                    continue;
                }
                if waited_before && !instance.started {
                    if instance.gone.is_none() {
                        wake.push(id.clone());
                    } else {
                        respawn.push(id.clone());
                    }
                }
            }
        }
        for id in &stop {
            self.stop_waiting(id, trigger).await;
        }
        for id in &wake {
            if let Err(error) = self.start_plugin(id, trigger).await {
                log::error(
                    "kernel",
                    &format!(
                        "`{id}` could not leave the waiting state: {}",
                        error.message
                    ),
                );
            }
        }
        for id in &respawn {
            if let Err(error) = self
                .clone()
                .bring_up(std::slice::from_ref(id), fresh, &[], trigger)
                .await
            {
                log::error(
                    "kernel",
                    &format!(
                        "`{id}` could not leave the waiting state: {}",
                        error.message
                    ),
                );
            }
        }
        {
            let mut state = self.state.lock().unwrap();
            state.blocked = blocked
                .iter()
                .map(|(id, missing)| {
                    (
                        id.clone(),
                        Waiting {
                            missing: missing.clone(),
                            trigger,
                        },
                    )
                })
                .collect();
        }
        for (id, missing) in &blocked {
            if before.contains_key(id) {
                continue;
            }
            let payload = json!({ "plugin": id, "missing": missing, "trigger": trigger });
            let size = payload.to_string().len();
            self.publish_event("kernel.plugin.blocked", payload, size);
        }
        blocked.into_keys().collect()
    }

    /// Stops a plugin that just became a waiting one, keeping its entry.
    async fn stop_waiting(&self, id: &str, trigger: &'static str) {
        let grace = {
            let state = self.state.lock().unwrap();
            match state.plugins.get(id) {
                Some(instance) => instance.cfg.timeouts.shutdown_grace(&state.config.limits),
                None => return,
            }
        };
        self.send_shutdown(id, proto::reason::RELOAD, Some(trigger));
        if !self.await_gone(id, grace).await {
            {
                let mut state = self.state.lock().unwrap();
                if let Some(instance) = state.plugins.get_mut(id) {
                    instance.process.force_kill();
                }
            }
            self.await_gone(id, grace).await;
        }
        if let Some(instance) = self.state.lock().unwrap().plugins.get_mut(id) {
            instance.started = false;
        }
    }
    async fn restart_plugin(
        self: Arc<Self>,
        id: &str,
        trigger: &'static str,
    ) -> Result<(), RpcError> {
        let _guard = self.restart_lock.lock().await;
        let fresh = self.state.lock().unwrap().config.clone();
        if !fresh.plugins.contains_key(id) {
            return Err(RpcError::new(
                codes::INVALID_PARAMS,
                format!("no plugin `{id}`"),
            ));
        }
        self.stop_plugin(id, trigger).await?;
        let outcome = self
            .clone()
            .bring_up(&[id.to_string()], &fresh, &[], trigger)
            .await;
        if outcome.is_ok() {
            self.clone().settle(&fresh, &[], trigger).await;
        }
        if let Err(error) = &outcome {
            log::error(
                "kernel",
                &format!(
                    "restart rejected: {}; `{id}` stays unavailable until the next restart",
                    error.message
                ),
            );
        }
        outcome.map_err(|error| RpcError {
            code: codes::PROVIDER_UNAVAILABLE,
            message: format!("`{id}` failed to restart: {}", error.message),
            data: Some(json!({ "plugin": id })),
        })
    }

    async fn stop_plugin(&self, id: &str, trigger: &'static str) -> Result<(), RpcError> {
        let grace = {
            let state = self.state.lock().unwrap();
            match state.plugins.get(id) {
                Some(instance) => instance.cfg.timeouts.shutdown_grace(&state.config.limits),
                None => return Ok(()),
            }
        };
        self.send_shutdown(id, proto::reason::RELOAD, Some(trigger));
        if !self.await_gone(id, grace).await {
            {
                let mut state = self.state.lock().unwrap();
                if let Some(instance) = state.plugins.get_mut(id) {
                    instance.process.force_kill();
                }
            }
            if !self.await_gone(id, grace).await {
                return Err(RpcError {
                    code: codes::PROVIDER_UNAVAILABLE,
                    message: format!("`{id}` is still running; not restarting it"),
                    data: Some(json!({ "plugin": id })),
                });
            }
        }
        self.state.lock().unwrap().plugins.remove(id);
        Ok(())
    }

    async fn await_gone(&self, id: &str, grace_ms: u64) -> bool {
        let deadline = Instant::now() + Duration::from_millis(grace_ms);
        loop {
            let alive = {
                let state = self.state.lock().unwrap();
                matches!(state.plugins.get(id), Some(instance) if instance.gone.is_none())
            };
            if !alive || Instant::now() >= deadline {
                return !alive;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    async fn on_closed(self: Arc<Self>, plugin: &str) {
        // `wait` can yield, so the child is taken out of the lock first.
        let child = {
            let mut state = self.state.lock().unwrap();
            state
                .plugins
                .get_mut(plugin)
                .and_then(|i| i.process.child.take())
        };
        let mut child = child;
        let status = match child.as_mut() {
            Some(inner) => inner.wait().await.ok(),
            None => None,
        };
        {
            let mut state = self.state.lock().unwrap();
            if let Some(instance) = state.plugins.get_mut(plugin) {
                instance.process.child = child;
            }
        }
        let detail = process::exit_payload(plugin, status);

        let (expected, check_mode, orphans, failed, trigger) = {
            let mut state = self.state.lock().unwrap();
            state.bus.remove_plugin(plugin);
            let check_mode = state.check_mode;
            let mut expected = true;
            let mut failed = Vec::new();
            let mut trigger = None;
            if let Some(instance) = state.plugins.get_mut(plugin) {
                expected = instance.shutting_down;
                trigger = instance.stop_trigger;
                instance.gone = Some(detail.clone());
                instance.inflight = 0;
                failed.extend(std::mem::take(&mut instance.waiting).into_values());
            }
            let orphans: Vec<String> = state
                .streams
                .iter()
                .filter(|(_, stream)| stream.provider == plugin)
                .map(|(id, _)| id.clone())
                .collect();
            let as_caller: Vec<String> = state
                .streams
                .iter()
                .filter(|(_, stream)| stream.caller == plugin)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &as_caller {
                state.streams.remove(id);
                state.upstream.retain(|_, sid| sid != id);
            }
            state.inflight_total = state.plugins.values().map(|i| i.inflight).sum();
            (expected, check_mode, orphans, failed, trigger)
        };

        for tx in failed {
            let _ = tx.send(Err(process::gone(plugin, &detail)));
        }
        for id in &orphans {
            self.terminate_stream(id, Some(process::gone(plugin, &detail)));
        }

        if check_mode {
            return;
        }
        if !expected {
            log::warn(
                "kernel",
                &format!("plugin={plugin} exited unexpectedly: {detail}"),
            );
            self.publish_event(
                "kernel.plugin.degraded",
                json!({
                    "plugin": plugin,
                    "code": codes::PROVIDER_UNAVAILABLE,
                    "signal": detail["signal"],
                    "message": "plugin exited without a shutdown request",
                }),
                128,
            );
        }
        let mut payload = json!({
            "plugin": plugin,
            "reason": if expected { "shutdown" } else { "crash" },
        });
        if expected {
            if let Some(trigger) = trigger {
                payload["trigger"] = json!(trigger);
            }
        }
        let size = payload.to_string().len();
        self.publish_event("kernel.plugin.stopped", payload, size);
    }
}

// ----------------------------------------------------------------- the host

/// The kernel's end of the host queue. Bounded like a stream buffer and counted
/// in bytes, so the same watermarks pause a slow embedder instead of letting it
/// grow without limit.
struct HostQueue {
    tx: mpsc::Sender<Value>,
    bytes: Arc<AtomicUsize>,
}

impl HostQueue {
    /// `false` means the queue is full and the frame was shed.
    fn push(&self, frame: &Value) -> bool {
        if self.tx.try_send(frame.clone()).is_err() {
            return false;
        }
        self.bytes
            .fetch_add(frame.to_string().len(), Ordering::Relaxed);
        true
    }

    fn queued_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}

/// The embedder's end of the kernel. Everything the kernel would have written to
/// a plugin's pipe - stream chunks, stream errors and subscribed events -
/// arrives here instead. A host is a caller and nothing else: it provides no
/// capability, it has no process, and it cannot be a routing target.
pub struct Host {
    rx: mpsc::Receiver<Value>,
    bytes: Arc<AtomicUsize>,
}

impl Host {
    /// Waits for the next notification. `None` means the kernel is gone.
    pub async fn recv(&mut self) -> Option<Value> {
        let value = self.rx.recv().await?;
        let size = value.to_string().len();
        let queued = self.bytes.load(Ordering::Relaxed);
        self.bytes
            .store(queued.saturating_sub(size), Ordering::Relaxed);
        Some(value)
    }

    /// What the embedder has not read yet.
    pub fn queued_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }
}

impl Kernel {
    /// The routing table as `start` hands it to a plugin: capability id ->
    /// `{plugin}`. A host has no snapshot of its own, so this is how it
    /// learns what exists before any event arrives.
    pub fn capabilities(&self) -> Value {
        to_json(&self.state.lock().unwrap().table)
    }

    /// Takes the embedder's end. `None` after the first call.
    pub fn host(&self) -> Option<Host> {
        self.host.lock().unwrap().take()
    }

    /// Calls a capability on the embedder's behalf. The kernel writes
    /// `meta.caller = "host"`; the reply is the provider's business result.
    pub async fn invoke(
        self: &Arc<Self>,
        capability: &str,
        method_name: &str,
        params: Value,
    ) -> Result<Value, RpcError> {
        self.host_invoke(capability, method_name, params, false)
            .await
    }

    /// Same, with `stream`. The reply is the provider's `{stream_id}` and the
    /// chunks arrive on `Host::recv`, exactly as they would at a plugin's pipe.
    pub async fn invoke_stream(
        self: &Arc<Self>,
        capability: &str,
        method_name: &str,
        params: Value,
    ) -> Result<Value, RpcError> {
        self.host_invoke(capability, method_name, params, true)
            .await
    }

    /// Subscribes the embedder to event topics, exactly like a plugin would.
    pub fn subscribe(&self, patterns: &[String], replay: bool) -> Result<String, RpcError> {
        let reply = self.subscribe_as(HOST, &json!({ "patterns": patterns }))?;
        if replay {
            self.announce_running();
        }
        Ok(reply["subscription_id"]
            .as_str()
            .unwrap_or_default()
            .to_string())
    }

    fn announce_running(&self) {
        let live: Vec<Value> = {
            let state = self.state.lock().unwrap();
            state
                .plugins
                .iter()
                .filter(|(_, instance)| instance.started && instance.gone.is_none())
                .map(|(id, instance)| {
                    json!({
                        "plugin": id,
                        "pid": instance.process.pid,
                        "trigger": instance.start_trigger,
                        "cwd": instance.cfg.cwd.display().to_string(),
                        "command": instance.cfg.command.display().to_string(),
                        "args": instance.cfg.args,
                    })
                })
                .collect()
        };
        for payload in live {
            let size = payload.to_string().len();
            self.publish_event("kernel.plugin.started", payload, size);
        }
        let waiting: Vec<Value> = {
            let state = self.state.lock().unwrap();
            state
                .blocked
                .iter()
                .map(|(id, entry)| {
                    json!({ "plugin": id, "missing": entry.missing, "trigger": entry.trigger })
                })
                .collect()
        };
        for payload in waiting {
            let size = payload.to_string().len();
            self.publish_event("kernel.plugin.blocked", payload, size);
        }
    }

    pub fn unsubscribe(&self, subscription_id: &str) -> Result<(), RpcError> {
        self.unsubscribe_as(&json!({ "subscription_id": subscription_id }))
            .map(|_| ())
    }

    /// The host has no instance view: it routes through the kernel's own table.
    async fn host_invoke(
        self: &Arc<Self>,
        capability: &str,
        method_name: &str,
        params: Value,
        stream: bool,
    ) -> Result<Value, RpcError> {
        let provider = {
            let state = self.state.lock().unwrap();
            match state.table.get(capability) {
                Some(route) => route.plugin.clone(),
                None => {
                    return Err(RpcError::new(
                        codes::UNKNOWN_CAPABILITY,
                        format!("no provider is configured for `{capability}`"),
                    ));
                }
            }
        };
        if let Some(error) = self.availability(&provider) {
            return Err(error);
        }
        let admitted = {
            let mut state = self.state.lock().unwrap();
            let limits = state.config.limits.clone();
            let full = state.host_inflight >= limits.max_inflight
                || state.inflight_total >= limits.max_inflight_total;
            if !full {
                state.host_inflight += 1;
                state.inflight_total += 1;
            }
            !full
        };
        if !admitted {
            return Err(RpcError::new(
                codes::OVERLOADED,
                "the host is at its in-flight limit",
            ));
        }
        let request_id = {
            let mut state = self.state.lock().unwrap();
            let n = state.next_host_call;
            state.next_host_call += 1;
            format!("host-{n}")
        };
        let outcome = if stream {
            let sid = self.start_stream(
                HOST,
                &provider,
                capability,
                method_name,
                params,
                &request_id,
                None,
            );
            Ok(json!({ "stream_id": sid }))
        } else {
            self.forward(
                HOST,
                &request_id,
                &provider,
                capability,
                method_name,
                params,
                None,
                None,
            )
            .await
        };
        let mut state = self.state.lock().unwrap();
        state.host_inflight = state.host_inflight.saturating_sub(1);
        state.inflight_total = state.inflight_total.saturating_sub(1);
        outcome
    }

    /// One value to whoever `caller` names: a plugin's pipe, or the host queue.
    /// `false` means the host queue is full, that is, the frame was shed.
    fn deliver(state: &State, caller: &str, frame: &Value) -> bool {
        if caller == HOST {
            return state.host.push(frame);
        }
        if let Some(instance) = state.plugins.get(caller) {
            instance.sink().send(frame);
        }
        true
    }
    /// Runs the unified shutdown flow, exactly as a plugin's `kernel.shutdown`
    /// would. The embedder is the UI as far as a plugin can tell, hence `ui_quit`.
    pub fn shutdown(&self) {
        self.shutdown_with(proto::reason::UI_QUIT);
    }

    /// The same flow, with the reason the plugins will be told. The vocabulary
    /// lives at the protocol surface (`reason::*`), not here: an embedder that
    /// only ever says `ui_quit` calls `shutdown`.
    pub fn shutdown_with(&self, reason: &'static str) {
        let _ = self.shutdown_tx.send(reason);
    }

    pub async fn restart(
        self: &Arc<Self>,
        plugin: &str,
        trigger: &'static str,
    ) -> Result<(), RpcError> {
        self.clone().restart_plugin(plugin, trigger).await
    }

    /// The embedder gives up one of its own streams or calls. The params are the
    /// wire shape (7.6): `{stream_id}` or `{request_id}`. Cancellation is
    /// cooperative and silent - the kernel stops forwarding and tells the
    /// provider, and the caller gets no terminal frame. An id nobody knows is a
    /// no-op, so cancelling twice is harmless.
    pub fn cancel(&self, params: &Value) {
        self.on_cancel(HOST, params);
    }
}
