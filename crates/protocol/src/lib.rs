//! blmoadll wire protocol.
//!
//! Two directions share one pipe: the kernel speaks JSON-RPC 2.0 to a plugin
//! process over its stdin/stdout, framed LSP-style with `Content-Length`.
//! Nothing in here knows what a "model" or a "session" is.

pub mod codes;
pub mod frame;
pub mod msg;

/// Protocol version carried by `initialize`. A mismatch is `-32015`.
pub const PROTOCOL_VERSION: u32 = 3;

/// JSON-RPC method names of the kernel <-> plugin protocol.
pub mod method {
    // kernel -> plugin
    pub const INITIALIZE: &str = "initialize";
    pub const START: &str = "start";
    pub const SHUTDOWN: &str = "shutdown";
    pub const INVOKE: &str = "invoke";

    // plugin -> kernel
    pub const KERNEL_INVOKE: &str = "kernel.invoke";
    pub const KERNEL_PUBLISH: &str = "kernel.publish";
    pub const KERNEL_SUBSCRIBE: &str = "kernel.subscribe";
    pub const KERNEL_UNSUBSCRIBE: &str = "kernel.unsubscribe";
    pub const KERNEL_LOG: &str = "kernel.log";
    pub const KERNEL_ATTACH: &str = "kernel.attach";
    pub const KERNEL_DETACH: &str = "kernel.detach";
    pub const KERNEL_WRITE: &str = "kernel.write";
    pub const KERNEL_SHUTDOWN: &str = "kernel.shutdown";

    // kernel -> host, on the shared plugin pipe
    pub const HOST_LOAD: &str = "$/host/load";
    pub const HOST_UNLOAD: &str = "$/host/unload";
    pub const HOST_KILL: &str = "$/host/kill";
    pub const HOST_PING: &str = "$/host/ping";

    // runtime -> kernel, on the runtime pipe
    pub const RUNTIME_READY: &str = "$/runtime/ready";
    pub const RUNTIME_CAPABILITIES: &str = "$/runtime/capabilities";
    pub const RUNTIME_EVENT: &str = "$/runtime/event";

    // kernel -> runtime, on the runtime pipe
    pub const RUNTIME_SUBSCRIBE: &str = "$/runtime/subscribe";
    pub const RUNTIME_UNSUBSCRIBE: &str = "$/runtime/unsubscribe";

    // notifications, both directions
    pub const EVENT: &str = "$/event";
    pub const IO_DATA: &str = "$/io/data";
    pub const IO_DETACHED: &str = "$/io/detached";
    pub const STREAM_CHUNK: &str = "$/stream/chunk";
    pub const STREAM_ERROR: &str = "$/stream/error";
    pub const CANCEL: &str = "$/cancel";

    // host -> kernel, on the shared plugin pipe
    pub const HOST_LOADING: &str = "$/host/loading";
    pub const HOST_EXITED: &str = "$/host/exited";
    pub const HOST_FAILED: &str = "$/host/failed";
    pub const HOST_CRASH: &str = "$/host/crash";
    pub const HOST_STUCK: &str = "$/host/stuck";
    pub const HOST_FATAL: &str = "$/host/fatal";
}

/// Shutdown reasons. A plugin MUST behave identically for all of them; they
/// exist for logs and metrics only.
pub mod reason {
    pub const KERNEL_EXIT: &str = "kernel_exit";
    pub const RELOAD: &str = "reload";
    pub const UI_QUIT: &str = "ui_quit";
    pub const CHECK: &str = "check";
}

/// Why a plugin's lifecycle changed. `boot` and `config` are the kernel's own
/// paths; `source` and `manual` come from a host `restart`, and `host` from the
/// kernel bringing a plugin up again after its extension host died.
pub mod trigger {
    pub const BOOT: &str = "boot";
    pub const CONFIG: &str = "config";
    pub const SOURCE: &str = "source";
    pub const MANUAL: &str = "manual";
    pub const HOST: &str = "host";
}

pub use frame::{FrameError, read_frame, write_frame};
pub use msg::{Incoming, RpcError, failure, notify, parse_frame, parse_tagged, request, success};

/// The caller label the kernel writes for the embedder. A plugin may not use it:
/// the loader refuses a config that defines a plugin with this id.
pub const HOST: &str = "host";

/// The id the kernel gives its one Runtime child. A plugin may not use it: the
/// Runtime is the only process that owns plugins.
pub const RUNTIME: &str = "runtime";
