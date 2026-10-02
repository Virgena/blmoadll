//! eggshellmod wire protocol.
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

    // notifications, both directions
    pub const EVENT: &str = "$/event";
    pub const IO_DATA: &str = "$/io/data";
    pub const IO_DETACHED: &str = "$/io/detached";
    pub const STREAM_CHUNK: &str = "$/stream/chunk";
    pub const STREAM_ERROR: &str = "$/stream/error";
    pub const CANCEL: &str = "$/cancel";
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
/// paths; `source` and `manual` come from a host `restart`.
pub mod trigger {
    pub const BOOT: &str = "boot";
    pub const CONFIG: &str = "config";
    pub const SOURCE: &str = "source";
    pub const MANUAL: &str = "manual";
}

pub use frame::{FrameError, read_frame, write_frame};
pub use msg::{Incoming, RpcError, failure, notify, parse_frame, request, success};

/// The caller label the kernel writes for the embedder. A plugin may not use it:
/// the loader refuses a config that defines a plugin with this id.
pub const HOST: &str = "host";
