//! What the kernel needs in order to run one plugin, and the ceilings it holds
//! itself to while doing it.
//!
//! The loader decides *what* to run from a config file; this is the shape it
//! hands over. Paths arrive already resolved, so the kernel never has to know
//! where the config file lived.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::Value;

pub const MIB: usize = 1024 * 1024;

/// Kernel-wide resource ceilings. Every field has a default, so a config file
/// only has to mention what it wants to change.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// The `[kernel.host]` table: one process that hosts every eligible Node
    /// plugin. `None` means one process per plugin, exactly as before.
    #[serde(default)]
    pub host: Option<HostRow>,
    /// The `[kernel.runtime]` table: one process that owns every plugin. When
    /// it is set the plugin rows belong to that process and the kernel starts
    /// no plugin of its own.
    #[serde(default)]
    pub runtime: Option<HostRow>,
    /// How long the kernel waits for the Runtime's first capability snapshot
    /// before it calls the launch failed.
    #[serde(default = "d_runtime_ready_ms")]
    pub runtime_ready_ms: u64,
    /// How many times the kernel may restart the host inside one window.
    #[serde(default = "d_host_restarts")]
    pub host_restarts: usize,
    #[serde(default = "d_host_restart_window_ms")]
    pub host_restart_window_ms: u64,
    /// How often the kernel asks the host if it is still responsive.
    #[serde(default = "d_host_ping_ms")]
    pub host_ping_ms: u64,
    #[serde(default = "d_initialize_timeout")]
    pub initialize_timeout_ms: u64,
    #[serde(default = "d_start_timeout")]
    pub start_timeout_ms: u64,
    #[serde(default = "d_shutdown_grace")]
    pub shutdown_grace_ms: u64,
    #[serde(default = "d_request_timeout")]
    pub request_timeout_ms: u64,
    #[serde(default = "d_stream_idle_timeout")]
    pub stream_idle_timeout_ms: u64,
    #[serde(default = "d_max_frame_bytes")]
    pub max_frame_bytes: usize,
    #[serde(default = "d_event_payload_bytes")]
    pub event_payload_bytes: usize,
    #[serde(default = "d_event_queue_len")]
    pub event_queue_len: usize,
    #[serde(default = "d_event_queue_bytes")]
    pub event_queue_bytes: usize,
    #[serde(default = "d_outbound_queue_bytes")]
    pub outbound_queue_bytes: usize,
    #[serde(default = "d_io_write_queue_bytes")]
    pub io_write_queue_bytes: usize,
    #[serde(default = "d_queue_high_water")]
    pub queue_high_water_bytes: usize,
    #[serde(default = "d_queue_low_water")]
    pub queue_low_water_bytes: usize,
    #[serde(default = "d_stream_buffer_chunks")]
    pub stream_buffer_chunks: usize,
    #[serde(default = "d_stream_buffer_bytes")]
    pub stream_buffer_bytes: usize,
    #[serde(default = "d_max_inflight")]
    pub max_inflight: usize,
    #[serde(default = "d_max_inflight_total")]
    pub max_inflight_total: usize,
    #[serde(default = "d_drain_ms")]
    pub drain_ms: u64,
    #[serde(default = "d_io_eof_idle_ms")]
    pub io_eof_idle_ms: u64,
    #[serde(default = "d_max_plugins")]
    pub max_plugins: usize,
    #[serde(default = "d_log_line_bytes")]
    pub log_line_bytes: usize,
    #[serde(default = "d_io_line_bytes")]
    pub io_line_bytes: usize,
}

fn d_host_restarts() -> usize {
    3
}
fn d_runtime_ready_ms() -> u64 {
    30_000
}
fn d_host_restart_window_ms() -> u64 {
    600_000
}
fn d_host_ping_ms() -> u64 {
    2_000
}
fn d_initialize_timeout() -> u64 {
    5_000
}
fn d_start_timeout() -> u64 {
    10_000
}
fn d_shutdown_grace() -> u64 {
    5_000
}
fn d_request_timeout() -> u64 {
    30_000
}
fn d_stream_idle_timeout() -> u64 {
    30_000
}
fn d_max_frame_bytes() -> usize {
    64 * MIB
}
fn d_event_payload_bytes() -> usize {
    256 * 1024
}
fn d_event_queue_len() -> usize {
    1024
}
fn d_event_queue_bytes() -> usize {
    4 * MIB
}
fn d_outbound_queue_bytes() -> usize {
    4 * MIB
}
fn d_io_write_queue_bytes() -> usize {
    4 * MIB
}
fn d_queue_high_water() -> usize {
    2 * MIB
}
fn d_queue_low_water() -> usize {
    1 * MIB
}
fn d_stream_buffer_chunks() -> usize {
    4096
}
fn d_stream_buffer_bytes() -> usize {
    8 * MIB
}
fn d_max_inflight() -> usize {
    64
}
fn d_max_inflight_total() -> usize {
    1024
}
fn d_drain_ms() -> u64 {
    5_000
}
fn d_io_eof_idle_ms() -> u64 {
    500
}
fn d_max_plugins() -> usize {
    64
}
fn d_log_line_bytes() -> usize {
    8 * 1024
}
fn d_io_line_bytes() -> usize {
    8 * 1024
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            host: None,
            runtime: None,
            runtime_ready_ms: d_runtime_ready_ms(),
            host_restarts: d_host_restarts(),
            host_restart_window_ms: d_host_restart_window_ms(),
            host_ping_ms: d_host_ping_ms(),
            initialize_timeout_ms: d_initialize_timeout(),
            start_timeout_ms: d_start_timeout(),
            shutdown_grace_ms: d_shutdown_grace(),
            request_timeout_ms: d_request_timeout(),
            stream_idle_timeout_ms: d_stream_idle_timeout(),
            max_frame_bytes: d_max_frame_bytes(),
            event_payload_bytes: d_event_payload_bytes(),
            event_queue_len: d_event_queue_len(),
            event_queue_bytes: d_event_queue_bytes(),
            outbound_queue_bytes: d_outbound_queue_bytes(),
            io_write_queue_bytes: d_io_write_queue_bytes(),
            queue_high_water_bytes: d_queue_high_water(),
            queue_low_water_bytes: d_queue_low_water(),
            stream_buffer_chunks: d_stream_buffer_chunks(),
            stream_buffer_bytes: d_stream_buffer_bytes(),
            max_inflight: d_max_inflight(),
            max_inflight_total: d_max_inflight_total(),
            drain_ms: d_drain_ms(),
            io_eof_idle_ms: d_io_eof_idle_ms(),
            max_plugins: d_max_plugins(),
            log_line_bytes: d_log_line_bytes(),
            io_line_bytes: d_io_line_bytes(),
        }
    }
}

/// Per-plugin overrides. `None` means "whatever the kernel default is".
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Timeouts {
    #[serde(default)]
    pub initialize_timeout_ms: Option<u64>,
    #[serde(default)]
    pub start_timeout_ms: Option<u64>,
    #[serde(default)]
    pub shutdown_grace_ms: Option<u64>,
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    #[serde(default)]
    pub stream_idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_inflight: Option<usize>,
}

impl Timeouts {
    pub fn initialize(&self, limits: &Limits) -> u64 {
        self.initialize_timeout_ms
            .unwrap_or(limits.initialize_timeout_ms)
    }

    pub fn start(&self, limits: &Limits) -> u64 {
        self.start_timeout_ms.unwrap_or(limits.start_timeout_ms)
    }

    pub fn shutdown_grace(&self, limits: &Limits) -> u64 {
        self.shutdown_grace_ms.unwrap_or(limits.shutdown_grace_ms)
    }

    pub fn request(&self, limits: &Limits) -> u64 {
        self.request_timeout_ms.unwrap_or(limits.request_timeout_ms)
    }

    pub fn stream_idle(&self, limits: &Limits) -> u64 {
        self.stream_idle_timeout_ms
            .unwrap_or(limits.stream_idle_timeout_ms)
    }

    pub fn max_inflight(&self, limits: &Limits) -> usize {
        self.max_inflight.unwrap_or(limits.max_inflight)
    }
}

/// A process row, as written: `[kernel.host]` (the legacy shared host) or
/// `[kernel.runtime]` (the Runtime that owns every plugin). Strings are raw
/// here; the loader resolves them exactly like a plugin row's execution fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostRow {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub clear_env: bool,
}

/// Everything needed to start one plugin process and talk to it.
#[derive(Debug, Clone, PartialEq)]
pub struct PluginSpec {
    pub id: String,
    /// Already resolved by the loader: absolute, or a bare name for `PATH`.
    pub command: PathBuf,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    pub clear_env: bool,
    /// The `[plugins.<id>.config]` table, handed over verbatim at `initialize`.
    pub config: Value,
    pub timeouts: Timeouts,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_defaults_are_the_documented_ones() {
        let limits = Limits::default();
        assert_eq!(limits.max_frame_bytes, 64 * MIB);
        assert_eq!(limits.event_payload_bytes, 256 * 1024);
        assert_eq!(limits.queue_high_water_bytes, 2 * MIB);
        assert_eq!(limits.queue_low_water_bytes, MIB);
        assert_eq!(limits.io_eof_idle_ms, 500);
        assert_eq!(limits.drain_ms, 5_000);
    }

    #[test]
    fn per_plugin_overrides_win_over_defaults() {
        let limits = Limits::default();
        let timeouts = Timeouts {
            start_timeout_ms: Some(1),
            ..Default::default()
        };
        assert_eq!(timeouts.start(&limits), 1);
        assert_eq!(timeouts.initialize(&limits), limits.initialize_timeout_ms);
        assert_eq!(timeouts.max_inflight(&limits), limits.max_inflight);
    }
}
