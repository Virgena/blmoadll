//! The loader: a config file in, a plugin set out.
//!
//! Nothing here spawns a process. It reads the config file and every file that
//! one `extends`, merges the layers, expands `${VAR}`, resolves paths against
//! the entry file's directory, checks the dependency graph, and hands the
//! kernel a list of plugins to run. Reloading is the same call again on the
//! same path, and `Config::fingerprint` is what tells a reloader the layers
//! changed.

pub mod config;
pub mod graph;
pub mod route;
pub mod spec;

pub use config::{Config, ConfigError};
pub use graph::{Decl, Issue, Req};
pub use route::{to_json, Route, RoutingTable};
pub use spec::{Limits, PluginSpec, Timeouts};
