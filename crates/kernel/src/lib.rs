//! eggshellmod kernel.
//!
//! It spawns and supervises plugin processes, routes capability calls between
//! them, relays events, multiplexes the terminal, hot reloads and shuts down.
//! It knows capability ids, semver ranges and which process to talk to - and
//! nothing else. Every capability's meaning lives in a plugin.

pub mod events;
pub mod io;
pub mod kernel;
pub mod process;

pub use events::EventBus;
pub use kernel::{run, Host, Kernel, Report};
