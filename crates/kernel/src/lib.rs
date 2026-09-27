//! eggshellmod kernel.
//!
//! It spawns and supervises plugin processes, routes capability calls between
//! them, relays events, multiplexes the terminal, hot reloads and shuts down.
//! It knows capability ids and which process serves one - and nothing else.
//! Every capability's meaning lives in a plugin.

pub mod events;
pub mod io;
pub mod kernel;
pub mod process;

pub use events::EventBus;
pub use kernel::{Host, Kernel, Report, run};
