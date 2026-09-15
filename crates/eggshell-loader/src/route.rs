//! What the kernel knows about capabilities: which plugin serves one, and at
//! which version.
//!
//! Nothing here knows what a capability *means*. The ids are opaque strings the
//! loader read out of a config file.

use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Route {
    pub plugin: String,
    pub version: String,
}

/// Capability id -> the plugin that serves it.
pub type RoutingTable = BTreeMap<String, Route>;

/// Renders a table the way `start` hands it to a plugin.
pub fn to_json(table: &RoutingTable) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for (capability, route) in table {
        object.insert(
            capability.clone(),
            serde_json::json!({ "plugin": route.plugin, "version": route.version }),
        );
    }
    serde_json::Value::Object(object)
}
