//! stderr logging: one JSON object per line.
//!
//! Deliberately hand-rolled instead of pulling a logging framework: the whole
//! surface is a handful of fields and the machine-readable shape *is* the
//! contract. Both the kernel and the loader write through this.

use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

/// 0 = quiet (errors only), 1 = info, 2 = debug.
static LEVEL: AtomicU8 = AtomicU8::new(1);
static LOCK: Mutex<()> = Mutex::new(());

pub fn init(verbosity: u8) {
    LEVEL.store(verbosity, Ordering::SeqCst);
}

pub fn verbosity() -> u8 {
    LEVEL.load(Ordering::SeqCst)
}

fn enabled(level: &str) -> bool {
    let wanted = match level {
        "error" | "warn" => 0,
        "info" => 1,
        _ => 2,
    };
    wanted <= verbosity()
}

pub fn format_line(level: &str, target: &str, message: &str, fields: &Value) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut object = json!({
        "ts": ts,
        "level": level,
        "target": target,
        "message": message,
    });
    if let Some(extra) = fields.as_object() {
        for (key, value) in extra {
            object[key] = value.clone();
        }
    }
    object.to_string()
}

pub fn emit(level: &str, target: &str, message: &str, fields: Value) {
    if !enabled(level) {
        return;
    }
    let line = format_line(level, target, message, &fields);
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let mut stderr = std::io::stderr();
    let _ = writeln!(stderr, "{line}");
}

pub fn info(target: &str, message: &str) {
    emit("info", target, message, json!({}));
}

pub fn warn(target: &str, message: &str) {
    emit("warn", target, message, json!({}));
}

pub fn error(target: &str, message: &str) {
    emit("error", target, message, json!({}));
}

pub fn debug(target: &str, message: &str) {
    emit("debug", target, message, json!({}));
}

/// Forwards one line of a plugin's stderr, truncated at `limit` bytes.
pub fn plugin_stderr(plugin: &str, line: &str, limit: usize) {
    let message = truncate(line, limit);
    emit(
        "info",
        "plugin.stderr",
        &message,
        json!({ "plugin": plugin }),
    );
}

/// Byte-safe truncation with a visible marker.
pub fn truncate(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_string();
    }
    let mut end = limit;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[truncated]", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_is_valid_json_with_extras() {
        let line = format_line("info", "kernel", "hello", &json!({"plugin": "p"}));
        let parsed: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["level"], json!("info"));
        assert_eq!(parsed["target"], json!("kernel"));
        assert_eq!(parsed["message"], json!("hello"));
        assert_eq!(parsed["plugin"], json!("p"));
        assert!(parsed["ts"].is_u64());
    }

    #[test]
    fn truncation_keeps_char_boundaries() {
        let text = "ααααα";
        let cut = truncate(text, 4);
        assert!(cut.ends_with("…[truncated]"));
        assert_eq!(cut, "αα…[truncated]");
        assert_eq!(truncate("short", 64), "short");
    }

    #[test]
    fn verbosity_gates_levels() {
        init(0);
        assert!(!enabled("info") && enabled("error"));
        init(2);
        assert!(enabled("debug") && enabled("info"));
        init(1);
    }
}
