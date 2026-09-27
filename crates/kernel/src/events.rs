//! Topic-based broadcast bus. The kernel relays; plugins only ever see events.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use protocol::method;

#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub len: usize,
    pub bytes: usize,
}

pub struct Delivery {
    pub plugin: String,
    /// Subscription this frame belongs to; the kernel acks it after the frame
    /// actually reaches the plugin. `None` means "not accounted".
    pub sub_id: Option<u64>,
    pub size: usize,
    pub frame: Value,
}

struct Sub {
    id: u64,
    plugin: String,
    patterns: Vec<String>,
    count: usize,
    bytes: usize,
    dropped: u64,
    last_notice: Option<Instant>,
}

#[derive(Default)]
pub struct EventBus {
    next_sub: AtomicU64,
    seq: AtomicU64,
    subs: Mutex<BTreeMap<u64, Sub>>,
}

impl EventBus {
    pub fn new() -> Self {
        EventBus {
            next_sub: AtomicU64::new(1),
            seq: AtomicU64::new(1),
            subs: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    pub fn subscribe(&self, plugin: &str, patterns: Vec<String>) -> u64 {
        let id = self.next_sub.fetch_add(1, Ordering::SeqCst);
        self.subs.lock().unwrap().insert(
            id,
            Sub {
                id,
                plugin: plugin.to_string(),
                patterns,
                count: 0,
                bytes: 0,
                dropped: 0,
                last_notice: None,
            },
        );
        id
    }

    pub fn unsubscribe(&self, id: u64) -> bool {
        self.subs.lock().unwrap().remove(&id).is_some()
    }

    /// Drops every subscription of a plugin; returns the ids that were removed.
    pub fn remove_plugin(&self, plugin: &str) -> Vec<u64> {
        let mut subs = self.subs.lock().unwrap();
        let doomed: Vec<u64> = subs
            .values()
            .filter(|sub| sub.plugin == plugin)
            .map(|sub| sub.id)
            .collect();
        for id in &doomed {
            subs.remove(id);
        }
        doomed
    }

    /// Counts a frame as delivered, releasing its share of the subscription's
    /// budget. Called once the frame has actually been written.
    pub fn ack(&self, sub_id: u64, size: usize) {
        let mut subs = self.subs.lock().unwrap();
        if let Some(sub) = subs.get_mut(&sub_id) {
            sub.count = sub.count.saturating_sub(1);
            sub.bytes = sub.bytes.saturating_sub(size);
        }
    }

    pub fn frame_size(topic: &str, payload_len: usize) -> usize {
        payload_len + topic.len() + 64
    }

    /// Routes one published event. Returns `(events_to_send, drop_notices)`.
    /// Drop notices bypass the budget by design: they are how a subscriber
    /// learns that it fell behind.
    pub fn deliver(
        &self,
        topic: &str,
        payload: &Value,
        payload_len: usize,
        limits: &Budget,
    ) -> (Vec<Delivery>, Vec<Delivery>) {
        let seq = self.next_seq();
        let size = Self::frame_size(topic, payload_len);
        let mut events = Vec::new();
        let mut notices = Vec::new();
        let mut subs = self.subs.lock().unwrap();

        for sub in subs.values_mut() {
            if !sub
                .patterns
                .iter()
                .any(|pattern| matches_topic(pattern, topic))
            {
                continue;
            }
            if sub.count + 1 > limits.len || sub.bytes + size > limits.bytes {
                sub.dropped += 1;
                let due = sub
                    .last_notice
                    .map(|at| at.elapsed() >= Duration::from_secs(1))
                    .unwrap_or(true);
                if due {
                    sub.last_notice = Some(Instant::now());
                    notices.push(Delivery {
                        plugin: sub.plugin.clone(),
                        sub_id: None,
                        size: 0,
                        frame: json!({
                            "jsonrpc": "2.0",
                            "method": method::EVENT,
                            "params": {
                                "topic": "kernel.event.dropped",
                                "seq": 0,
                                "payload": {
                                    "subscription_id": sub.id,
                                    "dropped_count": sub.dropped,
                                },
                            },
                        }),
                    });
                }
                continue;
            }
            sub.count += 1;
            sub.bytes += size;
            events.push(Delivery {
                plugin: sub.plugin.clone(),
                sub_id: Some(sub.id),
                size,
                frame: json!({
                    "jsonrpc": "2.0",
                    "method": method::EVENT,
                    "params": { "topic": topic, "seq": seq, "payload": payload },
                }),
            });
        }

        (events, notices)
    }
}

/// Segment-wise topic match; `*` matches exactly one segment. `**` is reserved
/// and matches nothing in v1.
pub fn matches_topic(pattern: &str, topic: &str) -> bool {
    let pattern: Vec<&str> = pattern.split('.').collect();
    let topic: Vec<&str> = topic.split('.').collect();
    pattern.len() == topic.len()
        && pattern
            .iter()
            .zip(topic.iter())
            .all(|(p, t)| *p == "*" || p == t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Budget {
        Budget {
            len: 2,
            bytes: 4096,
        }
    }

    #[test]
    fn matches_segments_only() {
        assert!(matches_topic("demo.*", "demo.turn"));
        assert!(!matches_topic("demo.*", "demo.turn.started"));
        assert!(!matches_topic("demo.*", "loops.turn"));
        assert!(matches_topic(
            "kernel.plugin.started",
            "kernel.plugin.started"
        ));
        assert!(matches_topic("*.*", "demo.turn"));
        assert!(!matches_topic("**", "demo.turn"));
    }

    #[test]
    fn delivers_to_matching_subscribers_with_increasing_seq() {
        let bus = EventBus::new();
        bus.subscribe("subscriber", vec!["demo.*".to_string()]);
        let payload = json!({"n": 1});
        let (events, notices) = bus.deliver("demo.turn", &payload, 8, &limits());
        assert_eq!(events.len(), 1);
        assert!(notices.is_empty());
        assert_eq!(events[0].plugin, "subscriber");
        assert_eq!(events[0].frame["params"]["topic"], json!("demo.turn"));
        let first = events[0].frame["params"]["seq"].as_u64().unwrap();

        let (events, _) = bus.deliver("demo.turn", &payload, 8, &limits());
        assert_eq!(
            events[0].frame["params"]["seq"].as_u64().unwrap(),
            first + 1
        );
    }

    #[test]
    fn overflow_drops_the_newest_and_notices_at_most_once_per_second() {
        let bus = EventBus::new();
        let id = bus.subscribe("subscriber", vec!["demo.*".to_string()]);
        let payload = json!({});
        assert_eq!(bus.deliver("demo.turn", &payload, 8, &limits()).0.len(), 1);
        assert_eq!(bus.deliver("demo.turn", &payload, 8, &limits()).0.len(), 1);

        let (events, notices) = bus.deliver("demo.turn", &payload, 8, &limits());
        assert!(events.is_empty());
        assert_eq!(notices.len(), 1);
        assert_eq!(
            notices[0].frame["params"]["topic"],
            json!("kernel.event.dropped")
        );
        assert_eq!(notices[0].frame["params"]["seq"], json!(0));
        assert_eq!(
            notices[0].frame["params"]["payload"]["subscription_id"],
            json!(id)
        );
        assert_eq!(
            notices[0].frame["params"]["payload"]["dropped_count"],
            json!(1)
        );

        let (_, again) = bus.deliver("demo.turn", &payload, 8, &limits());
        assert!(again.is_empty(), "drop notices must be rate limited");

        bus.ack(id, EventBus::frame_size("demo.turn", 8));
        bus.ack(id, EventBus::frame_size("demo.turn", 8));
        assert_eq!(bus.deliver("demo.turn", &payload, 8, &limits()).0.len(), 1);
    }

    #[test]
    fn unsubscribing_and_plugin_removal_clean_up() {
        let bus = EventBus::new();
        let id = bus.subscribe("subscriber", vec!["demo.*".to_string()]);
        assert!(bus.unsubscribe(id));
        assert!(!bus.unsubscribe(id));
        assert!(
            bus.deliver("demo.turn", &json!({}), 2, &limits())
                .0
                .is_empty()
        );

        bus.subscribe("subscriber", vec!["demo.*".to_string()]);
        bus.subscribe("subscriber", vec!["kernel.*".to_string()]);
        assert_eq!(bus.remove_plugin("subscriber").len(), 2);
        assert!(
            bus.deliver("demo.turn", &json!({}), 2, &limits())
                .0
                .is_empty()
        );
    }
}
