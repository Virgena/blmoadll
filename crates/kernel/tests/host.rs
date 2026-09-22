//! Process-level tests: real plugin processes, the real wire protocol.
//!
//! They need the test-double plugin, so they run with `--features fixture`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use kernel::{Host, Kernel};
use loader::Config;

const FIXTURE: &str = env!("CARGO_BIN_EXE_eggshell-fixture");

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("eggshell-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("scratch dir");
    path
}

/// The fixture path goes in a TOML literal string: on Windows it is full of
/// backslashes, and a basic string would treat them as escapes.
fn config(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("eggshell.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

async fn boot(path: &Path) -> Arc<Kernel> {
    let config = Config::load(path, &|name: &str| std::env::var(name).ok()).expect("load config");
    let kernel = Kernel::boot(config, false).await.expect("boot");
    kernel.clone().serve();
    kernel
}

async fn stop(kernel: &Arc<Kernel>) -> i32 {
    kernel.shutdown();
    kernel.clone().wait_for_exit().await
}

async fn next(host: &mut Host) -> Value {
    tokio::time::timeout(Duration::from_secs(10), host.recv())
        .await
        .expect("timed out waiting for the kernel")
        .expect("the kernel closed the host queue")
}

fn one_provider(provides: &str, extra: &str) -> String {
    format!(
        r#"
[plugins.provider]
command = '{FIXTURE}'
args = ["--provides", "{provides}"{extra}]

[capability]
"demo.text" = "provider"
"#
    )
}

#[tokio::test]
async fn check_accepts_a_slot_that_is_provided() {
    let dir = scratch("check-ok");
    let path = config(&dir, &one_provider("demo.text=1.0.0", ""));
    let report = kernel::run(&path, true).await;
    assert_eq!(report.code, 0, "{}", report.to_text());
    assert_eq!(report.start_order, vec!["provider".to_string()]);
    assert!(report.capabilities.contains_key("demo.text"));
}

#[tokio::test]
async fn check_rejects_a_slot_nobody_provides() {
    let dir = scratch("check-bad");
    let path = config(&dir, &format!(r#"
[plugins.provider]
command = '{FIXTURE}'

[capability]
"demo.text" = "provider"
"#));
    let report = kernel::run(&path, true).await;
    assert_eq!(report.code, 1);
    let text = report.to_text();
    assert!(text.contains("demo.text"), "{text}");
}

#[tokio::test]
async fn the_host_can_call_a_capability() {
    let dir = scratch("invoke");
    let path = config(&dir, &one_provider("demo.text=1.0.0", ""));
    let kernel = boot(&path).await;

    let reply = kernel.invoke("demo.text", "echo", json!({"hi": 1})).await.expect("call");
    assert_eq!(reply["got"]["hi"], json!(1));

    let error = kernel.invoke("demo.nope", "echo", json!({})).await.unwrap_err();
    assert_eq!(error.code, -32010);

    assert_eq!(stop(&kernel).await, 0);
}

#[tokio::test]
async fn the_host_receives_ordered_chunks_and_one_terminal_block() {
    let dir = scratch("stream");
    let path = config(&dir, &one_provider("demo.text=1.0.0", r#", "--chunks", "3""#));
    let kernel = boot(&path).await;
    let mut host = kernel.host().expect("host channel");

    let reply = kernel.invoke_stream("demo.text", "chat", json!({})).await.expect("stream");
    let stream_id = reply["stream_id"].as_str().expect("stream id").to_string();

    let mut chunks = Vec::new();
    for _ in 0..4 {
        let value = next(&mut host).await;
        assert_eq!(value["method"], json!("$/stream/chunk"));
        assert_eq!(value["params"]["stream_id"], json!(stream_id));
        chunks.push(value["params"].clone());
    }
    assert_eq!(chunks[0]["seq"], json!(0));
    assert_eq!(chunks[2]["data"]["delta"], json!("c2"));
    assert_eq!(chunks[3]["done"], json!(true));
    assert!(chunks[3]["data"].is_null());

    assert_eq!(stop(&kernel).await, 0);
}

#[tokio::test]
async fn a_provider_that_dies_does_not_take_the_kernel_with_it() {
    let dir = scratch("crash");
    let path = config(&dir, &format!(r#"
[plugins.doomed]
command = '{FIXTURE}'
args = ["--provides", "demo.text=1.0.0", "--exit-on-invoke"]

[plugins.healthy]
command = '{FIXTURE}'
args = ["--provides", "demo.other=1.0.0"]

[capability]
"demo.text" = "doomed"
"demo.other" = "healthy"
"#));
    let kernel = boot(&path).await;
    let mut host = kernel.host().expect("host channel");
    kernel.subscribe(&["kernel.plugin.*".to_string()], true).expect("subscribe");

    let mut replayed: Vec<String> = Vec::new();
    for _ in 0..2 {
        let event = next(&mut host).await;
        assert_eq!(event["params"]["topic"], json!("kernel.plugin.started"));
        assert_eq!(event["params"]["payload"]["trigger"], json!("boot"));
        assert!(event["params"]["payload"]["cwd"].is_string());
        assert!(event["params"]["payload"]["command"].is_string());
        assert!(event["params"]["payload"]["args"].is_array());
        replayed.push(event["params"]["payload"]["plugin"].as_str().unwrap().to_string());
    }
    replayed.sort();
    assert_eq!(replayed, ["doomed", "healthy"]);

    let error = kernel.invoke("demo.text", "echo", json!({})).await.unwrap_err();
    assert_eq!(error.code, -32011);

    // The dead provider keeps its slot and answers with the same code.
    let again = kernel.invoke("demo.text", "echo", json!({})).await.unwrap_err();
    assert_eq!(again.code, -32011);

    // The rest of the kernel is unaffected.
    assert!(kernel.invoke("demo.other", "echo", json!({})).await.is_ok());

    let event = next(&mut host).await;
    assert_eq!(event["params"]["topic"], json!("kernel.plugin.degraded"));
    assert_eq!(event["params"]["payload"]["plugin"], json!("doomed"));

    stop(&kernel).await;
}
