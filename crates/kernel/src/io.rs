//! Terminal I/O primitives: `kernel.attach` / `detach` / `write` + `$/io/data`.
//!
//! This is a transport layer, not an Agent capability. The kernel multiplexes
//! its own stdin/stdout between plugins; it still has no idea what a "UI" is.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, watch};

use protocol::codes;

use crate::kernel::Kernel;

/// The kernel's stdout, behind a byte-counted queue.
pub struct Stdout {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    bytes: Arc<AtomicUsize>,
}

impl Stdout {
    pub fn new() -> Stdout {
        let (tx, rx) = mpsc::unbounded_channel();
        let bytes = Arc::new(AtomicUsize::new(0));
        tokio::spawn(drain(tokio::io::stdout(), rx, bytes.clone()));
        Stdout { tx, bytes }
    }

    pub fn queued_bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// `kernel.write`'s two limits: per-message payload size, then queue depth.
    pub fn write(&self, data: &str, hard_limit: usize, payload_limit: usize) -> Result<(), i64> {
        if data.len() > payload_limit {
            return Err(codes::PAYLOAD_TOO_LARGE);
        }
        if self.queued_bytes() + data.len() > hard_limit {
            return Err(codes::OVERLOADED);
        }
        let bytes = data.as_bytes().to_vec();
        let len = bytes.len();
        self.bytes.fetch_add(len, Ordering::Relaxed);
        if self.tx.send(bytes).is_err() {
            self.bytes.fetch_sub(len, Ordering::Relaxed);
        }
        Ok(())
    }
}

async fn drain<W>(mut out: W, mut rx: mpsc::UnboundedReceiver<Vec<u8>>, bytes: Arc<AtomicUsize>)
where
    W: AsyncWrite + Unpin,
{
    while let Some(chunk) = rx.recv().await {
        if out.write_all(&chunk).await.is_err() || out.flush().await.is_err() {
            break;
        }
        bytes.fetch_sub(chunk.len(), Ordering::Relaxed);
    }
}

/// Reads the kernel's stdin, but only while some plugin owns it. Nothing is
/// read before the first `attach`, so piped input waits in the OS buffer and
/// `echo hi | blmoadll run` loses nothing.
pub fn start_stdin(kernel: Arc<Kernel>, attached: watch::Receiver<bool>, chunk: usize) {
    tokio::spawn(async move {
        let mut attached = attached;
        let mut reader = BufReader::new(tokio::io::stdin());
        let mut buf = vec![0u8; chunk.max(1)];
        loop {
            if !*attached.borrow_and_update() {
                if attached.changed().await.is_err() {
                    return;
                }
                continue;
            }
            tokio::select! {
                read = reader.read(&mut buf) => match read {
                    Ok(0) => {
                        kernel.stdin_eof();
                        return;
                    }
                    Ok(n) => kernel.stdin_bytes(&buf[..n]),
                    Err(_) => {
                        kernel.stdin_eof();
                        return;
                    }
                },
                changed = attached.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    // Detached mid-read: the in-flight read is dropped. That is
                    // the "discard what was read but not delivered" case the
                    // contract allows. ponytail: a read already handed to
                    // tokio's blocking stdin pool cannot be paused, so re-attach
                    // after a detach can lose at most one chunk.
                }
            }
        }
    });
}
