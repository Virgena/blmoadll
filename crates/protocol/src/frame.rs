//! LSP-style framing: `Content-Length: <bytes>\r\n\r\n<utf8 json>`.
//!
//! Other headers are ignored; only `Content-Length` matters.

use std::io;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Cap on the header block itself, independent of the frame cap.
const MAX_HEADER_BYTES: usize = 8 * 1024;

#[derive(Debug)]
pub enum FrameError {
    /// Frame exceeded the negotiated cap. Maps to `-32016`.
    TooLarge(usize),
    BadHeader(String),
    Io(io::Error),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::TooLarge(n) => write!(f, "frame of {n} bytes exceeds max_frame_bytes"),
            FrameError::BadHeader(m) => write!(f, "bad frame header: {m}"),
            FrameError::Io(e) => write!(f, "frame i/o error: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        FrameError::Io(e)
    }
}

/// Reads one frame. `Ok(None)` means the peer closed the pipe at a frame boundary.
pub async fn read_frame<R>(r: &mut R, max_frame_bytes: usize) -> Result<Option<Vec<u8>>, FrameError>
where
    R: AsyncBufRead + Unpin,
{
    let mut content_length: Option<usize> = None;
    let mut header_bytes = 0usize;
    loop {
        let line = read_header_line(r).await?;
        if line.is_empty() {
            // EOF: clean only if we had not started a header.
            return if header_bytes == 0 {
                Ok(None)
            } else {
                Err(FrameError::BadHeader("eof inside header".into()))
            };
        }
        header_bytes += line.len() + 1;
        if header_bytes > MAX_HEADER_BYTES {
            return Err(FrameError::BadHeader("header too large".into()));
        }
        let trimmed = trim_crlf(&line);
        if trimmed.is_empty() {
            break;
        }
        if let Some(len) = content_length_of(trimmed)? {
            content_length = Some(len);
        }
    }

    let len =
        content_length.ok_or_else(|| FrameError::BadHeader("missing Content-Length".into()))?;
    if len > max_frame_bytes {
        return Err(FrameError::TooLarge(len));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

/// Writes one frame and flushes it.
pub async fn write_frame<W>(w: &mut W, payload: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let header = format!("Content-Length: {}\r\n\r\n", payload.len());
    w.write_all(header.as_bytes()).await?;
    w.write_all(payload).await?;
    w.flush().await
}

async fn read_header_line<R>(r: &mut R) -> Result<Vec<u8>, FrameError>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        match r.read_u8().await {
            Ok(b'\n') => return Ok(line),
            Ok(b) => {
                line.push(b);
                if line.len() > MAX_HEADER_BYTES {
                    return Err(FrameError::BadHeader("header line too long".into()));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(line),
            Err(e) => return Err(FrameError::Io(e)),
        }
    }
}

fn trim_crlf(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    while end > 0 && (line[end - 1] == b'\r' || line[end - 1] == b' ') {
        end -= 1;
    }
    &line[..end]
}

fn content_length_of(line: &[u8]) -> Result<Option<usize>, FrameError> {
    let Some(colon) = line.iter().position(|b| *b == b':') else {
        return Ok(None); // header without a value: ignored
    };
    let name = String::from_utf8_lossy(&line[..colon]).to_ascii_lowercase();
    if name != "content-length" {
        return Ok(None);
    }
    let value = String::from_utf8_lossy(&line[colon + 1..])
        .trim()
        .to_string();
    let len = value
        .parse::<usize>()
        .map_err(|_| FrameError::BadHeader(format!("bad Content-Length: {value:?}")))?;
    Ok(Some(len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    async fn round_trip(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_frame(&mut out, payload).await.unwrap();
        let mut reader = BufReader::new(out.as_slice());
        read_frame(&mut reader, 1024).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn round_trips_exactly() {
        assert_eq!(round_trip(b"{}").await, b"{}");
        assert_eq!(round_trip(b"hello world").await, b"hello world");
        assert_eq!(round_trip(b"").await, b"");
    }

    #[tokio::test]
    async fn ignores_other_headers_and_reads_back_to_back_frames() {
        let mut buf = b"X-Whatever: 3\r\nContent-Length: 2\r\n\r\nhi".to_vec();
        write_frame(&mut buf, b"!!").await.unwrap();
        let mut reader = BufReader::new(buf.as_slice());
        assert_eq!(read_frame(&mut reader, 64).await.unwrap().unwrap(), b"hi");
        assert_eq!(read_frame(&mut reader, 64).await.unwrap().unwrap(), b"!!");
        assert!(read_frame(&mut reader, 64).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_oversize_frame() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"1234567890").await.unwrap();
        let mut reader = BufReader::new(buf.as_slice());
        match read_frame(&mut reader, 4).await {
            Err(FrameError::TooLarge(10)) => {}
            other => panic!("expected TooLarge(10), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_missing_content_length() {
        let mut buf = b"X-Nothing: 1\r\n\r\n".to_vec();
        buf.extend_from_slice(b"body");
        let mut reader = BufReader::new(buf.as_slice());
        assert!(matches!(
            read_frame(&mut reader, 64).await,
            Err(FrameError::BadHeader(_))
        ));
    }

    #[tokio::test]
    async fn eof_at_frame_boundary_is_none() {
        let mut reader = BufReader::new(&b""[..]);
        assert!(read_frame(&mut reader, 64).await.unwrap().is_none());
    }
}
