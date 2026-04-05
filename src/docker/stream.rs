//! Adapter that bridges bollard's Docker log stream into [`AsyncRead`].
//!
//! The existing output system reads from `impl AsyncRead` uniformly.
//! Docker logs come as a `Stream<Item=Result<LogOutput>>` from bollard.
//! [`DockerLogReader`] bridges the two by buffering stream items and
//! presenting them as a byte stream.

use bollard::container::LogOutput;
use bytes::BytesMut;
use futures_util::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// An [`AsyncRead`] adapter over a bollard Docker log stream.
///
/// Buffers log entries and serves them as a byte stream compatible
/// with [`crate::output::ServiceWriter::process_stream`].
pub struct DockerLogReader {
    stream: Pin<Box<dyn Stream<Item = Result<LogOutput, bollard::errors::Error>> + Send>>,
    buffer: BytesMut,
}

impl DockerLogReader {
    /// Create a new reader from a bollard log stream.
    pub fn new(
        stream: Pin<Box<dyn Stream<Item = Result<LogOutput, bollard::errors::Error>> + Send>>,
    ) -> Self {
        Self {
            stream,
            buffer: BytesMut::new(),
        }
    }
}

impl AsyncRead for DockerLogReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        // Drain buffered bytes first.
        if !this.buffer.is_empty() {
            let n = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer.split_to(n));
            return Poll::Ready(Ok(()));
        }

        // Poll the stream for more data.
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(log_output))) => {
                let bytes = log_output.into_bytes();
                let n = std::cmp::min(buf.remaining(), bytes.len());
                buf.put_slice(&bytes[..n]);
                if n < bytes.len() {
                    this.buffer.extend_from_slice(&bytes[n..]);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Err(std::io::Error::other(e)))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())), // EOF
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio_stream::iter;

    fn make_log_output(s: &str) -> LogOutput {
        LogOutput::StdOut {
            message: bytes::Bytes::from(s.to_string()),
        }
    }

    #[tokio::test]
    async fn test_reads_single_entry() {
        let items = vec![Ok(make_log_output("hello\n"))];
        let stream = Box::pin(iter(items));
        let mut reader = DockerLogReader::new(stream);

        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello\n");
    }

    #[tokio::test]
    async fn test_reads_multiple_entries() {
        let items = vec![
            Ok(make_log_output("line1\n")),
            Ok(make_log_output("line2\n")),
        ];
        let stream = Box::pin(iter(items));
        let mut reader = DockerLogReader::new(stream);

        let mut all = String::new();
        reader.read_to_string(&mut all).await.unwrap();
        assert_eq!(all, "line1\nline2\n");
    }

    #[tokio::test]
    async fn test_empty_stream_is_eof() {
        let items: Vec<Result<LogOutput, bollard::errors::Error>> = vec![];
        let stream = Box::pin(iter(items));
        let mut reader = DockerLogReader::new(stream);

        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 0); // EOF
    }

    #[tokio::test]
    async fn test_small_read_buffer_causes_buffering() {
        let items = vec![Ok(make_log_output("hello world\n"))];
        let stream = Box::pin(iter(items));
        let mut reader = DockerLogReader::new(stream);

        // Read with a small buffer.
        let mut buf = vec![0u8; 5];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");

        // Read the rest from the internal buffer.
        let mut buf2 = vec![0u8; 64];
        let n2 = reader.read(&mut buf2).await.unwrap();
        assert_eq!(&buf2[..n2], b" world\n");
    }
}
