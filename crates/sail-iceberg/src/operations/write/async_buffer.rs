use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures::TryFuture;
use tokio::io::AsyncWrite;
use tokio::sync::RwLock as TokioRwLock;

#[derive(Debug, Default, Clone)]
pub(super) struct AsyncShareableBuffer {
    buffer: Arc<TokioRwLock<Vec<u8>>>,
    bytes_written: Arc<AtomicU64>,
}

impl AsyncShareableBuffer {
    pub(super) async fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.buffer)
            .ok()
            .map(|lock| lock.into_inner())
    }

    pub(super) fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }
}

impl AsyncWrite for AsyncShareableBuffer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.clone();
        let buf = buf.to_vec();

        let fut = async move {
            let mut buffer = this.buffer.write().await;
            buffer.extend_from_slice(&buf);
            this.bytes_written
                .fetch_add(buf.len() as u64, Ordering::Relaxed);
            Ok(buf.len())
        };

        tokio::pin!(fut);
        fut.try_poll(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
