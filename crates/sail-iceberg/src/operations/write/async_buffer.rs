// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/operations/write/async_utils.rs>
//
// Private copy of `sail-delta-lake`'s `AsyncShareableBuffer` (kept intentionally local so the
// Iceberg writer can measure the total bytes flushed to the sink without coupling the crates).

//! Async in-memory buffer used as the sink for the Iceberg parquet writer.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures::TryFuture;
use tokio::io::AsyncWrite;
use tokio::sync::RwLock as TokioRwLock;

/// An in-memory buffer that allows for shared ownership and interior mutability.
/// The underlying buffer is wrapped in an `Arc` and `RwLock`, so cloning the instance
/// allows multiple owners to have access to the same underlying buffer.
#[derive(Debug, Default, Clone)]
pub(super) struct AsyncShareableBuffer {
    buffer: Arc<TokioRwLock<Vec<u8>>>,
    bytes_written: Arc<AtomicU64>,
}

impl AsyncShareableBuffer {
    /// Consumes this instance and returns the underlying buffer.
    /// Returns `None` if there are other references to the instance.
    pub(super) async fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.buffer)
            .ok()
            .map(|lock| lock.into_inner())
    }

    /// Total bytes appended to the buffer so far.
    ///
    /// Synchronous counterpart to a buffer-length read; used to estimate the flushed size of a
    /// data file without holding the (non-`Sync`) parquet writer across an await.
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
