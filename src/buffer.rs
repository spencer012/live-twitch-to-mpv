//! Byte-accounted bounded channel, analogous to streamlink's RingBuffer.
//!
//! Senders acquire one semaphore permit per byte, so the total amount of
//! buffered-but-unread data never exceeds the configured capacity. This gives
//! the segment pipeline backpressure against a slow player.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};

pub fn byte_channel(capacity: usize) -> (ByteSender, ByteReceiver) {
    let semaphore = Arc::new(Semaphore::new(capacity));
    let (tx, rx) = mpsc::unbounded_channel();
    (
        ByteSender {
            tx,
            semaphore,
            capacity,
        },
        ByteReceiver { rx },
    )
}

#[derive(Debug)]
pub struct ChannelClosed;

pub struct ByteSender {
    tx: mpsc::UnboundedSender<(Bytes, tokio::sync::OwnedSemaphorePermit)>,
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl ByteSender {
    /// Send bytes into the buffer, waiting for free space if the buffer is
    /// full. Chunks larger than the total capacity are split.
    pub async fn send(&self, data: Bytes) -> Result<(), ChannelClosed> {
        let mut data = data;
        while !data.is_empty() {
            let chunk = data.split_to(data.len().min(self.capacity));
            let permit = self
                .semaphore
                .clone()
                .acquire_many_owned(chunk.len() as u32)
                .await
                .map_err(|_| ChannelClosed)?;
            self.tx.send((chunk, permit)).map_err(|_| ChannelClosed)?;
        }
        Ok(())
    }
}

pub struct ByteReceiver {
    rx: mpsc::UnboundedReceiver<(Bytes, tokio::sync::OwnedSemaphorePermit)>,
}

impl ByteReceiver {
    /// Receive the next chunk; `None` means the stream has ended.
    pub async fn recv(&mut self) -> Option<Bytes> {
        // Dropping the permit frees buffer space for the senders.
        self.rx.recv().await.map(|(bytes, _permit)| bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backpressure_and_order() {
        let (tx, mut rx) = byte_channel(8);
        tx.send(Bytes::from_static(b"12345678")).await.unwrap();

        // Buffer is full: the next send must wait until we read.
        let pending = tokio::spawn(async move {
            tx.send(Bytes::from_static(b"abc")).await.unwrap();
        });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());

        assert_eq!(rx.recv().await.unwrap(), Bytes::from_static(b"12345678"));
        pending.await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), Bytes::from_static(b"abc"));
    }

    #[tokio::test]
    async fn oversized_chunks_are_split() {
        let (tx, mut rx) = byte_channel(4);
        let sender = tokio::spawn(async move {
            tx.send(Bytes::from_static(b"123456789")).await.unwrap();
        });
        let mut out = Vec::new();
        while out.len() < 9 {
            out.extend_from_slice(&rx.recv().await.unwrap());
        }
        sender.await.unwrap();
        assert_eq!(out, b"123456789");
    }
}
