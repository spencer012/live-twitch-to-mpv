//! Byte-accounted bounded channel, analogous to streamlink's RingBuffer.
//!
//! Senders acquire one semaphore permit per byte, so the total amount of
//! buffered-but-unread data never exceeds the configured capacity. This gives
//! the segment pipeline backpressure against a slow player.
//!
//! Recorder mode additionally interleaves [`SlotMarker`]s with the data so
//! the consumer knows exactly which bytes belong to which segment. Markers
//! take no buffer space and are only ever sent when instrumentation is on;
//! [`ByteReceiver::recv`] skips them, so player mode sees only bytes.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::hls::events::SlotMarker;

enum Entry {
    Data(Bytes, OwnedSemaphorePermit),
    Marker(SlotMarker),
}

/// An item read from the buffer by an instrumented consumer.
#[derive(Debug)]
pub enum Item {
    Data(Bytes),
    Marker(SlotMarker),
}

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
    tx: mpsc::UnboundedSender<Entry>,
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
            self.tx
                .send(Entry::Data(chunk, permit))
                .map_err(|_| ChannelClosed)?;
        }
        Ok(())
    }

    /// Send an in-band marker (ordered with the data; takes no capacity).
    pub fn marker(&self, marker: SlotMarker) -> Result<(), ChannelClosed> {
        self.tx
            .send(Entry::Marker(marker))
            .map_err(|_| ChannelClosed)
    }
}

pub struct ByteReceiver {
    rx: mpsc::UnboundedReceiver<Entry>,
}

impl ByteReceiver {
    /// Receive the next chunk; `None` means the stream has ended.
    pub async fn recv(&mut self) -> Option<Bytes> {
        loop {
            match self.recv_item().await? {
                Item::Data(bytes) => return Some(bytes),
                Item::Marker(_) => continue,
            }
        }
    }

    /// Receive the next data chunk or marker; `None` means the stream ended.
    pub async fn recv_item(&mut self) -> Option<Item> {
        // Dropping the permit frees buffer space for the senders.
        Some(match self.rx.recv().await? {
            Entry::Data(bytes, _permit) => Item::Data(bytes),
            Entry::Marker(marker) => Item::Marker(marker),
        })
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
    async fn markers_are_ordered_and_skipped_by_recv() {
        let (tx, mut rx) = byte_channel(8);
        tx.send(Bytes::from_static(b"ab")).await.unwrap();
        tx.marker(SlotMarker::End { error: None }).unwrap();
        tx.send(Bytes::from_static(b"cd")).await.unwrap();
        drop(tx);
        assert!(matches!(rx.recv_item().await, Some(Item::Data(b)) if b == "ab"));
        assert!(matches!(
            rx.recv_item().await,
            Some(Item::Marker(SlotMarker::End { error: None }))
        ));
        // Plain recv() never surfaces markers.
        assert_eq!(rx.recv().await.unwrap(), Bytes::from_static(b"cd"));
        assert!(rx.recv().await.is_none());
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
