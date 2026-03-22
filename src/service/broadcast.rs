use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::domain::{ContentType, LikeEvent, LikeEventKind};

/// Broadcast system for real-time like events.
pub struct Broadcaster {
    /// Channels keyed by (ContentType, ContentID) to allow subscribers to listen to specific content or content types.
    channels: Arc<DashMap<(ContentType, Uuid), broadcast::Sender<LikeEvent>>>,
    /// Capacity of each broadcast channel.
    channel_capacity: usize,
}

impl Broadcaster {
    pub fn new(channel_capacity: usize) -> Self {
        Self { channels: Arc::new(DashMap::new()), channel_capacity }
    }

    /// Broadcasts a like event to all subscribers of the specific content type and content ID. If there are no subscribers, the event is dropped silently.
    pub fn broadcast(&self, event: LikeEvent) {
        let key = (event.content_type.clone(), event.content_id);
        let sender = self.channels.get(&key).map(|s| s.clone()); // Get a clone of the sender to avoid holding the lock while sending
        if let Some(sender) = sender {
            // If sending fails, it means there are no active subscribers, so we can remove the channel
            if sender.send(event).is_err() {
                self.channels.remove(&key);
            }
        }
    }

    /// Subscribes to like events for a specific content type and content ID, returning a broadcast receiver that will receive events for that content. If the channel doesn't exist yet, it will be created.
    pub fn subscribe(
        &self,
        content_type: ContentType,
        content_id: &Uuid,
    ) -> broadcast::Receiver<LikeEvent> {
        let key = (content_type, *content_id);

        // Get or create a channel for this content
        let sender = self.channels.entry(key).or_insert_with(|| {
            let (tx, _) = broadcast::channel(self.channel_capacity);
            tx
        });

        sender.subscribe()
    }

    /// Shuts down the broadcaster by sending a shutdown event to all subscribers and clearing all channels.
    pub fn shutdown(&self) {
        tracing::info!("Shutting down broadcaster and notifying all subscribers");

        for entry in self.channels.iter() {
            let (content_type, content_id) = &entry.key();
            let sender = entry.value();

            let shutdown_event = LikeEvent {
                event: LikeEventKind::Shutdown,
                content_type: content_type.clone(),
                content_id: *content_id,
                timestamp: chrono::Utc::now(),
                user_id: Uuid::nil(), // No specific user associated with shutdown events
                count: 0,             // Count is not relevant for shutdown events
            };

            let _ = sender.send(shutdown_event); // Ignore send errors since subscribers may have already dropped their receivers
        }

        self.channels.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::config::default_sse_channel_capacity;

    use super::*;
    use chrono::Utc;
    use tokio::time::{Duration, sleep};

    // Helper to create a test event
    fn create_event(
        kind: LikeEventKind,
        content_type: &str,
        content_id: Uuid,
        count: i64,
    ) -> LikeEvent {
        LikeEvent {
            event: kind,
            user_id: Uuid::new_v4(),
            content_type: ContentType(Arc::from(content_type)),
            content_id,
            count,
            timestamp: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_subscribe_and_receive_events() {
        let broadcaster = Broadcaster::new(default_sse_channel_capacity());
        let content_type = ContentType(Arc::from("post"));
        let content_id = Uuid::new_v4();

        // 1. Subscribe to the channel
        let mut rx = broadcaster.subscribe(content_type.clone(), &content_id);

        // 2. Broadcast an event
        let event = create_event(LikeEventKind::Like, "post", content_id, 42);
        broadcaster.broadcast(event.clone());

        // 3. Receive the event
        let received = rx.recv().await.expect("Failed to receive event");
        assert_eq!(received.count, 42);
        assert_eq!(received.event, LikeEventKind::Like);

        // 4. Broadcast a different event
        let unlike_event = create_event(LikeEventKind::Unlike, "post", content_id, 41);
        broadcaster.broadcast(unlike_event.clone());

        let received2 = rx.recv().await.expect("Failed to receive second event");
        assert_eq!(received2.count, 41);
        assert_eq!(received2.event, LikeEventKind::Unlike);
    }

    #[tokio::test]
    async fn test_isolated_channels() {
        let broadcaster = Broadcaster::new(default_sse_channel_capacity());
        let ct_post = ContentType(Arc::from("post"));
        let ct_video = ContentType(Arc::from("video"));
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut rx1 = broadcaster.subscribe(ct_post.clone(), &id1);
        let mut rx2 = broadcaster.subscribe(ct_video.clone(), &id2);

        // Send to channel 1
        broadcaster.broadcast(create_event(LikeEventKind::Like, "post", id1, 10));
        // Send to channel 2
        broadcaster.broadcast(create_event(LikeEventKind::Like, "video", id2, 20));

        // rx1 should only get the event for id1
        let received1 = rx1.recv().await.unwrap();
        assert_eq!(received1.content_id, id1);
        assert_eq!(received1.count, 10);

        // rx2 should only get the event for id2
        let received2 = rx2.recv().await.unwrap();
        assert_eq!(received2.content_id, id2);
        assert_eq!(received2.count, 20);
    }

    #[tokio::test]
    async fn test_channel_cleanup_when_empty() {
        let broadcaster = Broadcaster::new(default_sse_channel_capacity());
        let content_type = ContentType(Arc::from("post"));
        let content_id = Uuid::new_v4();

        // 1. Subscribe, creating the channel
        let rx = broadcaster.subscribe(content_type.clone(), &content_id);

        // Assert the channel exists in the registry
        assert!(broadcaster.channels.contains_key(&(content_type.clone(), content_id)));

        // 2. Drop the receiver to simulate a client disconnecting.
        drop(rx);

        // 3. Broadcast an event.
        // Because there are 0 receivers, `send()` will return an Error, triggering cleanup.
        broadcaster.broadcast(create_event(LikeEventKind::Like, "post", content_id, 1));

        // Yield to the Tokio runtime briefly to ensure the Drop propagates and the cleanup finishes
        sleep(Duration::from_millis(10)).await;

        // 4. Assert the channel was deleted
        assert!(!broadcaster.channels.contains_key(&(content_type, content_id)));
    }

    #[tokio::test]
    async fn test_shutdown_all_sends_shutdown_event_and_closes() {
        let broadcaster = Arc::new(Broadcaster::new(default_sse_channel_capacity()));
        let content_type = ContentType(Arc::from("post"));
        let content_id = Uuid::new_v4();

        // 1. Subscribe
        let mut rx = broadcaster.subscribe(content_type.clone(), &content_id);

        // 2. Trigger Shutdown in the background
        let b_clone = broadcaster.clone();
        tokio::spawn(async move {
            b_clone.shutdown();
        });

        // 3. The next event received MUST be Shutdown event
        let received = rx.recv().await.expect("Failed to receive shutdown event");
        assert_eq!(received.event, LikeEventKind::Shutdown);
        assert_eq!(received.content_id, content_id);

        // 4. The channel should immediately close because `channels.clear()` drops the Sender
        let next_result = rx.recv().await;
        assert!(matches!(next_result, Err(broadcast::error::RecvError::Closed)));

        // 5. Channels should be empty
        assert!(broadcaster.channels.is_empty());
    }

    #[tokio::test]
    async fn test_backpressure_handles_lagging_clients() {
        let broadcaster = Broadcaster::new(default_sse_channel_capacity());
        let content_type = ContentType(Arc::from("post"));
        let content_id = Uuid::new_v4();

        let mut rx = broadcaster.subscribe(content_type.clone(), &content_id);

        // Send 20 events instantly without reading them.
        let max = (default_sse_channel_capacity() + 4) as i64; // 20
        for i in 0..max {
            broadcaster.broadcast(create_event(LikeEventKind::Like, "post", content_id, i));
        }

        // The first 16 events will be received, but the next 4 will cause a lag error because the buffer is full and the client isn't keeping up.
        let result = rx.recv().await;

        assert!(matches!(result, Err(broadcast::error::RecvError::Lagged(4))));

        let next_event = rx.recv().await.unwrap();
        assert_eq!(next_event.count, 4); // The 5th event sent (index 4)
    }
}
