use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::time::sleep;
use tracing::instrument;
use uuid::Uuid;

use crate::clients::content::ContentValidationClient;
use crate::clients::error::ClientError;
use crate::config::CacheConfig;
use crate::domain::{
    ContentType, DomainError, LikeEvent, LikeEventKind, LikeRecord, PaginationCursor,
};
use crate::http::observability::{AppMetrics, LikeOperationLabel};
use crate::repository::{
    cache_repo::{CacheRepository, CachedLikeStatus},
    like_repo::LikeRepository,
};
use crate::service::broadcast::Broadcaster;

const COUNT_CACHE_LOCK_TTL_SECS: u64 = 1;
const COUNT_CACHE_RETRY_DELAY_MS: u64 = 25;
const COUNT_CACHE_RETRY_ATTEMPTS: u8 = 3;

/// Orchestrates authentication/content validation and like repository operations.
pub struct LikeService {
    repo: Arc<dyn LikeRepository>,
    cache: Arc<dyn CacheRepository>,
    content_client: Arc<dyn ContentValidationClient>,
    config: CacheConfig,
    broadcaster: Arc<Broadcaster>,
    metrics: Arc<AppMetrics>,
}

impl LikeService {
    pub fn new(
        config: CacheConfig,
        repo: Arc<dyn LikeRepository>,
        cache: Arc<dyn CacheRepository>,
        content_client: Arc<dyn ContentValidationClient>,
        broadcaster: Arc<Broadcaster>,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        Self { repo, cache, content_client, config, broadcaster, metrics }
    }

    /// Helper function to apply side effects after like/unlike operations, such as updating caches, recording metrics, and broadcasting events. This centralizes the logic for these side effects to ensure consistency across like and unlike operations.
    #[allow(clippy::too_many_arguments)]
    async fn apply_write_side_effects(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
        status: CachedLikeStatus,
        count_update: Option<i64>,
        metric: Option<LikeOperationLabel>,
        event: Option<(LikeEventKind, i64, DateTime<Utc>)>,
    ) {
        // Best-effort cache update
        let _ = self
            .cache
            .set_like_status(
                user_id,
                content_type.clone(),
                content_id,
                status,
                self.config.user_status_ttl_secs,
            )
            .await;

        // Only update the count cache if there was an actual change in like status
        if let Some(count) = count_update {
            let _ = self
                .cache
                .set_count(
                    content_type.clone(),
                    content_id,
                    count,
                    self.config.like_counts_ttl_secs,
                )
                .await;
        }

        // Record metrics for the like operation if applicable
        if let Some(operation) = metric {
            self.metrics.observe_like_operation(content_type.0.as_ref(), operation);
        }

        // Broadcast an event to SSE subscribers if applicable
        if let Some((event_kind, count, timestamp)) = event {
            self.broadcaster.broadcast(LikeEvent {
                event: event_kind,
                user_id,
                content_type,
                content_id,
                count,
                timestamp,
            });
        }
    }

    /// Handles a like operation: validates the token, checks content existence, and records the like.
    /// Returns (is_new_like, updated_count, timestamp) on success.
    #[instrument(skip(self), err)]
    pub async fn like(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64, DateTime<Utc>), DomainError> {
        let content_exists_in_cache = matches!(
            self.cache.get_content_exists(content_type.clone(), content_id).await,
            Ok(Some(true))
        );

        if !content_exists_in_cache {
            self.content_client.validate_content(content_type.clone(), content_id).await.map_err(
                |error| match error {
                    ClientError::NotFound => DomainError::ContentNotFound {
                        content_type: content_type.0.to_string(),
                        content_id,
                    },
                    other => DomainError::Client(other),
                },
            )?;

            let _ = self
                .cache
                .set_content_exists(
                    content_type.clone(),
                    content_id,
                    self.config.content_validation_ttl_secs,
                )
                .await;
        }

        let (already_existed, new_count, timestamp) =
            self.repo.insert_like(user_id, content_type.clone(), content_id).await?;

        self.apply_write_side_effects(
            user_id,
            content_type,
            content_id,
            CachedLikeStatus::Liked(timestamp),
            (!already_existed).then_some(new_count),
            (!already_existed).then_some(LikeOperationLabel::Like),
            Some((LikeEventKind::Like, new_count, timestamp)),
        )
        .await;

        Ok((already_existed, new_count, timestamp))
    }

    /// Handles an unlike operation: validates the token and removes the like record.
    /// Returns (was_liked, updated_count) on success.
    #[instrument(skip(self), err)]
    pub async fn unlike(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64), DomainError> {
        let (was_liked, new_count) =
            self.repo.delete_like(user_id, content_type.clone(), content_id).await?;

        self.apply_write_side_effects(
            user_id,
            content_type,
            content_id,
            CachedLikeStatus::Unliked,
            was_liked.then_some(new_count),
            was_liked.then_some(LikeOperationLabel::Unlike),
            was_liked.then_some((LikeEventKind::Unlike, new_count, Utc::now())),
        )
        .await;

        Ok((was_liked, new_count))
    }

    /// Retrieves the total like count for a given content item.
    /// DDIA Paranoia Note: If strict bounds are required, this method could be
    /// wrapped in `tokio::time::timeout` to prevent hanging on DB connection starvation.
    #[instrument(skip(self), err)]
    pub async fn get_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<i64, DomainError> {
        // Try cache
        if let Some(count) = self.get_cached_count(content_type.clone(), content_id).await {
            return Ok(count);
        }

        // Cache Stampede (Thundering Herd) Prevention:
        // Only ONE concurrent request is allowed to query the DB for this content item.
        if self.acquire_count_lock_if_available(content_type.clone(), content_id).await {
            return self.refresh_count_cache_with_lock(content_type, content_id).await;
        }

        // Other concurrent requests wait in a poll-loop for the first request to populate the cache.
        if let Some(count) = self.wait_for_cached_count(content_type.clone(), content_id).await {
            return Ok(count);
        }

        // Fallback: If lock holder died or cache is completely down, query DB directly
        self.refresh_count_cache(content_type, content_id).await
    }

    /// Helper function to get like count from cache. Returns None if not present or on error.
    async fn get_cached_count(&self, content_type: ContentType, content_id: Uuid) -> Option<i64> {
        self.cache.get_count(content_type, content_id).await.ok().flatten()
    }

    /// Helper function to acquire a lock for count recomputation when Redis allows it.
    async fn acquire_count_lock_if_available(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> bool {
        self.cache
            .try_acquire_count_lock(content_type, content_id, COUNT_CACHE_LOCK_TTL_SECS)
            .await
            .unwrap_or(false)
    }

    /// Helper function to refresh the like count cache while ensuring the lock is released afterwards, even if the DB call fails.
    async fn refresh_count_cache_with_lock(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<i64, DomainError> {
        let result = self.refresh_count_cache(content_type.clone(), content_id).await;
        let _ = self.cache.release_count_lock(content_type, content_id).await;
        result
    }

    /// Helper function to wait for a short period, polling the cache for the like count to be populated by another request that acquired the lock. Returns the count if it becomes available within the retry attempts, or None if it still isn't available after retries.
    async fn wait_for_cached_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Option<i64> {
        for _ in 0..COUNT_CACHE_RETRY_ATTEMPTS {
            sleep(Duration::from_millis(COUNT_CACHE_RETRY_DELAY_MS)).await;

            if let Some(count) = self.get_cached_count(content_type.clone(), content_id).await {
                return Some(count);
            }
        }

        None
    }

    /// Helper function to query the database for the like count and populate the cache. Used both for the slow path when cache is missed and for refreshing the cache after acquiring the lock.
    async fn refresh_count_cache(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<i64, DomainError> {
        let count = self.repo.get_count(content_type.clone(), content_id).await?;

        // This blind cache write can lose a race against a concurrent like/unlike that already
        // wrote a newer count into Redis after our DB read completed. 5-minute TTL is good enough for this social use case.
        // If we need strict ordering later, this path should switch to a Lua compare/set style
        // update that only fills the cache when the key is still absent.
        let _ = self
            .cache
            .set_count(content_type, content_id, count, self.config.like_counts_ttl_secs)
            .await;

        Ok(count)
    }

    /// Retrieves the like status for a specific user and content item.
    /// Returns the timestamp of when the user liked the item, or None if not liked.
    /// Uses a short-lived Redis cache to mask asynchronous replication lag after writes.
    #[instrument(skip(self), err)]
    pub async fn get_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<DateTime<Utc>>, DomainError> {
        // DDIA Consistency: By checking the cache first, we guarantee Read-After-Write
        // consistency for the user, hiding the replication lag of the DB reader_pool.
        if let Some(status) =
            self.cache.get_like_status(user_id, content_type.clone(), content_id).await?
        {
            return Ok(match status {
                CachedLikeStatus::Liked(timestamp) => Some(timestamp),
                CachedLikeStatus::Unliked => None,
            });
        }

        let status = self.repo.get_status(user_id, content_type, content_id).await?;

        Ok(status)
    }

    /// Retrieves a list of content items that the authenticated user has liked, optionally filtered by content type.
    #[instrument(skip(self), err)]
    pub async fn get_user_likes(
        &self,
        user_id: Uuid,
        content_type: Option<ContentType>,
        cursor: Option<PaginationCursor>,
        limit: i64,
    ) -> Result<Vec<LikeRecord>, DomainError> {
        let likes = self.repo.get_user_likes(user_id, content_type, cursor, limit).await?;

        Ok(likes)
    }

    /// Batch retrieves like counts for multiple content items in a single request.
    #[instrument(skip(self, items), fields(batch_size = items.len()), err)]
    pub async fn batch_get_counts(
        &self,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<i64>, DomainError> {
        // Short-circuit for empty input to avoid unnecessary cache/DB calls
        if items.is_empty() {
            return Ok(Vec::new());
        }

        // Try cache first
        let cached_counts = self.cache.batch_get_counts(items).await?;
        let mut counts = Vec::with_capacity(items.len());
        let mut missing_items = Vec::new();
        let mut missing_indexes = Vec::new();

        for (index, item) in items.iter().enumerate() {
            match cached_counts.get(index).copied().flatten() {
                Some(count) => counts.push(count),
                None => {
                    counts.push(0);
                    missing_indexes.push(index);
                    missing_items.push(item.clone());
                }
            }
        }

        // If there are any cache misses, fetch the counts from the database and update the cache in batch.
        if !missing_items.is_empty() {
            let fresh_counts = self.repo.batch_get_counts(&missing_items).await?;
            let cache_updates: Vec<(ContentType, Uuid, i64)> = missing_items
                .iter()
                .cloned()
                .zip(fresh_counts.iter().copied())
                .map(|((content_type, content_id), count)| (content_type, content_id, count))
                .collect();

            for (missing_index, fresh_count) in missing_indexes.into_iter().zip(fresh_counts) {
                counts[missing_index] = fresh_count;
            }

            // Update the cache with the fresh counts
            let _ =
                self.cache.set_batch_counts(&cache_updates, self.config.like_counts_ttl_secs).await;
        }

        Ok(counts)
    }

    /// Batch retrieves like statuses for multiple content items for the authenticated user.
    /// Intentionally bypasses cache for now because these results are user-scoped
    #[instrument(skip(self, items), fields(batch_size = items.len(), user_id = %user_id), err)]
    pub async fn batch_get_statuses(
        &self,
        user_id: Uuid,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<DateTime<Utc>>>, DomainError> {
        let statuses = self.repo.batch_get_statuses(user_id, items).await?;

        Ok(statuses)
    }

    /// Retrieves the top liked content items, optionally filtered by content type and time range.
    #[instrument(skip(self), err)]
    pub async fn get_top_liked(
        &self,
        content_type: Option<ContentType>,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, DomainError> {
        let now = Utc::now();

        // Map the datetime back to a string key for the cache lookup
        let window_str = match since {
            Some(dt) if dt > now - chrono::Duration::hours(25) => "24h",
            Some(dt) if dt > now - chrono::Duration::days(8) => "7d",
            Some(dt) if dt > now - chrono::Duration::days(31) => "30d",
            None => "all_time",
            _ => {
                return Err(DomainError::InvalidTimeWindow(
                    "Custom windows not supported for Leaderboard".to_string(),
                ));
            }
        };

        // Try cache first
        if let Ok(cached_items) =
            self.cache.get_leaderboard(content_type.clone(), window_str, limit).await
        {
            if !cached_items.is_empty() {
                return Ok(cached_items);
            }
        }

        // Slow path - query DB directly
        let top_liked = self.repo.get_top_liked(content_type, since, limit).await?;

        Ok(top_liked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::content::MockContentValidationClient;
    use crate::http::observability::AppMetrics;
    use crate::repository::cache_repo::MockCacheRepository;
    use crate::repository::error::RepoError;
    use crate::repository::like_repo::MockLikeRepository;
    use chrono::TimeZone;
    use mockall::{Sequence, predicate::eq};
    use sqlx::Error as SqlxError;
    use std::sync::Arc;

    fn test_metrics() -> Arc<AppMetrics> {
        Arc::new(AppMetrics::new())
    }

    #[tokio::test]
    async fn test_like_new_updates_cache() {
        let config = CacheConfig::default();
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_insert_like()
            .times(1)
            .returning(move |_, _, _| Ok((false, 42, timestamp)));

        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));
        mock_cache
            .expect_set_like_status()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
                eq(CachedLikeStatus::Liked(timestamp)),
                eq(config.user_status_ttl_secs),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        // NEW like, MUST update the cache
        mock_cache
            .expect_set_count()
            .with(eq(ct.clone()), eq(content_id), eq(42), eq(config.like_counts_ttl_secs))
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let service = LikeService::new(
            config,
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );
        let result = service.like(user_id, ct, content_id).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_like_existing_skips_cache() {
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_insert_like().times(1).returning(move |_, _, _| Ok((true, 42, timestamp)));

        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));
        mock_cache
            .expect_set_like_status()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
                eq(CachedLikeStatus::Liked(timestamp)),
                eq(CacheConfig::default().user_status_ttl_secs),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        // ALREADY EXISTED (true), it MUST NOT call the count cache
        mock_cache.expect_set_count().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );
        let result = service.like(user_id, ct, content_id).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_like_repo_error_bubbles_up() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let mut mock_cache = MockCacheRepository::new();
        let mut mock_repo = MockLikeRepository::new();

        // Force the repo to return a Database error
        mock_repo
            .expect_insert_like()
            .times(1)
            .returning(|_, _, _| Err(RepoError::Db(SqlxError::RowNotFound)));

        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));
        mock_cache.expect_set_like_status().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.like(user_id, ct, content_id).await;

        // Assert
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            DomainError::Repository(RepoError::Db(SqlxError::RowNotFound))
        ));
    }

    #[tokio::test]
    async fn test_unlike_existing_updates_cache() {
        // Arrange
        let config = CacheConfig::default();
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let like_count_after = 41;

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_delete_like()
            .with(eq(user_id), eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(move |_, _, _| Ok((true, like_count_after))); // Returns: (was_liked, new_count)

        // EXISTING like was removed, MUST update the cache
        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_set_like_status()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
                eq(CachedLikeStatus::Unliked),
                eq(config.user_status_ttl_secs),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        mock_cache
            .expect_set_count()
            .with(
                eq(ct.clone()),
                eq(content_id),
                eq(like_count_after),
                eq(config.like_counts_ttl_secs),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let service = LikeService::new(
            config,
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.unlike(user_id, ct, content_id).await;

        // Assert
        assert!(result.is_ok());
        let (was_liked, count) = result.unwrap();
        assert!(was_liked);
        assert_eq!(count, like_count_after);
    }

    #[tokio::test]
    async fn test_unlike_non_existing_skips_cache() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_delete_like().times(1).returning(move |_, _, _| Ok((false, 42))); // Returns: (was_liked=false, count)

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_set_like_status()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
                eq(CachedLikeStatus::Unliked),
                eq(CacheConfig::default().user_status_ttl_secs),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        // NEVER EXISTED (false), it MUST NOT call Redis
        mock_cache.expect_set_count().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.unlike(user_id, ct, content_id).await;

        // Assert
        assert!(result.is_ok());
        let (was_liked, _) = result.unwrap();
        assert!(!was_liked);
    }

    #[tokio::test]
    async fn test_unlike_repo_error_bubbles_up() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let mock_cache = MockCacheRepository::new();
        let mut mock_repo = MockLikeRepository::new();

        // Force the repo to return a Database error
        mock_repo
            .expect_delete_like()
            .times(1)
            .returning(|_, _, _| Err(RepoError::Db(SqlxError::RowNotFound)));

        let mut mock_cache = mock_cache;
        mock_cache.expect_set_like_status().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.unlike(user_id, ct, content_id).await;

        // Assert
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            DomainError::Repository(RepoError::Db(SqlxError::RowNotFound))
        ));
    }

    #[tokio::test]
    async fn test_get_count_cache_hit_returns_early() {
        let ct = ContentType::from("bonus_hunter");
        let content_id = Uuid::new_v4();

        let mut mock_repo = MockLikeRepository::new();
        // The DB should NEVER be called on a cache hit!
        mock_repo.expect_get_count().times(0);

        let mut mock_cache = MockCacheRepository::new();
        // Simulate a cache hit
        mock_cache
            .expect_get_count()
            .with(eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(|_, _| Ok(Some(99)));

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_count(ct, content_id).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 99);
    }

    #[tokio::test]
    async fn test_get_count_cache_miss_calls_db_and_populates_cache() {
        let config = CacheConfig::default();
        let ct = ContentType::from("bonus_hunter");
        let content_id = Uuid::new_v4();
        let db_count = 150;

        let mut mock_repo = MockLikeRepository::new();
        // The DB MUST be called because the cache missed
        mock_repo
            .expect_get_count()
            .with(eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(move |_, _| Ok(db_count));

        let mut mock_cache = MockCacheRepository::new();

        // 1. First, the service tries the cache and misses
        mock_cache.expect_get_count().times(1).returning(|_, _| Ok(None));
        mock_cache
            .expect_try_acquire_count_lock()
            .with(eq(ct.clone()), eq(content_id), eq(COUNT_CACHE_LOCK_TTL_SECS))
            .times(1)
            .returning(|_, _, _| Ok(true));

        // 2. Second, the service populates the cache with the DB result
        mock_cache
            .expect_set_count()
            .with(eq(ct.clone()), eq(content_id), eq(db_count), eq(config.like_counts_ttl_secs))
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        mock_cache
            .expect_release_count_lock()
            .with(eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(|_, _| Ok(()));

        let service = LikeService::new(
            config,
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_count(ct, content_id).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), db_count);
    }

    #[tokio::test]
    async fn test_get_count_waits_for_locked_refresh_and_uses_cached_value() {
        let ct = ContentType::from("bonus_hunter");
        let content_id = Uuid::new_v4();
        let mut sequence = Sequence::new();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_count().times(0);

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_count()
            .with(eq(ct.clone()), eq(content_id))
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(|_, _| Ok(None));
        mock_cache
            .expect_try_acquire_count_lock()
            .with(eq(ct.clone()), eq(content_id), eq(COUNT_CACHE_LOCK_TTL_SECS))
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_, _, _| Ok(false));
        mock_cache
            .expect_get_count()
            .with(eq(ct.clone()), eq(content_id))
            .times(1)
            .in_sequence(&mut sequence)
            .return_once(|_, _| Ok(Some(150)));

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_count(ct, content_id).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 150);
    }

    #[tokio::test]
    async fn test_get_status_liked() {
        // Arrange
        let user_id = Uuid::new_v4();
        let content_type = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let expected_time = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();
        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_like_status()
            .with(eq(user_id), eq(content_type.clone()), eq(content_id))
            .times(1)
            .returning(|_, _, _| Ok(None));
        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_status()
            .with(eq(user_id), eq(content_type.clone()), eq(content_id))
            .times(1)
            .returning(move |_, _, _| Ok(Some(expected_time)));

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.get_status(user_id, content_type, content_id).await;

        // Assert
        assert_eq!(result.unwrap(), Some(expected_time));
    }

    #[tokio::test]
    async fn test_get_status_uses_short_lived_cache_hit() {
        let user_id = Uuid::new_v4();
        let content_type = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let expected_time = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_like_status()
            .with(eq(user_id), eq(content_type.clone()), eq(content_id))
            .times(1)
            .returning(move |_, _, _| Ok(Some(CachedLikeStatus::Liked(expected_time))));

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_status().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_status(user_id, content_type, content_id).await;

        assert_eq!(result.unwrap(), Some(expected_time));
    }

    #[tokio::test]
    async fn test_get_status_uses_short_lived_cache_for_unliked_state() {
        let user_id = Uuid::new_v4();
        let content_type = ContentType::from("post");
        let content_id = Uuid::new_v4();

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_like_status()
            .with(eq(user_id), eq(content_type.clone()), eq(content_id))
            .times(1)
            .returning(|_, _, _| Ok(Some(CachedLikeStatus::Unliked)));

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_status().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_status(user_id, content_type, content_id).await;

        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_batch_get_counts() {
        // Arrange
        let config = CacheConfig::default();
        let ct1 = ContentType::from("post");
        let ct2 = ContentType::from("bonus_hunter");
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let items = vec![(ct1.clone(), id1), (ct2.clone(), id2)];

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_batch_get_counts()
            .with(eq(vec![(ct2.clone(), id2)]))
            .times(1)
            .returning(|_| Ok(vec![200]));

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_batch_get_counts()
            .with(eq(items.clone()))
            .times(1)
            .returning(|_| Ok(vec![Some(100), None]));
        mock_cache
            .expect_set_batch_counts()
            .with(eq(vec![(ct2.clone(), id2, 200)]), eq(config.like_counts_ttl_secs))
            .times(1)
            .returning(|_, _| Ok(()));

        let service = LikeService::new(
            config,
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.batch_get_counts(&items).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![100, 200]);
    }

    #[tokio::test]
    async fn test_batch_get_counts_uses_cache_when_all_items_are_present() {
        let ct1 = ContentType::from("post");
        let ct2 = ContentType::from("bonus_hunter");
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let items = vec![(ct1, id1), (ct2, id2)];

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_batch_get_counts().times(0);

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_batch_get_counts()
            .with(eq(items.clone()))
            .times(1)
            .returning(|_| Ok(vec![Some(10), Some(20)]));
        mock_cache.expect_set_batch_counts().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.batch_get_counts(&items).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![10, 20]);
    }

    #[tokio::test]
    async fn test_batch_get_counts_partial_cache_hit() {
        let ct1 = ContentType::from("post");
        let id1 = Uuid::new_v4(); // Will be in cache
        let id2 = Uuid::new_v4(); // Will be a cache MISS

        let mut mock_cache = MockCacheRepository::new();
        // Cache returns Some(10) for id1, and None for id2
        mock_cache.expect_batch_get_counts().times(1).returning(move |_| Ok(vec![Some(10), None]));

        let mut mock_repo = MockLikeRepository::new();
        // DB should ONLY be queried for id2!
        mock_repo
            .expect_batch_get_counts()
            .with(eq(vec![(ct1.clone(), id2)]))
            .times(1)
            .returning(|_| Ok(vec![20]));

        // Mock setting the missing item back into the cache after fetching from the DB
        mock_cache
            .expect_set_batch_counts()
            .with(eq(vec![(ct1.clone(), id2, 20)]), eq(CacheConfig::default().like_counts_ttl_secs))
            .times(1)
            .returning(|_, _| Ok(()));

        // Act
        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );
        let results =
            service.batch_get_counts(&[(ct1.clone(), id1), (ct1.clone(), id2)]).await.unwrap();

        // Assert: Results are merged in correct order
        assert_eq!(results, vec![10, 20]);
    }

    #[tokio::test]
    async fn test_batch_get_statuses() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct1 = ContentType::from("post");
        let id1 = Uuid::new_v4();
        let ts = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();
        let mock_cache = MockCacheRepository::new();
        let items = vec![(ct1.clone(), id1)];

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_batch_get_statuses()
            .with(eq(user_id), eq(items.clone()))
            .times(1)
            .returning(move |_, _| Ok(vec![Some(ts)]));

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.batch_get_statuses(user_id, &items).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![Some(ts)]);
    }

    #[tokio::test]
    async fn test_get_top_liked() {
        // Arrange
        let content_type = ContentType::from("post");
        let since = Utc::now() - chrono::Duration::hours(1);
        let limit = 10;
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let ct_clone1 = content_type.clone();
        let ct_clone2 = content_type.clone();

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_leaderboard()
            .with(eq(Some(content_type.clone())), eq("24h"), eq(limit))
            .times(1)
            .returning(|_, _, _| Ok(vec![]));

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_top_liked()
            .with(eq(Some(content_type.clone())), eq(Some(since)), eq(limit))
            .times(1)
            .returning(move |_, _, _| {
                Ok(vec![(ct_clone1.clone(), id1, 1500), (ct_clone2.clone(), id2, 900)])
            });

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        // Act
        let result = service.get_top_liked(Some(content_type), Some(since), limit).await;

        // Assert
        assert!(result.is_ok());
        let top = result.unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].2, 1500);
        assert_eq!(top[1].2, 900);
    }

    #[tokio::test]
    async fn test_get_top_liked_cache_hit_for_specific_content_type() {
        let content_type = ContentType::from("post");
        let expected_ct = content_type.clone();
        let since = Utc::now() - chrono::Duration::hours(1);
        let limit = 10;
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_leaderboard()
            .with(eq(Some(content_type.clone())), eq("24h"), eq(limit))
            .times(1)
            .returning(move |_, _, _| {
                Ok(vec![(expected_ct.clone(), id1, 1500), (expected_ct.clone(), id2, 900)])
            });

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_top_liked().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_top_liked(Some(content_type.clone()), Some(since), limit).await;

        assert!(result.is_ok());
        let top = result.unwrap();
        assert_eq!(top, vec![(content_type.clone(), id1, 1500), (content_type, id2, 900)]);
    }

    #[tokio::test]
    async fn test_get_top_liked_global_cache_hit_returns_early() {
        let limit = 10;
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let ct1 = ContentType::from("post");
        let ct2 = ContentType::from("bonus_hunter");

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_leaderboard()
            .with(eq(None), eq("all_time"), eq(limit))
            .times(1)
            .returning(move |_, _, _| Ok(vec![(ct1.clone(), id1, 1500), (ct2.clone(), id2, 900)]));

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_top_liked().times(0);

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_top_liked(None, None, limit).await;

        assert!(result.is_ok());
        let top = result.unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].1, id1);
        assert_eq!(top[1].1, id2);
    }

    #[tokio::test]
    async fn test_get_top_liked_global_cache_miss_hits_db() {
        let limit = 10;
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let ct1 = ContentType::from("post");
        let ct2 = ContentType::from("bonus_hunter");

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_leaderboard()
            .with(eq(None), eq("all_time"), eq(limit))
            .times(1)
            .returning(|_, _, _| Ok(vec![]));

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_top_liked()
            .with(eq(None), eq(None), eq(limit))
            .times(1)
            .returning(move |_, _, _| Ok(vec![(ct1.clone(), id1, 1500), (ct2.clone(), id2, 900)]));

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.get_top_liked(None, None, limit).await;

        assert!(result.is_ok());
        let top = result.unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].1, id1);
        assert_eq!(top[1].1, id2);
    }

    #[tokio::test]
    async fn test_get_user_likes_with_cursor_and_filter() {
        let user_id = Uuid::new_v4();
        let content_type = ContentType::from("top_picks");
        let limit = 20;
        let ts = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();
        let cursor_id = Uuid::new_v4();
        let cursor = PaginationCursor { created_at: ts, id: cursor_id };
        let mock_cache = MockCacheRepository::new();
        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_user_likes()
            // Verify the service passes down the optional values correctly
            .with(
                eq(user_id),
                eq(Some(content_type.clone())),
                eq(Some(PaginationCursor { created_at: ts, id: cursor_id })),
                eq(limit),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(vec![])); // Empty vec is fine for this assertion

        let service = LikeService::new(
            CacheConfig::default(),
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(MockContentValidationClient::new()),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );
        let result = service.get_user_likes(user_id, Some(content_type), Some(cursor), limit).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_like_content_cache_miss_calls_client_and_populates_cache() {
        let config = CacheConfig::default();
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_insert_like()
            .with(eq(user_id), eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(move |_, _, _| Ok((false, 42, timestamp)));

        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(None));
        mock_cache
            .expect_set_content_exists()
            .with(eq(ct.clone()), eq(content_id), eq(config.content_validation_ttl_secs))
            .times(1)
            .returning(|_, _, _| Ok(()));
        mock_cache
            .expect_set_like_status()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
                eq(CachedLikeStatus::Liked(timestamp)),
                eq(config.user_status_ttl_secs),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        mock_cache
            .expect_set_count()
            .with(eq(ct.clone()), eq(content_id), eq(42), eq(config.like_counts_ttl_secs))
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let mut mock_content_client = MockContentValidationClient::new();
        mock_content_client
            .expect_validate_content()
            .with(eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(|_, _| Ok(()));

        let service = LikeService::new(
            config,
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(mock_content_client),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.like(user_id, ct, content_id).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_like_content_cache_hit_skips_client() {
        let config = CacheConfig::default();
        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_insert_like()
            .with(eq(user_id), eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(move |_, _, _| Ok((false, 42, timestamp)));

        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));
        mock_cache.expect_set_content_exists().times(0);
        mock_cache
            .expect_set_like_status()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
                eq(CachedLikeStatus::Liked(timestamp)),
                eq(config.user_status_ttl_secs),
            )
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        mock_cache
            .expect_set_count()
            .with(eq(ct.clone()), eq(content_id), eq(42), eq(config.like_counts_ttl_secs))
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let mut mock_content_client = MockContentValidationClient::new();
        mock_content_client.expect_validate_content().times(0);

        let service = LikeService::new(
            config,
            Arc::new(mock_repo),
            Arc::new(mock_cache),
            Arc::new(mock_content_client),
            Arc::new(Broadcaster::new(16)),
            test_metrics(),
        );

        let result = service.like(user_id, ct, content_id).await;

        assert!(result.is_ok());
    }
}
