use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::clients::content::ContentValidationClient;
use crate::clients::error::ClientError;
use crate::config::CacheConfig;
use crate::domain::{
    ContentType, DomainError, LikeEvent, LikeEventKind, LikeRecord, PaginationCursor,
};
use crate::http::observability::AppMetrics;
use crate::repository::{cache_repo::CacheRepository, like_repo::LikeRepository};
use crate::service::broadcast::Broadcaster;

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

    /// Handles a like operation: validates the token, checks content existence, and records the like.
    /// Returns (is_new_like, updated_count, timestamp) on success.
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

        // Update cache if this was a new like
        if !already_existed {
            let _ = self
                .cache
                .set_count(
                    content_type.clone(),
                    content_id,
                    new_count,
                    self.config.like_counts_ttl_secs,
                )
                .await;

            self.metrics.observe_like(content_type.0.as_ref(), "like");
        }

        // Broadcast the like event to subscribers
        self.broadcaster.broadcast(LikeEvent {
            event: LikeEventKind::Like,
            user_id,
            content_type,
            content_id,
            count: new_count,
            timestamp,
        });

        Ok((already_existed, new_count, timestamp))
    }

    /// Handles an unlike operation: validates the token and removes the like record.
    /// Returns (was_liked, updated_count) on success.
    pub async fn unlike(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64), DomainError> {
        let (was_liked, new_count) =
            self.repo.delete_like(user_id, content_type.clone(), content_id).await?;

        // Update cache if there was an actual like removed
        if was_liked {
            let _ = self
                .cache
                .set_count(
                    content_type.clone(),
                    content_id,
                    new_count,
                    self.config.like_counts_ttl_secs,
                )
                .await;

            self.metrics.observe_like(content_type.0.as_ref(), "unlike");

            // Broadcast the unlike event to subscribers
            self.broadcaster.broadcast(LikeEvent {
                event: LikeEventKind::Unlike,
                user_id,
                content_type,
                content_id,
                count: new_count,
                timestamp: Utc::now(),
            });
        }

        Ok((was_liked, new_count))
    }

    /// Retrieves the total like count for a given content item.
    pub async fn get_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<i64, DomainError> {
        // Try cache
        if let Ok(Some(count)) = self.cache.get_count(content_type.clone(), content_id).await {
            return Ok(count);
        }

        // Cache miss or cache failure, fallback to DB
        let count = self.repo.get_count(content_type.clone(), content_id).await?;

        // Populate cache
        let _ = self
            .cache
            .set_count(content_type, content_id, count, self.config.like_counts_ttl_secs)
            .await;

        Ok(count)
    }

    /// Retrieves the like status for a specific user and content item.
    /// Returns the timestamp of when the user liked the item, or None if not liked.
    /// Does not cache user-specific like status
    pub async fn get_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<DateTime<Utc>>, DomainError> {
        let status = self.repo.get_status(user_id, content_type, content_id).await?;

        Ok(status)
    }

    /// Retrieves a list of content items that the authenticated user has liked, optionally filtered by content type.
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
    pub async fn batch_get_counts(
        &self,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<i64>, DomainError> {
        let counts = self.repo.batch_get_counts(items).await?;

        Ok(counts)
    }

    /// Batch retrieves like statuses for multiple content items for the authenticated user.
    pub async fn batch_get_statuses(
        &self,
        user_id: Uuid,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<DateTime<Utc>>>, DomainError> {
        let statuses = self.repo.batch_get_statuses(user_id, items).await?;

        Ok(statuses)
    }

    /// Retrieves the top liked content items, optionally filtered by content type and time range.
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
    use mockall::predicate::eq;
    use sqlx::Error as SqlxError;
    use std::sync::Arc;

    // Helper to create a dummy ContentType
    fn content_type(raw: &str) -> ContentType {
        ContentType(Arc::from(raw.to_string()))
    }

    fn test_metrics() -> Arc<AppMetrics> {
        Arc::new(AppMetrics::new())
    }

    #[tokio::test]
    async fn test_like_new_updates_cache() {
        let config = CacheConfig::default();
        let user_id = Uuid::new_v4();
        let ct = content_type("post");
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_insert_like()
            .times(1)
            .returning(move |_, _, _| Ok((false, 42, timestamp)));

        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));
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
        let ct = content_type("post");
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_insert_like().times(1).returning(move |_, _, _| Ok((true, 42, timestamp)));

        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));
        // ALREADY EXISTED (true), it MUST NOT call Redis
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
        let ct = content_type("post");
        let content_id = Uuid::new_v4();
        let mut mock_cache = MockCacheRepository::new();
        let mut mock_repo = MockLikeRepository::new();

        // Force the repo to return a Database error
        mock_repo
            .expect_insert_like()
            .times(1)
            .returning(|_, _, _| Err(RepoError::Db(SqlxError::RowNotFound)));

        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));

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
        let ct = content_type("post");
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
        assert_eq!(was_liked, true);
        assert_eq!(count, like_count_after);
    }

    #[tokio::test]
    async fn test_unlike_non_existing_skips_cache() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = content_type("post");
        let content_id = Uuid::new_v4();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_delete_like().times(1).returning(move |_, _, _| Ok((false, 42))); // Returns: (was_liked=false, count)

        let mut mock_cache = MockCacheRepository::new();
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
        assert_eq!(was_liked, false);
    }

    #[tokio::test]
    async fn test_unlike_repo_error_bubbles_up() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = content_type("post");
        let content_id = Uuid::new_v4();
        let mock_cache = MockCacheRepository::new();
        let mut mock_repo = MockLikeRepository::new();

        // Force the repo to return a Database error
        mock_repo
            .expect_delete_like()
            .times(1)
            .returning(|_, _, _| Err(RepoError::Db(SqlxError::RowNotFound)));

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
        let ct = content_type("bonus_hunter");
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
        let ct = content_type("bonus_hunter");
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

        // 2. Second, the service populates the cache with the DB result
        mock_cache
            .expect_set_count()
            .with(eq(ct.clone()), eq(content_id), eq(db_count), eq(config.like_counts_ttl_secs))
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

        let result = service.get_count(ct, content_id).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), db_count);
    }

    #[tokio::test]
    async fn test_get_status_liked() {
        // Arrange
        let user_id = Uuid::new_v4();
        let content_type = content_type("post");
        let content_id = Uuid::new_v4();
        let expected_time = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();
        let mock_cache = MockCacheRepository::new();
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

    // TODO: add pagination tests

    #[tokio::test]
    async fn test_batch_get_counts() {
        // Arrange
        let ct1 = content_type("post");
        let ct2 = content_type("bonus_hunter");
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mock_cache = MockCacheRepository::new();
        let items = vec![(ct1.clone(), id1), (ct2.clone(), id2)];

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_batch_get_counts()
            .with(eq(items.clone()))
            .times(1)
            .returning(|_| Ok(vec![100, 200]));

        let service = LikeService::new(
            CacheConfig::default(),
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
    async fn test_batch_get_statuses() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct1 = content_type("post");
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
        let content_type = content_type("post");
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
        let content_type = content_type("post");
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
        let ct1 = content_type("post");
        let ct2 = content_type("bonus_hunter");

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
        let ct1 = content_type("post");
        let ct2 = content_type("bonus_hunter");

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
        let content_type = content_type("top_picks");
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
        let ct = content_type("post");
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
        let ct = content_type("post");
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
