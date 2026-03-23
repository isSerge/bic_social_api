use std::sync::Arc;

use crate::domain::ContentType;
use crate::http::observability::AppMetrics;
use crate::repository::error::RepoError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use deadpool_redis::{
    Connection, Pool,
    redis::{AsyncCommands, Script},
};
use uuid::Uuid;

// Sentinel value used in the cache to represent an unliked status, since we want to distinguish between "not in cache" and "cached as unliked".
const UNLIKED_STATUS_SENTINEL: &str = "0";

/// Repository trait for caching like counts and other related data.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CacheRepository: Send + Sync {
    /// Gets the current like count for a specific content item. Returns None if not cached.
    async fn get_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<i64>, RepoError>;

    /// Gets the current like counts for multiple content items. Returns None for individual cache misses.
    async fn batch_get_counts(
        &self,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<i64>>, RepoError>;

    /// Sets the like count for a specific content item in the cache with a TTL.
    /// The TTL is used to ensure that cached counts are refreshed periodically from db.
    async fn set_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
        count: i64,
        ttl_secs: u64,
    ) -> Result<(), RepoError>;

    /// Sets multiple like counts in the cache with the same TTL.
    async fn set_batch_counts(
        &self,
        items: &[(ContentType, Uuid, i64)],
        ttl_secs: u64,
    ) -> Result<(), RepoError>;

    /// Gets a short-lived cached user-specific like status. Returns None on cache miss.
    async fn get_like_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<CachedLikeStatus>, RepoError>;

    /// Sets a short-lived cached user-specific like status.
    async fn set_like_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
        status: CachedLikeStatus,
        ttl_secs: u64,
    ) -> Result<(), RepoError>;

    /// Attempts to acquire a short-lived recomputation lock for a count cache entry.
    async fn try_acquire_count_lock(
        &self,
        content_type: ContentType,
        content_id: Uuid,
        ttl_secs: u64,
    ) -> Result<bool, RepoError>;

    /// Releases a recomputation lock for a count cache entry.
    async fn release_count_lock(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(), RepoError>;

    /// Gets the user_id associated with a token from the cache. Returns None if not cached or token is invalid.
    async fn get_token(&self, token: &str) -> Result<Option<Uuid>, RepoError>;

    /// Sets the user_id associated with a token in the cache with a TTL.
    async fn set_token(&self, token: &str, user_id: Uuid, ttl_secs: u64) -> Result<(), RepoError>;

    /// Returns true if the content item has been validated as existing. Returns None on cache miss.
    async fn get_content_exists(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<bool>, RepoError>;

    /// Marks a content item as validated to exist, with a TTL.
    async fn set_content_exists(
        &self,
        content_type: ContentType,
        content_id: Uuid,
        ttl_secs: u64,
    ) -> Result<(), RepoError>;

    /// Checks if a request is allowed by the rate limiter.
    async fn check_rate_limit(
        &self,
        key: &str,
        limit: u32,
        window_secs: u64,
    ) -> Result<RateLimitStatus, RepoError>;

    /// Sets the leaderboard data for a specific content type and time window in the cache.
    async fn set_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        items: Vec<(ContentType, Uuid, i64)>,
    ) -> Result<(), RepoError>;

    /// Gets the leaderboard data for a specific content type and time window from the cache.
    /// Returns an empty vector if not cached.
    async fn get_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, RepoError>;
}

/// Concrete implementation of CacheRepository using Redis.
pub struct RedisCacheRepository {
    pool: Pool,
    metrics: Arc<AppMetrics>,
}

/// Status returned by the rate limiter indicating whether the request is allowed, the current count of requests in the window, and how many seconds until the limit resets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitStatus {
    pub allowed: bool,
    pub current_count: u32,
    pub retry_after_secs: u64,
}

/// Short-lived cached like status used to hide replica lag for immediate status reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CachedLikeStatus {
    Liked(DateTime<Utc>),
    Unliked,
}

impl RedisCacheRepository {
    pub fn new(pool: Pool, metrics: Arc<AppMetrics>) -> Self {
        RedisCacheRepository { pool, metrics }
    }

    /// Helper function to generate Redis keys for like counts.
    fn count_key(content_type: ContentType, content_id: Uuid) -> String {
        format!("like_count:{}:{}", content_type.0, content_id)
    }

    /// Helper to consistently format the token cache key
    fn token_key(token: &str) -> String {
        format!("likes:token:{}", token)
    }

    /// Helper function to generate Redis keys for user-specific like status.
    fn like_status_key(user_id: Uuid, content_type: ContentType, content_id: Uuid) -> String {
        format!("like_status:{}:{}:{}", user_id, content_type.0, content_id)
    }

    /// Helper function to generate Redis keys for count recomputation locks to prevent thundering herd on cache misses.
    fn count_lock_key(content_type: ContentType, content_id: Uuid) -> String {
        format!("lock:{}:{}", content_type.0, content_id)
    }

    /// Helper function to generate Redis keys for leaderboard data.
    fn leaderboard_key(content_type: Option<ContentType>, window_name: &str) -> String {
        match content_type {
            Some(ct) => format!("leaderboard:{}:{}", ct.0, window_name),
            None => format!("leaderboard:all:{}", window_name),
        }
    }

    /// Helper function to generate Redis keys for content existence validation.
    fn content_exists_key(content_type: ContentType, content_id: Uuid) -> String {
        format!("content_exists:{}:{}", content_type.0, content_id)
    }

    /// Helper function to get a Redis connection, with error handling that allows the app to operate without cache if Redis is unavailable.
    async fn get_connection(&self) -> Option<Connection> {
        match self.pool.get().await {
            Ok(conn) => Some(conn),
            Err(e) => {
                tracing::warn!("Failed to get Redis connection: {}. Operating without cache.", e);
                None
            }
        }
    }

    fn observe_cache_result(&self, operation: &str, result: &str) {
        self.metrics.observe_cache_operation(operation, result);
    }
}

#[async_trait]
impl CacheRepository for RedisCacheRepository {
    async fn get_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<i64>, RepoError> {
        // Return Ok(None) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("get_count", "error");
            return Ok(None);
        };

        let key = Self::count_key(content_type, content_id);

        match conn.get::<_, Option<i64>>(&key).await {
            Ok(Some(count)) => {
                self.observe_cache_result("get_count", "hit");
                Ok(Some(count))
            }
            Ok(None) => {
                self.observe_cache_result("get_count", "miss");
                Ok(None)
            }
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis GET failed");
                self.observe_cache_result("get_count", "error");
                Ok(None)
            }
        }
    }

    async fn batch_get_counts(
        &self,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<i64>>, RepoError> {
        // If the input list is empty, return early with an empty result without querying Redis
        if items.is_empty() {
            self.observe_cache_result("batch_get_counts", "miss");
            return Ok(Vec::new());
        }

        // Return Ok(vec![None; items.len()]) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("batch_get_counts", "error");
            return Ok(vec![None; items.len()]);
        };

        // Generate the list of keys for MGET based on the content type and ID pairs
        let keys: Vec<String> = items
            .iter()
            .map(|(content_type, content_id)| Self::count_key(content_type.clone(), *content_id))
            .collect();

        let result: Result<Vec<Option<i64>>, _> =
            deadpool_redis::redis::cmd("MGET").arg(&keys).query_async(&mut conn).await;

        match result {
            Ok(counts) => {
                let metric_result = if counts.iter().any(Option::is_some) { "hit" } else { "miss" };
                self.observe_cache_result("batch_get_counts", metric_result);
                Ok(counts)
            }
            Err(error) => {
                tracing::warn!(error = %error, keys = ?keys, "Redis MGET for count batch failed");
                self.observe_cache_result("batch_get_counts", "error");
                Ok(vec![None; items.len()])
            }
        }
    }

    async fn set_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
        count: i64,
        ttl_secs: u64,
    ) -> Result<(), RepoError> {
        // Return Ok(()) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("set_count", "error");
            return Ok(());
        };

        let key = Self::count_key(content_type, content_id);

        if let Err(e) = conn.set_ex::<_, _, ()>(&key, count, ttl_secs).await {
            tracing::warn!(error = %e, key = %key, "Redis SETEX failed");
            self.observe_cache_result("set_count", "error");
        } else {
            self.observe_cache_result("set_count", "hit");
        }

        Ok(())
    }

    async fn set_batch_counts(
        &self,
        items: &[(ContentType, Uuid, i64)],
        ttl_secs: u64,
    ) -> Result<(), RepoError> {
        // If the input list is empty, return early without querying Redis
        if items.is_empty() {
            self.observe_cache_result("set_batch_counts", "hit");
            return Ok(());
        }

        // Return Ok(()) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("set_batch_counts", "error");
            return Ok(());
        };

        // Use a Redis pipeline to set multiple counts atomically and efficiently. Each count is set with the same TTL.
        let mut pipe = deadpool_redis::redis::pipe();
        pipe.atomic();

        for (content_type, content_id, count) in items {
            let key = Self::count_key(content_type.clone(), *content_id);
            pipe.cmd("SET").arg(&key).arg(*count).arg("EX").arg(ttl_secs).ignore();
        }

        // Execute the pipeline and log the result.
        if let Err(error) = pipe.query_async::<()>(&mut conn).await {
            tracing::warn!(error = %error, item_count = items.len(), "Redis pipeline for count batch failed");
            self.observe_cache_result("set_batch_counts", "error");
        } else {
            self.observe_cache_result("set_batch_counts", "hit");
        }

        Ok(())
    }

    async fn get_like_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<CachedLikeStatus>, RepoError> {
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("get_like_status", "error");
            return Ok(None);
        };

        let key = Self::like_status_key(user_id, content_type, content_id);

        match conn.get::<_, Option<String>>(&key).await {
            // If the sentinel value is found, it means the user has unliked the content. We return a specific Unliked status in this case.
            Ok(Some(value)) if value == UNLIKED_STATUS_SENTINEL => {
                self.observe_cache_result("get_like_status", "hit");
                Ok(Some(CachedLikeStatus::Unliked))
            }
            // If a timestamp string is found, we attempt to parse it. If parsing succeeds, we return a Liked status with the timestamp. If parsing fails, we log a warning and treat it as a cache miss by returning Ok(None).
            Ok(Some(value)) => match DateTime::parse_from_rfc3339(&value) {
                Ok(timestamp) => {
                    self.observe_cache_result("get_like_status", "hit");
                    Ok(Some(CachedLikeStatus::Liked(timestamp.with_timezone(&Utc))))
                }
                Err(error) => {
                    tracing::warn!(error = %error, key = %key, value = %value, "Redis cached like status payload was invalid");
                    self.observe_cache_result("get_like_status", "miss");
                    Ok(None)
                }
            },
            // If the key is not found, we treat it as a cache miss and return Ok(None).
            Ok(None) => {
                self.observe_cache_result("get_like_status", "miss");
                Ok(None)
            }
            // If there was an error communicating with Redis, we log a warning and return Ok(None) to allow the application to function without cache.
            Err(error) => {
                tracing::warn!(error = %error, key = %key, "Redis GET for cached like status failed");
                self.observe_cache_result("get_like_status", "error");
                Ok(None)
            }
        }
    }

    async fn set_like_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
        status: CachedLikeStatus,
        ttl_secs: u64,
    ) -> Result<(), RepoError> {
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("set_like_status", "error");
            return Ok(());
        };

        let key = Self::like_status_key(user_id, content_type, content_id);
        let value = match status {
            CachedLikeStatus::Liked(timestamp) => timestamp.to_rfc3339(),
            CachedLikeStatus::Unliked => UNLIKED_STATUS_SENTINEL.to_string(),
        };

        // If there was an error setting the value in Redis, we log a warning but do not return an error to allow the application to function without cache. We also record the cache operation result for metrics.
        if let Err(error) = conn.set_ex::<_, _, ()>(&key, value, ttl_secs).await {
            tracing::warn!(error = %error, key = %key, "Redis SETEX for cached like status failed");
            self.observe_cache_result("set_like_status", "error");
        } else {
            self.observe_cache_result("set_like_status", "hit");
        }

        Ok(())
    }

    async fn get_token(&self, token: &str) -> Result<Option<Uuid>, RepoError> {
        // Return Ok(None) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("get_token", "error");
            return Ok(None);
        };

        let key = Self::token_key(token);

        match conn.get::<_, Option<String>>(&key).await {
            Ok(Some(uuid_str)) => {
                // If it parses successfully, return it. Otherwise treat as miss.
                let parsed = Uuid::parse_str(&uuid_str).ok();
                self.observe_cache_result(
                    "get_token",
                    if parsed.is_some() { "hit" } else { "miss" },
                );
                Ok(parsed)
            }
            Ok(None) => {
                self.observe_cache_result("get_token", "miss");
                Ok(None)
            }
            Err(e) => {
                tracing::warn!(error = %e, "Redis GET token failed");
                self.observe_cache_result("get_token", "error");
                Ok(None)
            }
        }
    }

    async fn set_token(&self, token: &str, user_id: Uuid, ttl_secs: u64) -> Result<(), RepoError> {
        // Return Ok(()) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("set_token", "error");
            return Ok(());
        };

        let key = Self::token_key(token);

        if let Err(e) = conn.set_ex::<_, _, ()>(&key, user_id.to_string(), ttl_secs).await {
            tracing::warn!(error = %e, "Redis SETEX token failed");
            self.observe_cache_result("set_token", "error");
        } else {
            self.observe_cache_result("set_token", "hit");
        }

        Ok(())
    }

    async fn get_content_exists(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<bool>, RepoError> {
        // Return Ok(None) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("get_content_exists", "error");
            return Ok(None);
        };

        let key = Self::content_exists_key(content_type, content_id);

        match conn.exists::<_, bool>(&key).await {
            Ok(true) => {
                self.observe_cache_result("get_content_exists", "hit");
                Ok(Some(true))
            }
            Ok(false) => {
                self.observe_cache_result("get_content_exists", "miss");
                Ok(None)
            }
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis EXISTS failed");
                self.observe_cache_result("get_content_exists", "error");
                Ok(None)
            }
        }
    }

    async fn set_content_exists(
        &self,
        content_type: ContentType,
        content_id: Uuid,
        ttl_secs: u64,
    ) -> Result<(), RepoError> {
        // Return Ok(()) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("set_content_exists", "error");
            return Ok(());
        };

        let key = Self::content_exists_key(content_type, content_id);

        if let Err(e) = conn.set_ex::<_, _, ()>(&key, 1u8, ttl_secs).await {
            tracing::warn!(error = %e, key = %key, "Redis SETEX content_exists failed");
            self.observe_cache_result("set_content_exists", "error");
        } else {
            self.observe_cache_result("set_content_exists", "hit");
        }

        Ok(())
    }

    async fn check_rate_limit(
        &self,
        key: &str,
        limit: u32,
        window_secs: u64,
    ) -> Result<RateLimitStatus, RepoError> {
        // Return an allowed status if Redis is unavailable to allow the request (fail open)
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("check_rate_limit", "error");
            return Ok(RateLimitStatus { allowed: true, current_count: 0, retry_after_secs: 0 });
        };

        // Use Redis script for atomicity
        let script = Script::new(
            r#"
                local current = redis.call('INCR', KEYS[1])
                if current == 1 then
                    redis.call('EXPIRE', KEYS[1], ARGV[1])
                end
                local ttl = redis.call('TTL', KEYS[1])
                return {current, ttl}
            "#,
        );

        // Invoke the script with the key and window size as arguments. Returns the current count and TTL.
        let result: Result<(u32, u64), _> =
            script.key(key).arg(window_secs).invoke_async(&mut conn).await;

        match result {
            Ok((count, ttl)) => {
                self.observe_cache_result("check_rate_limit", "hit");
                // If TTL is negative (key missing or no expiry), fallback to window_secs
                let ttl = if ttl > 0 { ttl } else { window_secs };
                Ok(RateLimitStatus {
                    allowed: count <= limit,
                    current_count: count,
                    retry_after_secs: ttl,
                })
            }
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis rate limit script failed");
                self.observe_cache_result("check_rate_limit", "error");
                Ok(RateLimitStatus { allowed: true, current_count: 0, retry_after_secs: 0 })
            }
        }
    }

    async fn try_acquire_count_lock(
        &self,
        content_type: ContentType,
        content_id: Uuid,
        ttl_secs: u64,
    ) -> Result<bool, RepoError> {
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("try_acquire_count_lock", "error");
            return Ok(false);
        };

        let key = Self::count_lock_key(content_type, content_id);
        let result: Result<Option<String>, _> = deadpool_redis::redis::cmd("SET")
            .arg(&key)
            .arg(1)
            .arg("EX")
            .arg(ttl_secs)
            .arg("NX")
            .query_async(&mut conn)
            .await;

        match result {
            Ok(Some(_)) => {
                self.observe_cache_result("try_acquire_count_lock", "hit");
                Ok(true)
            }
            Ok(None) => {
                self.observe_cache_result("try_acquire_count_lock", "miss");
                Ok(false)
            }
            Err(error) => {
                tracing::warn!(error = %error, key = %key, "Redis SET NX for count lock failed");
                self.observe_cache_result("try_acquire_count_lock", "error");
                Ok(false)
            }
        }
    }

    async fn release_count_lock(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(), RepoError> {
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("release_count_lock", "error");
            return Ok(());
        };

        let key = Self::count_lock_key(content_type, content_id);
        if let Err(error) = conn.del::<_, ()>(&key).await {
            tracing::warn!(error = %error, key = %key, "Redis DEL for count lock failed");
            self.observe_cache_result("release_count_lock", "error");
        } else {
            self.observe_cache_result("release_count_lock", "hit");
        }

        Ok(())
    }

    async fn set_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        items: Vec<(ContentType, Uuid, i64)>,
    ) -> Result<(), RepoError> {
        // Return Ok(()) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("set_leaderboard", "error");
            return Ok(());
        };

        let key = Self::leaderboard_key(content_type, window_name);

        let mut pipe = redis::pipe();
        pipe.atomic().del(&key); // Clear existing leaderboard

        if !items.is_empty() {
            let redis_entries: Vec<(i64, String)> = items
                .into_iter()
                .map(|(ct, content_id, like_count)| {
                    (like_count, format!("{}:{}", ct.0.as_ref(), content_id))
                })
                .collect();

            pipe.zadd_multiple(&key, &redis_entries).ignore();
        }

        // TODO: replace magic number
        pipe.expire(&key, 90).ignore();

        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
            tracing::warn!(error = %e, key = %key, "Redis pipeline for set_leaderboard failed");
            self.observe_cache_result("set_leaderboard", "error");
        } else {
            self.observe_cache_result("set_leaderboard", "hit");
        }

        Ok(())
    }

    async fn get_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, RepoError> {
        // If limit is zero or negative, return empty vector immediately without querying Redis
        if limit <= 0 {
            self.observe_cache_result("get_leaderboard", "miss");
            return Ok(vec![]);
        }

        // Return Ok(None) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            self.observe_cache_result("get_leaderboard", "error");
            return Ok(vec![]);
        };

        let key = Self::leaderboard_key(content_type, window_name);

        // ZREVRANGE key 0 (limit-1) WITHSCORES
        let stop = limit - 1;
        let result: Result<Vec<(String, f64)>, _> = deadpool_redis::redis::cmd("ZREVRANGE")
            .arg(&key)
            .arg(0)
            .arg(stop)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await;

        match result {
            Ok(raw_items) => {
                // Convert from Vec<("content_type:uuid", f64)> to Vec<(ContentType, Uuid, i64)>,
                // skipping malformed entries.
                let parsed: Vec<(ContentType, Uuid, i64)> = raw_items
                    .into_iter()
                    .filter_map(|(member, score)| {
                        if !score.is_finite() {
                            return None;
                        }

                        let (ct_raw, id_raw) = member.rsplit_once(':')?;
                        let content_type = ContentType(std::sync::Arc::from(ct_raw.to_owned()));
                        let content_id = Uuid::parse_str(id_raw).ok()?;
                        let score = score.round() as i64;
                        Some((content_type, content_id, score))
                    })
                    .collect();
                self.observe_cache_result(
                    "get_leaderboard",
                    if parsed.is_empty() { "miss" } else { "hit" },
                );
                Ok(parsed)
            }
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis ZREVRANGE failed");
                self.observe_cache_result("get_leaderboard", "error");
                Ok(vec![])
            }
        }
    }
}

// NOTE: Happy path tests are covered by integration tests
#[cfg(test)]
mod tests {
    use deadpool_redis::{Config, Runtime};
    use std::sync::Arc;

    use super::*;

    fn test_metrics() -> Arc<AppMetrics> {
        Arc::new(AppMetrics::new())
    }

    // TODO: re-use
    fn content_type(name: &str) -> ContentType {
        ContentType(Arc::from(name.to_string()))
    }

    // Helper to create a RedisCacheRepository with a broken pool for testing graceful degradation
    fn broken_redis_pool() -> Pool {
        let config = Config::from_url("redis://invalid:6379");
        config.create_pool(Some(Runtime::Tokio1)).unwrap()
    }

    #[test]
    fn test_leaderboard_key_for_specific_content_type() {
        let key = RedisCacheRepository::leaderboard_key(Some(content_type("post")), "24h");

        assert_eq!(key, "leaderboard:post:24h");
    }

    #[test]
    fn test_leaderboard_key_for_all_content_types() {
        let key = RedisCacheRepository::leaderboard_key(None, "all_time");

        assert_eq!(key, "leaderboard:all:all_time");
    }

    #[tokio::test]
    async fn test_get_count_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        // This should NOT return a RepoError. It should return Ok(None) so the service can fallback to Postgres.
        let result = cache.get_count(content_type("post"), Uuid::new_v4()).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_set_count_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        // This should NOT return a RepoError. It should return Ok(()) because the DB write already succeeded anyway.
        let result = cache.set_count(content_type("post"), Uuid::new_v4(), 42, 300).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_batch_get_counts_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());
        let items = vec![
            (content_type("post"), Uuid::new_v4()),
            (content_type("bonus_hunter"), Uuid::new_v4()),
        ];

        let result = cache.batch_get_counts(&items).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![None, None]);
    }

    #[tokio::test]
    async fn test_set_batch_counts_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());
        let items = vec![
            (content_type("post"), Uuid::new_v4(), 42),
            (content_type("bonus_hunter"), Uuid::new_v4(), 7),
        ];

        let result = cache.set_batch_counts(&items, 300).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_like_status_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result =
            cache.get_like_status(Uuid::new_v4(), content_type("post"), Uuid::new_v4()).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_set_like_status_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache
            .set_like_status(
                Uuid::new_v4(),
                content_type("post"),
                Uuid::new_v4(),
                CachedLikeStatus::Unliked,
                5,
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_try_acquire_count_lock_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.try_acquire_count_lock(content_type("post"), Uuid::new_v4(), 1).await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_release_count_lock_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.release_count_lock(content_type("post"), Uuid::new_v4()).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_token_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        // This should NOT return a RepoError. It should return Ok(None) so the service can fallback to Postgres.
        let result = cache.get_token("invalid_token").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_set_token_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        // This should NOT return a RepoError. It should return Ok(()) because the DB write already succeeded anyway.
        let result = cache.set_token("some_token", Uuid::new_v4(), 300).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_content_exists_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.get_content_exists(content_type("post"), Uuid::new_v4()).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_set_content_exists_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.set_content_exists(content_type("post"), Uuid::new_v4(), 3600).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_rate_limit_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.check_rate_limit("test_key", 5, 60).await;

        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            RateLimitStatus { allowed: true, current_count: 0, retry_after_secs: 0 }
        );
    }

    #[tokio::test]
    async fn test_set_leaderboard_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());
        let items = vec![
            (content_type("post"), Uuid::new_v4(), 10),
            (content_type("post"), Uuid::new_v4(), 5),
        ];

        let result = cache.set_leaderboard(Some(content_type("post")), "24h", items).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_empty_leaderboard_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.set_leaderboard(None, "all_time", Vec::new()).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_leaderboard_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.get_leaderboard(Some(content_type("post")), "24h", 10).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::new());
    }

    #[tokio::test]
    async fn test_get_leaderboard_with_zero_limit() {
        let cache = RedisCacheRepository::new(broken_redis_pool(), test_metrics());

        let result = cache.get_leaderboard(Some(content_type("post")), "24h", 0).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::new());
    }
}
