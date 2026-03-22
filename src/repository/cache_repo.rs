use crate::domain::ContentType;
use crate::repository::error::RepoError;
use async_trait::async_trait;
use deadpool_redis::{
    Connection, Pool,
    redis::{AsyncCommands, Script},
};
use uuid::Uuid;

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

    /// Sets the like count for a specific content item in the cache with a TTL.
    /// The TTL is used to ensure that cached counts are refreshed periodically from db.
    async fn set_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
        count: i64,
        ttl_secs: u64,
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
    /// Returns `Ok((true, 0))` if allowed.
    /// Returns `Ok((false, retry_after_secs))` if rate limited.
    async fn check_rate_limit(
        &self,
        key: &str,
        limit: u32,
        window_secs: u64,
    ) -> Result<(bool, u64), RepoError>;

    /// Sets the leaderboard data for a specific content type and time window in the cache.
    async fn set_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        items: Vec<(Uuid, i64)>,
    ) -> Result<(), RepoError>;

    /// Gets the leaderboard data for a specific content type and time window from the cache.
    /// Returns an empty vector if not cached.
    async fn get_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        limit: i64,
    ) -> Result<Vec<(Uuid, i64)>, RepoError>;
}

/// Concrete implementation of CacheRepository using Redis.
pub struct RedisCacheRepository {
    pool: Pool,
}

impl RedisCacheRepository {
    pub fn new(pool: Pool) -> Self {
        RedisCacheRepository { pool }
    }

    /// Helper function to generate Redis keys for like counts.
    fn count_key(content_type: ContentType, content_id: Uuid) -> String {
        format!("like_count:{}:{}", content_type.0, content_id)
    }

    /// Helper to consistently format the token cache key
    fn token_key(token: &str) -> String {
        format!("likes:token:{}", token)
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
            return Ok(None);
        };

        let key = Self::count_key(content_type, content_id);

        match conn.get::<_, Option<i64>>(&key).await {
            Ok(count) => Ok(count),
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis GET failed");
                Ok(None)
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
            return Ok(());
        };

        let key = Self::count_key(content_type, content_id);

        if let Err(e) = conn.set_ex::<_, _, ()>(&key, count, ttl_secs).await {
            tracing::warn!(error = %e, key = %key, "Redis SETEX failed");
        }

        Ok(())
    }

    async fn get_token(&self, token: &str) -> Result<Option<Uuid>, RepoError> {
        // Return Ok(None) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            return Ok(None);
        };

        let key = Self::token_key(token);

        match conn.get::<_, Option<String>>(&key).await {
            Ok(Some(uuid_str)) => {
                // If it parses successfully, return it. Otherwise treat as miss.
                Ok(Uuid::parse_str(&uuid_str).ok())
            }
            Ok(None) => Ok(None),
            Err(e) => {
                tracing::warn!(error = %e, "Redis GET token failed");
                Ok(None)
            }
        }
    }

    async fn set_token(&self, token: &str, user_id: Uuid, ttl_secs: u64) -> Result<(), RepoError> {
        // Return Ok(()) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            return Ok(());
        };

        let key = Self::token_key(token);

        if let Err(e) = conn.set_ex::<_, _, ()>(&key, user_id.to_string(), ttl_secs).await {
            tracing::warn!(error = %e, "Redis SETEX token failed");
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
            return Ok(None);
        };

        let key = Self::content_exists_key(content_type, content_id);

        match conn.exists::<_, bool>(&key).await {
            Ok(true) => Ok(Some(true)),
            Ok(false) => Ok(None),
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis EXISTS failed");
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
            return Ok(());
        };

        let key = Self::content_exists_key(content_type, content_id);

        if let Err(e) = conn.set_ex::<_, _, ()>(&key, 1u8, ttl_secs).await {
            tracing::warn!(error = %e, key = %key, "Redis SETEX content_exists failed");
        }

        Ok(())
    }

    async fn check_rate_limit(
        &self,
        key: &str,
        limit: u32,
        window_secs: u64,
    ) -> Result<(bool, u64), RepoError> {
        // Return Ok((true, 0)) if Redis is unavailable to allow the request (fail open)
        let Some(mut conn) = self.get_connection().await else {
            return Ok((true, 0));
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
                // If TTL is negative (key missing or no expiry), fallback to window_secs
                let ttl = if ttl > 0 { ttl } else { window_secs };

                if count > limit {
                    // Blocked: return false and the seconds remaining until reset.
                    Ok((false, ttl))
                } else {
                    // Allowed
                    Ok((true, 0))
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis rate limit script failed");
                return Ok((true, 0)); // Fail open on error
            }
        }
    }

    async fn set_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        items: Vec<(Uuid, i64)>,
    ) -> Result<(), RepoError> {
        // Return Ok(()) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
            return Ok(());
        };

        let key = Self::leaderboard_key(content_type, window_name);

        let mut pipe = redis::pipe();
        pipe.atomic().del(&key); // Clear existing leaderboard

        if !items.is_empty() {
            let redis_entries: Vec<(i64, String)> = items
                .into_iter()
                .map(|(content_id, like_count)| (like_count, content_id.to_string()))
                .collect();

            pipe.zadd_multiple(&key, &redis_entries).ignore();
        }

        // TODO: replace magic number
        pipe.expire(&key, 90).ignore();

        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
            tracing::warn!(error = %e, key = %key, "Redis pipeline for set_leaderboard failed");
        }

        Ok(())
    }

    async fn get_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        limit: i64,
    ) -> Result<Vec<(Uuid, i64)>, RepoError> {
        // If limit is zero or negative, return empty vector immediately without querying Redis
        if limit <= 0 {
            return Ok(vec![]);
        }

        // Return Ok(None) to indicate cache miss if cannot connect
        let Some(mut conn) = self.get_connection().await else {
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
                // Convert from Vec<(String, f64)> to Vec<(Uuid, i64)>, filtering out any entries with invalid UUIDs or non-finite scores.
                let parsed = raw_items
                    .into_iter()
                    .filter_map(|(id_str, score)| {
                        if !score.is_finite() {
                            return None;
                        }

                        let score = score.round() as i64;
                        Uuid::parse_str(&id_str).ok().map(|uuid| (uuid, score))
                    })
                    .collect();
                Ok(parsed)
            }
            Err(e) => {
                tracing::warn!(error = %e, key = %key, "Redis ZREVRANGE failed");
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
        let cache = RedisCacheRepository::new(broken_redis_pool());

        // This should NOT return a RepoError. It should return Ok(None) so the service can fallback to Postgres.
        let result = cache.get_count(content_type("post"), Uuid::new_v4()).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_set_count_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        // This should NOT return a RepoError. It should return Ok(()) because the DB write already succeeded anyway.
        let result = cache.set_count(content_type("post"), Uuid::new_v4(), 42, 300).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_token_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        // This should NOT return a RepoError. It should return Ok(None) so the service can fallback to Postgres.
        let result = cache.get_token("invalid_token").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_set_token_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        // This should NOT return a RepoError. It should return Ok(()) because the DB write already succeeded anyway.
        let result = cache.set_token("some_token", Uuid::new_v4(), 300).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_content_exists_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        let result = cache.get_content_exists(content_type("post"), Uuid::new_v4()).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn test_set_content_exists_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        let result = cache.set_content_exists(content_type("post"), Uuid::new_v4(), 3600).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_rate_limit_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        let result = cache.check_rate_limit("test_key", 5, 60).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (true, 0));
    }

    #[tokio::test]
    async fn test_set_leaderboard_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());
        let items = vec![(Uuid::new_v4(), 10), (Uuid::new_v4(), 5)];

        let result = cache.set_leaderboard(Some(content_type("post")), "24h", items).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_empty_leaderboard_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        let result = cache.set_leaderboard(None, "all_time", Vec::new()).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_leaderboard_degrades_gracefully_when_redis_is_down() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        let result = cache.get_leaderboard(Some(content_type("post")), "24h", 10).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::new());
    }

    #[tokio::test]
    async fn test_get_leaderboard_with_zero_limit() {
        let cache = RedisCacheRepository::new(broken_redis_pool());

        let result = cache.get_leaderboard(Some(content_type("post")), "24h", 0).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::new());
    }
}
