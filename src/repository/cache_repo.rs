use crate::domain::ContentType;
use crate::repository::error::RepoError;
use async_trait::async_trait;
use deadpool_redis::{Connection, Pool, redis::AsyncCommands};
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

    // TODO: add methods for token, leaderboard, rate limiting, etc.
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

    // TODO: add similar helpers for other cache types (token, leaderboard, etc.)

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
        // If we can't get a Redis connection, we return Ok(None) to indicate cache miss, allowing the app to function without cache.
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
        // If we can't get a Redis connection, we log the error and return Ok(()), allowing the app to function without cache.
        let Some(mut conn) = self.get_connection().await else {
            return Ok(());
        };

        let key = Self::count_key(content_type, content_id);

        if let Err(e) = conn.set_ex::<_, _, ()>(&key, count, ttl_secs).await {
            tracing::warn!(error = %e, key = %key, "Redis SETEX failed");
        }

        Ok(())
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
}
