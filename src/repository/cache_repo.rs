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

    // TODO: add methods for leaderboard, rate limiting, etc.
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

    // TODO: add similar helpers for other cache types (leaderboard, etc.)

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
}
