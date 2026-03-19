use thiserror::Error;

/// Errors that can occur in repository operations.
#[derive(Debug, Error)]
pub enum RepoError {
    /// Errors related to database operations.
    #[error("Database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("Cache connection error: {0}")]
    CachePool(#[from] deadpool_redis::PoolError),

    #[error("Cache operation error: {0}")]
    CacheOp(#[from] deadpool_redis::redis::RedisError),
}
