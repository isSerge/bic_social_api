use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::config::AppConfig;

pub mod cache_repo;
pub mod error;
pub mod like_repo;

/// Initializes the Writer and Reader PostgreSQL connection pools.
/// Runs any pending database migrations against the Writer pool before returning.
pub async fn setup_database_pools(config: &AppConfig) -> Result<(PgPool, PgPool), sqlx::Error> {
    let max_connections = config.db_max_connections;
    let min_connections = config.db_min_connections;
    let acquire_timeout = Duration::from_secs(config.db_acquire_timeout_secs);

    // 1. Configure the shared pool options
    let pool_options = PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(acquire_timeout);

    tracing::info!("Connecting to Writer Database...");
    let writer_pool = pool_options.clone().connect(&config.database_url).await?;

    tracing::info!("Connecting to Reader Database...");
    let reader_pool = pool_options.connect(&config.read_database_url).await?;

    // 2. Automatically run migrations on the Writer pool
    tracing::info!("Running pending database migrations...");
    sqlx::migrate!("./migrations").run(&writer_pool).await?;

    tracing::info!("Database pools initialized and migrated successfully.");

    Ok((writer_pool, reader_pool))
}
