use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::config::AppConfig;

pub mod cache_repo;
pub mod error;
pub mod like_repo;

/// Initializes the Writer and Reader PostgreSQL connection pools.
/// Runs any pending database migrations against the Writer pool before returning.
pub async fn setup_database_pools(config: &AppConfig) -> Result<(PgPool, PgPool), sqlx::Error> {
    let max_connections = config.database.max_connections;
    let min_connections = config.database.min_connections;
    let acquire_timeout = Duration::from_secs(config.database.acquire_timeout_secs);

    // 1. Configure the shared pool options
    let pool_options = PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(acquire_timeout);

    tracing::info!("Connecting to Writer Database...");
    let writer_pool = pool_options.clone().connect(&config.database.url).await?;

    tracing::info!("Connecting to Reader Database...");
    let reader_pool = pool_options.connect(&config.database.read_url).await?;

    // 2. Automatically run migrations on the Writer pool
    tracing::info!("Running pending database migrations...");
    sqlx::migrate!("./migrations").run(&writer_pool).await?;

    // 3. Wait for the Reader (replica) to catch up with the schema
    tracing::info!("Waiting for reader replica to replicate schema...");
    let mut attempts = 0;
    loop {
        let result = sqlx::query("SELECT 1 FROM like_counts LIMIT 0").execute(&reader_pool).await;
        if result.is_ok() {
            break;
        }
        attempts += 1;
        if attempts >= 30 {
            tracing::error!("Reader replica did not replicate schema after 30 attempts");
            return Err(sqlx::Error::Protocol(
                "Reader replica schema not ready after timeout".into(),
            ));
        }
        tracing::warn!(
            "Reader replica schema not ready yet (attempt {}/30), retrying...",
            attempts
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    tracing::info!("Database pools initialized and migrated successfully.");

    Ok((writer_pool, reader_pool))
}
