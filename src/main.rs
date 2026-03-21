//! Main entry point for the Social API application.

mod clients;
mod config;
mod domain;
mod http;
mod repository;
pub mod server_utils;
mod service;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use deadpool_redis::Runtime;
use tokio::{net::TcpListener, signal};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    clients::{content::HttpContentClient, profile::ProfileClient},
    config::{AppConfig, ContentTypeRegistry},
    http::AppState,
    repository::{cache_repo::RedisCacheRepository, like_repo::PgLikeRepository},
    service::like_service::LikeService,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables.
    let config = Arc::new(
        AppConfig::new().expect("Configuration error: missing or invalid environment variables"),
    );

    // Initialize logging based on the loaded configuration.
    tracing_subscriber::registry()
        .with(EnvFilter::new(&config.app.log_level))
        .with(
            fmt::layer()
                .json()
                .with_writer(std::io::stdout)
                .with_file(false)
                .with_line_number(false)
                .with_thread_ids(false)
                .with_target(true),
        )
        .init();

    tracing::info!(service = "social-api", "Starting Social API with configuration: {:?}", config);

    // Init content type registry
    let content_type_registry = ContentTypeRegistry::from_env();

    // Initialize database connection pools and run migrations
    let (writer_pool, reader_pool) = repository::setup_database_pools(&config)
        .await
        .expect("Failed to initialize database pools");

    // Initialize Redis connection pool for caching
    let redis_pool = deadpool_redis::Config::from_url(&config.redis.url)
        .builder()
        .expect("Invalid Redis URL")
        .max_size(config.redis.pool_size)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("Failed to create Redis pool"); // TODO: handle Redis connection errors gracefully

    // Create Like Repository, and Like Service
    let like_repo = PgLikeRepository::new(writer_pool.clone(), reader_pool.clone()); // Cloning is cheap for PgPool
    let cache_repo = Arc::new(RedisCacheRepository::new(redis_pool));

    // Initialize HTTP client and Profile API client
    let http_client = reqwest::Client::new();
    let content_client =
        Arc::new(HttpContentClient::new(http_client.clone(), config.clients.content_url.clone()));
    let like_service = LikeService::new(
        Arc::new(like_repo),
        cache_repo.clone(),
        content_client,
        config.cache.clone(),
    );
    let profile_client =
        ProfileClient::new(http_client.clone(), config.clients.profile_url.clone());

    // Create shared application state
    let state = AppState {
        config: Arc::clone(&config),
        content_type_registry: Arc::new(content_type_registry),
        like_service: Arc::new(like_service),
        profile_client: Arc::new(profile_client),
        cache: cache_repo,
    };

    // Create the HTTP router with the application state
    let app = http::router::create_router(state);
    let port = server_utils::resolve_port(config.server.port);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!(service = "social-api", "Server listening on {}", addr);

    // Serve the application with graceful shutdown
    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

    let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);

    // TODO: flush metrics, close database connections, etc. during shutdown

    if let Err(_) = tokio::time::timeout(shutdown_timeout, server).await {
        tracing::warn!(
            service = "social-api",
            "Graceful shutdown timed out after {} seconds. Forcing exit.",
            shutdown_timeout.as_secs()
        );
    } else {
        tracing::info!(service = "social-api", "Server drained cleanly.");
    }

    // Close database connections
    tracing::info!(service = "social-api", "Closing database connections...");
    writer_pool.close().await;
    reader_pool.close().await;

    tracing::info!(service = "social-api", "Shutdown complete");

    Ok(())
}

/// Listens for standard OS termination signals (SIGINT / SIGTERM).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!(service = "social-api", "Received Ctrl+C, initiating graceful shutdown...");
        },
        _ = terminate => {
            tracing::info!(service = "social-api", "Received SIGTERM, initiating graceful shutdown...");
        },
    }
}
