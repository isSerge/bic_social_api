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
use reqwest_middleware::ClientBuilder;
use reqwest_retry::{Jitter, RetryTransientMiddleware, policies::ExponentialBackoff};
use tokio::{net::TcpListener, signal, sync::watch, task::JoinHandle};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    clients::{content::HttpContentClient, profile::ProfileClient},
    config::{AppConfig, ContentTypeRegistry},
    http::{
        AppState,
        observability::{AppMetrics, RealReadinessProbe},
    },
    repository::{
        cache_repo::RedisCacheRepository,
        like_repo::{LikeRepository, PgLikeRepository},
    },
    service::{broadcast::Broadcaster, leaderboard::LeaderboardWorker, like_service::LikeService},
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
    let content_type_registry = Arc::new(ContentTypeRegistry::from_env());

    // Initialize database connection pools and run migrations
    let (writer_pool, reader_pool) = repository::setup_database_pools(&config)
        .await
        .expect("Failed to initialize database pools");

    // Initialize Redis connection pool for caching
    let redis_pool = deadpool_redis::Config::from_url(&config.redis.url)
        .builder()
        .map_err(|e| format!("Invalid Redis URL: {e}"))?
        .max_size(config.redis.pool_size)
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(|e| format!("Failed to create Redis pool: {e}"))?;

    // Create Like Repository, and Like Service
    let like_repo: Arc<dyn LikeRepository> =
        Arc::new(PgLikeRepository::new(writer_pool.clone(), reader_pool.clone())); // Cloning is cheap for PgPool
    let metrics = Arc::new(AppMetrics::new());
    let cache_repo = Arc::new(RedisCacheRepository::new(redis_pool.clone(), Arc::clone(&metrics)));

    // Initialize HTTP clients for external dependencies.
    let reqwest_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.clients.timeout_secs))
        .connect_timeout(Duration::from_secs(config.clients.connect_timeout_secs))
        .pool_idle_timeout(Duration::from_secs(config.clients.pool_idle_timeout_secs))
        .build()
        .expect("Failed to build HTTP client");
    let retry_policy = ExponentialBackoff::builder()
        .jitter(Jitter::Full)
        .build_with_max_retries(config.clients.max_retries);
    let http_client = ClientBuilder::new(reqwest_client.clone())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    let content_client = Arc::new(HttpContentClient::new(
        http_client.clone(),
        Arc::clone(&content_type_registry),
        config.circuit_breaker,
        Arc::clone(&metrics),
    ));

    // Broadcaster for real-time updates (SSE)
    let broadcaster = Arc::new(Broadcaster::new(config.server.sse_channel_capacity));
    let readiness = Arc::new(RealReadinessProbe::new(
        writer_pool.clone(),
        reader_pool.clone(),
        i64::from(config.database.max_connections) * 2,
        redis_pool,
        reqwest_client.clone(),
        Arc::clone(&content_type_registry),
        Arc::clone(&metrics),
    ));

    // Create LikeService with all dependencies
    let like_service = LikeService::new(
        config.cache,
        Arc::clone(&like_repo),
        cache_repo.clone(),
        content_client,
        Arc::clone(&broadcaster),
        Arc::clone(&metrics),
    );

    // Initialize Profile API client for token validation in the auth middleware
    let profile_client = ProfileClient::new(
        http_client,
        config.clients.profile_url.clone(),
        config.circuit_breaker,
        Arc::clone(&metrics),
    );

    // Create shared application state
    let state = AppState {
        config: Arc::clone(&config),
        content_type_registry,
        like_service: Arc::new(like_service),
        profile_client: Arc::new(profile_client),
        cache: cache_repo,
        broadcaster: Arc::clone(&broadcaster),
        readiness,
        metrics: Arc::clone(&metrics),
    };

    // Create the background worker
    let leaderboard_worker = LeaderboardWorker::new(
        Arc::clone(&config),
        Arc::clone(&state.content_type_registry),
        Arc::clone(&like_repo),
        Arc::clone(&state.cache),
    );

    let leaderboard_worker_handle = tokio::spawn(async move {
        leaderboard_worker.start().await;
    });

    // Create the HTTP router with the application state
    let app = http::router::create_router(state);
    let port = server_utils::resolve_port(config.server.port);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;

    tracing::info!(service = "social-api", "Server listening on {}", addr);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Serve the application with graceful shutdown
    let server = axum::serve(
        listener,
        // Use into_make_service_with_connect_info to get client IPs for rate limiting
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(wait_for_shutdown(shutdown_rx));

    let shutdown_timeout = Duration::from_secs(config.server.shutdown_timeout_secs);
    let server_handle = tokio::spawn(async move { server.await });

    shutdown_signal().await;
    initiate_shutdown(
        &shutdown_tx,
        &broadcaster,
        leaderboard_worker_handle,
        &metrics,
        server_handle,
        shutdown_timeout,
    )
    .await;

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

/// Waits for the shutdown signal to be triggered via the watch channel
async fn wait_for_shutdown(mut shutdown_rx: watch::Receiver<bool>) {
    if *shutdown_rx.borrow() {
        return;
    }

    let _ = shutdown_rx.changed().await;
}

/// Initiates the shutdown sequence
async fn initiate_shutdown(
    shutdown_tx: &watch::Sender<bool>,
    broadcaster: &Broadcaster,
    leaderboard_worker_handle: JoinHandle<()>,
    metrics: &AppMetrics,
    server_handle: JoinHandle<std::io::Result<()>>,
    shutdown_timeout: Duration,
) {
    let _ = shutdown_tx.send(true);

    stop_leaderboard_worker(leaderboard_worker_handle).await;

    tracing::info!(service = "social-api", "Shutting down SSE broadcaster...");
    broadcaster.shutdown();

    match tokio::time::timeout(shutdown_timeout, server_handle).await {
        Ok(Ok(Ok(()))) => {
            tracing::info!(service = "social-api", "Server drained cleanly.");
        }
        Ok(Ok(Err(error))) => {
            tracing::error!(service = "social-api", error = %error, "Server exited with error during shutdown");
        }
        Ok(Err(error)) => {
            tracing::error!(service = "social-api", error = %error, "Server task join failed during shutdown");
        }
        Err(_) => {
            tracing::warn!(
                service = "social-api",
                "Graceful shutdown timed out after {} seconds. Forcing exit.",
                shutdown_timeout.as_secs()
            );
        }
    }

    flush_metrics(metrics);
}

/// Helper function to stop the leaderboard worker gracefully, waiting for it to finish any in-progress work and logging the outcome.
async fn stop_leaderboard_worker(leaderboard_worker_handle: JoinHandle<()>) {
    tracing::info!(service = "social-api", "Stopping leaderboard worker...");
    leaderboard_worker_handle.abort();

    match leaderboard_worker_handle.await {
        Err(error) if error.is_cancelled() => {
            tracing::info!(service = "social-api", "Leaderboard worker stopped.");
        }
        Err(error) => {
            tracing::warn!(service = "social-api", error = %error, "Leaderboard worker ended unexpectedly during shutdown");
        }
        Ok(()) => {
            tracing::info!(service = "social-api", "Leaderboard worker finished cleanly.");
        }
    }
}

/// Helper function to flush the final metrics snapshot during shutdown, logging the size and content of the rendered metrics for observability.
fn flush_metrics(metrics: &AppMetrics) {
    match metrics.render() {
        Ok(body) => {
            tracing::info!(
                service = "social-api",
                metrics_bytes = body.len(),
                metrics_lines = body.lines().count(),
                "Rendered final Prometheus metrics snapshot"
            );
            tracing::debug!(service = "social-api", metrics_snapshot = %body, "Final Prometheus metrics snapshot");
        }
        Err(error) => {
            tracing::warn!(service = "social-api", error = %error, "Failed to render final Prometheus metrics snapshot");
        }
    }
}
