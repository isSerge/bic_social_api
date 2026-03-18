//! Main entry point for the Social API application.

mod clients;
mod config;
mod domain;
mod error;
mod http;
mod repository;
pub mod server_utils;

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::AppConfig;

fn main() {
    // Load configuration from environment variables.
    let config =
        AppConfig::new().expect("Configuration error: missing or invalid environment variables");

    // Initialize logging based on the loaded configuration.
    tracing_subscriber::registry()
        .with(EnvFilter::new(&config.log_level))
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_file(false)
                .with_line_number(false)
                .with_thread_ids(false)
                .with_target(true)
                .compact(),
        )
        .init();

    tracing::info!("Starting Social API with configuration: {:?}", config);
}
