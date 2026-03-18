//! Configuration management for the Social API application.

use ::config::{Config as RawConfig, ConfigError, Environment};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    // ── Required ─────────────────────────────────────────────────────────────
    pub database_url: String,
    pub read_database_url: String,
    pub redis_url: String,
    pub http_port: u16,
    pub content_api_post_url: String,
    pub content_api_bonus_hunter_url: String,
    pub content_api_top_picks_url: String,
    pub profile_api_url: String,

    // ── Optional (defaults provided below) ───────────────────────────────────
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_rust_log")]
    pub rust_log: String,
    #[serde(default = "default_db_max_connections")]
    pub db_max_connections: u32,
    #[serde(default = "default_db_min_connections")]
    pub db_min_connections: u32,
    #[serde(default = "default_db_acquire_timeout_secs")]
    pub db_acquire_timeout_secs: u64,
    #[serde(default = "default_redis_pool_size")]
    pub redis_pool_size: usize,
    #[serde(default = "default_rate_limit_write_per_minute")]
    pub rate_limit_write_per_minute: u32,
    #[serde(default = "default_rate_limit_read_per_minute")]
    pub rate_limit_read_per_minute: u32,
    #[serde(default = "default_cache_ttl_like_counts_secs")]
    pub cache_ttl_like_counts_secs: u64,
    #[serde(default = "default_cache_ttl_content_validation_secs")]
    pub cache_ttl_content_validation_secs: u64,
    #[serde(default = "default_cache_ttl_user_status_secs")]
    pub cache_ttl_user_status_secs: u64,
    #[serde(default = "default_circuit_breaker_failure_threshold")]
    pub circuit_breaker_failure_threshold: u32,
    #[serde(default = "default_circuit_breaker_recovery_timeout_secs")]
    pub circuit_breaker_recovery_timeout_secs: u64,
    #[serde(default = "default_circuit_breaker_success_threshold")]
    pub circuit_breaker_success_threshold: u32,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_sse_heartbeat_interval_secs")]
    pub sse_heartbeat_interval_secs: u64,
    #[serde(default = "default_leaderboard_refresh_interval_secs")]
    pub leaderboard_refresh_interval_secs: u64,
}

impl AppConfig {
    /// Load configuration from environment variables.
    pub fn new() -> Result<Self, ConfigError> {
        dotenvy::dotenv().ok();

        RawConfig::builder()
            // Environment variables override everything.
            // The `config` crate matches field names case-insensitively,
            // so DATABASE_URL → database_url, HTTP_PORT → http_port, etc.
            .add_source(Environment::default())
            .build()?
            .try_deserialize()
    }
}

pub fn default_log_level() -> String {
    "info".to_string()
}

pub fn default_rust_log() -> String {
    "social_api=debug".to_string()
}

pub fn default_db_max_connections() -> u32 {
    20
}

pub fn default_db_min_connections() -> u32 {
    5
}

pub fn default_db_acquire_timeout_secs() -> u64 {
    5
}

pub fn default_redis_pool_size() -> usize {
    10
}

pub fn default_rate_limit_write_per_minute() -> u32 {
    30
}

pub fn default_rate_limit_read_per_minute() -> u32 {
    1000
}

pub fn default_cache_ttl_like_counts_secs() -> u64 {
    300
}

pub fn default_cache_ttl_content_validation_secs() -> u64 {
    3600
}

pub fn default_cache_ttl_user_status_secs() -> u64 {
    60
}

pub fn default_circuit_breaker_failure_threshold() -> u32 {
    5
}

pub fn default_circuit_breaker_recovery_timeout_secs() -> u64 {
    30
}

pub fn default_circuit_breaker_success_threshold() -> u32 {
    3
}

pub fn default_shutdown_timeout_secs() -> u64 {
    30
}

pub fn default_sse_heartbeat_interval_secs() -> u64 {
    15
}

pub fn default_leaderboard_refresh_interval_secs() -> u64 {
    60
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    static SERIAL_TEST: Mutex<()> = Mutex::new(());

    #[test]
    fn test_config_defaults() {
        let _lock = SERIAL_TEST.lock().unwrap();
        unsafe {
            // Clear variables that might affect the test
            env::remove_var("DATABASE_URL");
            env::remove_var("READ_DATABASE_URL");
            env::remove_var("REDIS_URL");
            env::remove_var("HTTP_PORT");
            env::remove_var("CONTENT_API_POST_URL");
            env::remove_var("CONTENT_API_BONUS_HUNTER_URL");
            env::remove_var("CONTENT_API_TOP_PICKS_URL");
            env::remove_var("PROFILE_API_URL");
            env::remove_var("LOG_LEVEL");
            env::remove_var("RUST_LOG");
            env::remove_var("DB_MAX_CONNECTIONS");
            env::remove_var("DB_MIN_CONNECTIONS");
            env::remove_var("DB_ACQUIRE_TIMEOUT_SECS");
            env::remove_var("REDIS_POOL_SIZE");
            env::remove_var("RATE_LIMIT_WRITE_PER_MINUTE");
            env::remove_var("RATE_LIMIT_READ_PER_MINUTE");
            env::remove_var("CACHE_TTL_LIKE_COUNTS_SECS");
            env::remove_var("CACHE_TTL_CONTENT_VALIDATION_SECS");
            env::remove_var("CACHE_TTL_USER_STATUS_SECS");
            env::remove_var("CIRCUIT_BREERER_FAILURE_THRESHOLD");
            env::remove_var("CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS");
            env::remove_var("CIRCUIT_BREAKER_SUCCESS_THRESHOLD");
            env::remove_var("SHUTDOWN_TIMEOUT_SECS");
            env::remove_var("SSE_HEARTBEAT_INTERVAL_SECS");
            env::remove_var("LEADERBOARD_REFRESH_INTERVAL_SECS");

            // Set required variables
            env::set_var("DATABASE_URL", "postgres://localhost/db");
            env::set_var("READ_DATABASE_URL", "postgres://localhost/db_read");
            env::set_var("REDIS_URL", "redis://localhost");
            env::set_var("HTTP_PORT", "8080");
            env::set_var("CONTENT_API_POST_URL", "http://localhost/post");
            env::set_var("CONTENT_API_BONUS_HUNTER_URL", "http://localhost/bonus");
            env::set_var("CONTENT_API_TOP_PICKS_URL", "http://localhost/top");
            env::set_var("PROFILE_API_URL", "http://localhost/profile");
        }

        let config = AppConfig::new().expect("Failed to load config");

        assert_eq!(config.database_url, "postgres://localhost/db");
        assert_eq!(config.log_level, default_log_level());
        assert_eq!(config.db_max_connections, default_db_max_connections());
        assert_eq!(config.http_port, 8080);
    }

    #[test]
    fn test_config_overrides() {
        let _lock = SERIAL_TEST.lock().unwrap();
        unsafe {
            env::set_var("DATABASE_URL", "postgres://localhost/db");
            env::set_var("READ_DATABASE_URL", "postgres://localhost/db_read");
            env::set_var("REDIS_URL", "redis://localhost");
            env::set_var("HTTP_PORT", "9090");
            env::set_var("CONTENT_API_POST_URL", "http://localhost/post");
            env::set_var("CONTENT_API_BONUS_HUNTER_URL", "http://localhost/bonus");
            env::set_var("CONTENT_API_TOP_PICKS_URL", "http://localhost/top");
            env::set_var("PROFILE_API_URL", "http://localhost/profile");

            env::set_var("LOG_LEVEL", "debug");
            env::set_var("DB_MAX_CONNECTIONS", "50");
        }

        let config = AppConfig::new().expect("Failed to load config");

        assert_eq!(config.http_port, 9090);
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.db_max_connections, 50);
    }
}
