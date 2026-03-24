//! Configuration management for the Social API application.

use ::config::{Config as RawConfig, ConfigError, Environment};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub database: DatabaseConfig,
    #[serde(flatten)]
    pub redis: RedisConfig,
    #[serde(flatten)]
    pub server: ServerConfig,
    #[serde(flatten)]
    pub clients: ClientsConfig,
    #[serde(flatten)]
    pub cache: CacheConfig,
    #[serde(flatten)]
    pub limits: LimitsConfig,
    #[serde(flatten)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(flatten)]
    pub app: GeneralConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    #[serde(rename = "database_url")]
    pub url: String,
    #[serde(rename = "read_database_url")]
    pub read_url: String,
    #[serde(rename = "db_max_connections", default = "default_db_max_connections")]
    pub max_connections: u32,
    #[serde(rename = "db_min_connections", default = "default_db_min_connections")]
    pub min_connections: u32,
    #[serde(rename = "db_acquire_timeout_secs", default = "default_db_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    #[serde(rename = "redis_url")]
    pub url: String,
    #[serde(rename = "redis_pool_size", default = "default_redis_pool_size")]
    pub pool_size: usize,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(rename = "http_port")]
    pub port: u16,
    #[serde(rename = "shutdown_timeout_secs", default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(rename = "sse_channel_capacity", default = "default_sse_channel_capacity")]
    pub sse_channel_capacity: usize,
}

#[derive(Debug, Deserialize)]
pub struct ClientsConfig {
    #[serde(rename = "profile_api_url")]
    pub profile_url: String,
    #[serde(rename = "http_timeout_secs", default = "default_http_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(rename = "http_connect_timeout_secs", default = "default_http_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(
        rename = "http_pool_idle_timeout_secs",
        default = "default_http_pool_idle_timeout_secs"
    )]
    pub pool_idle_timeout_secs: u64,
    #[serde(rename = "http_max_retries", default = "default_http_max_retries")]
    pub max_retries: u32,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct CacheConfig {
    #[serde(rename = "cache_ttl_like_counts_secs", default = "default_cache_ttl_like_counts_secs")]
    pub like_counts_ttl_secs: u64,
    #[serde(
        rename = "cache_ttl_content_validation_secs",
        default = "default_cache_ttl_content_validation_secs"
    )]
    pub content_validation_ttl_secs: u64,
    #[serde(rename = "cache_ttl_user_status_secs", default = "default_cache_ttl_user_status_secs")]
    pub user_status_ttl_secs: u64,
    #[serde(rename = "cache_ttl_leaderboard_secs", default = "default_cache_ttl_leaderboard_secs")]
    pub leaderboard_ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            like_counts_ttl_secs: default_cache_ttl_like_counts_secs(),
            content_validation_ttl_secs: default_cache_ttl_content_validation_secs(),
            user_status_ttl_secs: default_cache_ttl_user_status_secs(),
            leaderboard_ttl_secs: default_cache_ttl_leaderboard_secs(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LimitsConfig {
    #[serde(
        rename = "rate_limit_write_per_minute",
        default = "default_rate_limit_write_per_minute"
    )]
    pub write_per_minute: u32,
    #[serde(rename = "rate_limit_read_per_minute", default = "default_rate_limit_read_per_minute")]
    pub read_per_minute: u32,
    #[serde(rename = "rate_limit_window_secs", default = "default_rate_limit_window_secs")]
    pub rate_limit_window_secs: u64,
    #[serde(default = "default_max_batch_pairs")]
    pub max_batch_pairs: usize,
    #[serde(default = "default_max_top_liked_limit")]
    pub max_top_liked_limit: usize,
    #[serde(default = "default_user_likes_default_page_size")]
    pub user_likes_default_page_size: usize,
    #[serde(default = "default_user_likes_max_page_size")]
    pub user_likes_max_page_size: usize,
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct CircuitBreakerConfig {
    #[serde(
        rename = "circuit_breaker_failure_threshold",
        default = "default_circuit_breaker_failure_threshold"
    )]
    pub failure_threshold: u32,
    #[serde(
        rename = "circuit_breaker_recovery_timeout_secs",
        default = "default_circuit_breaker_recovery_timeout_secs"
    )]
    pub recovery_timeout_secs: u64,
    #[serde(
        rename = "circuit_breaker_success_threshold",
        default = "default_circuit_breaker_success_threshold"
    )]
    pub success_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_circuit_breaker_failure_threshold(),
            recovery_timeout_secs: default_circuit_breaker_recovery_timeout_secs(),
            success_threshold: default_circuit_breaker_success_threshold(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct GeneralConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(
        rename = "sse_heartbeat_interval_secs",
        default = "default_sse_heartbeat_interval_secs"
    )]
    pub heartbeat_interval_secs: u64,
    #[serde(
        rename = "leaderboard_refresh_interval_secs",
        default = "default_leaderboard_refresh_interval_secs"
    )]
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
            .add_source(Environment::default().try_parsing(true))
            .build()?
            .try_deserialize()
    }
}

// Default impl for testing purposes only, real config should always be created from env vars
#[cfg(test)]
impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                url: "postgres://localhost:5432/social_api".to_string(),
                read_url: "postgres://localhost:5432/social_api".to_string(),
                max_connections: default_db_max_connections(),
                min_connections: default_db_min_connections(),
                acquire_timeout_secs: default_db_acquire_timeout_secs(),
            },
            redis: RedisConfig {
                url: "redis://localhost:6379".to_string(),
                pool_size: default_redis_pool_size(),
            },
            server: ServerConfig {
                port: 8080,
                shutdown_timeout_secs: default_shutdown_timeout_secs(),
                sse_channel_capacity: default_sse_channel_capacity(),
            },
            clients: ClientsConfig {
                profile_url: "http://localhost:8082".to_string(),
                timeout_secs: default_http_timeout_secs(),
                connect_timeout_secs: default_http_connect_timeout_secs(),
                pool_idle_timeout_secs: default_http_pool_idle_timeout_secs(),
                max_retries: default_http_max_retries(),
            },
            cache: CacheConfig {
                like_counts_ttl_secs: default_cache_ttl_like_counts_secs(),
                content_validation_ttl_secs: default_cache_ttl_content_validation_secs(),
                user_status_ttl_secs: default_cache_ttl_user_status_secs(),
                leaderboard_ttl_secs: default_cache_ttl_leaderboard_secs(),
            },
            limits: LimitsConfig {
                write_per_minute: default_rate_limit_write_per_minute(),
                read_per_minute: default_rate_limit_read_per_minute(),
                rate_limit_window_secs: default_rate_limit_window_secs(),
                max_batch_pairs: default_max_batch_pairs(),
                max_top_liked_limit: default_max_top_liked_limit(),
                user_likes_default_page_size: default_user_likes_default_page_size(),
                user_likes_max_page_size: default_user_likes_max_page_size(),
            },
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: default_circuit_breaker_failure_threshold(),
                recovery_timeout_secs: default_circuit_breaker_recovery_timeout_secs(),
                success_threshold: default_circuit_breaker_success_threshold(),
            },
            app: GeneralConfig {
                log_level: default_log_level(),
                heartbeat_interval_secs: default_sse_heartbeat_interval_secs(),
                leaderboard_refresh_interval_secs: default_leaderboard_refresh_interval_secs(),
            },
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_db_max_connections() -> u32 {
    20
}

fn default_db_min_connections() -> u32 {
    5
}

fn default_db_acquire_timeout_secs() -> u64 {
    5
}

fn default_redis_pool_size() -> usize {
    10
}

fn default_http_timeout_secs() -> u64 {
    3
}

fn default_http_connect_timeout_secs() -> u64 {
    1
}

fn default_http_pool_idle_timeout_secs() -> u64 {
    90
}

fn default_http_max_retries() -> u32 {
    3
}

fn default_rate_limit_write_per_minute() -> u32 {
    30
}

fn default_rate_limit_read_per_minute() -> u32 {
    1000
}

fn default_rate_limit_window_secs() -> u64 {
    60
}

fn default_cache_ttl_like_counts_secs() -> u64 {
    300
}

fn default_cache_ttl_content_validation_secs() -> u64 {
    3600
}

fn default_cache_ttl_user_status_secs() -> u64 {
    60
}

fn default_cache_ttl_leaderboard_secs() -> u64 {
    90
}

fn default_circuit_breaker_failure_threshold() -> u32 {
    5
}

fn default_circuit_breaker_recovery_timeout_secs() -> u64 {
    30
}

fn default_circuit_breaker_success_threshold() -> u32 {
    3
}

fn default_shutdown_timeout_secs() -> u64 {
    30
}

fn default_sse_heartbeat_interval_secs() -> u64 {
    15
}

fn default_leaderboard_refresh_interval_secs() -> u64 {
    60
}

fn default_max_batch_pairs() -> usize {
    100
}

fn default_max_top_liked_limit() -> usize {
    50
}

fn default_user_likes_default_page_size() -> usize {
    20
}

fn default_user_likes_max_page_size() -> usize {
    100
}

fn default_sse_channel_capacity() -> usize {
    16
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
            env::remove_var("CONTENT_API_URL");
            env::remove_var("PROFILE_API_URL");
            env::remove_var("HTTP_TIMEOUT_SECS");
            env::remove_var("HTTP_CONNECT_TIMEOUT_SECS");
            env::remove_var("HTTP_POOL_IDLE_TIMEOUT_SECS");
            env::remove_var("HTTP_MAX_RETRIES");
            env::remove_var("LOG_LEVEL");
            env::remove_var("RUST_LOG");
            env::remove_var("DB_MAX_CONNECTIONS");
            env::remove_var("DB_MIN_CONNECTIONS");
            env::remove_var("DB_ACQUIRE_TIMEOUT_SECS");
            env::remove_var("REDIS_POOL_SIZE");
            env::remove_var("RATE_LIMIT_WRITE_PER_MINUTE");
            env::remove_var("RATE_LIMIT_READ_PER_MINUTE");
            env::remove_var("RATE_LIMIT_WINDOW_SECS");
            env::remove_var("CACHE_TTL_LIKE_COUNTS_SECS");
            env::remove_var("CACHE_TTL_CONTENT_VALIDATION_SECS");
            env::remove_var("CACHE_TTL_USER_STATUS_SECS");
            env::remove_var("CACHE_TTL_LEADERBOARD_SECS");
            env::remove_var("CIRCUIT_BREAKER_FAILURE_THRESHOLD");
            env::remove_var("CIRCUIT_BREAKER_RECOVERY_TIMEOUT_SECS");
            env::remove_var("CIRCUIT_BREAKER_SUCCESS_THRESHOLD");
            env::remove_var("SHUTDOWN_TIMEOUT_SECS");
            env::remove_var("SSE_HEARTBEAT_INTERVAL_SECS");
            env::remove_var("LEADERBOARD_REFRESH_INTERVAL_SECS");
            env::remove_var("MAX_BATCH_PAIRS");
            env::remove_var("MAX_TOP_LIKED_LIMIT");
            env::remove_var("USER_LIKES_DEFAULT_PAGE_SIZE");
            env::remove_var("USER_LIKES_MAX_PAGE_SIZE");

            // Set required variables
            env::set_var("DATABASE_URL", "postgres://localhost/db");
            env::set_var("READ_DATABASE_URL", "postgres://localhost/db_read");
            env::set_var("REDIS_URL", "redis://localhost");
            env::set_var("HTTP_PORT", "8080");
            env::set_var("CONTENT_API_URL", "http://localhost/content");
            env::set_var("PROFILE_API_URL", "http://localhost/profile");
        }

        let config = AppConfig::new().expect("Failed to load config");

        assert_eq!(config.database.url, "postgres://localhost/db");
        assert_eq!(config.app.log_level, default_log_level());
        assert_eq!(config.database.max_connections, default_db_max_connections());
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.clients.timeout_secs, default_http_timeout_secs());
        assert_eq!(config.clients.connect_timeout_secs, default_http_connect_timeout_secs());
        assert_eq!(config.clients.pool_idle_timeout_secs, default_http_pool_idle_timeout_secs());
        assert_eq!(config.clients.max_retries, default_http_max_retries());
        assert_eq!(config.limits.rate_limit_window_secs, default_rate_limit_window_secs());
        assert_eq!(
            config.limits.user_likes_default_page_size,
            default_user_likes_default_page_size()
        );
        assert_eq!(config.limits.user_likes_max_page_size, default_user_likes_max_page_size());
        assert_eq!(config.cache.leaderboard_ttl_secs, default_cache_ttl_leaderboard_secs());
    }

    #[test]
    fn test_config_overrides() {
        let _lock = SERIAL_TEST.lock().unwrap();
        unsafe {
            env::set_var("DATABASE_URL", "postgres://localhost/db");
            env::set_var("READ_DATABASE_URL", "postgres://localhost/db_read");
            env::set_var("REDIS_URL", "redis://localhost");
            env::set_var("HTTP_PORT", "9090");
            env::set_var("CONTENT_API_URL", "http://localhost/content");
            env::set_var("PROFILE_API_URL", "http://localhost/profile");
            env::set_var("HTTP_TIMEOUT_SECS", "7");
            env::set_var("HTTP_CONNECT_TIMEOUT_SECS", "2");
            env::set_var("HTTP_POOL_IDLE_TIMEOUT_SECS", "45");
            env::set_var("HTTP_MAX_RETRIES", "5");

            env::set_var("LOG_LEVEL", "debug");
            env::set_var("DB_MAX_CONNECTIONS", "50");
        }

        let config = AppConfig::new().expect("Failed to load config");

        assert_eq!(config.server.port, 9090);
        assert_eq!(config.app.log_level, "debug");
        assert_eq!(config.database.max_connections, 50);
        assert_eq!(config.clients.timeout_secs, 7);
        assert_eq!(config.clients.connect_timeout_secs, 2);
        assert_eq!(config.clients.pool_idle_timeout_secs, 45);
        assert_eq!(config.clients.max_retries, 5);
    }
}
