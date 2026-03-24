use std::borrow::Cow;
use std::time::Instant;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Registry, TextEncoder,
};

/// Bounded HTTP method labels for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethodLabel {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
    Options,
    Other,
}

impl HttpMethodLabel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Head => "HEAD",
            Self::Options => "OPTIONS",
            Self::Other => "OTHER",
        }
    }
}

/// Converts an `axum::http::Method` to a `HttpMethodLabel`.
impl From<&axum::http::Method> for HttpMethodLabel {
    fn from(method: &axum::http::Method) -> Self {
        match *method {
            axum::http::Method::GET => Self::Get,
            axum::http::Method::POST => Self::Post,
            axum::http::Method::PUT => Self::Put,
            axum::http::Method::PATCH => Self::Patch,
            axum::http::Method::DELETE => Self::Delete,
            axum::http::Method::HEAD => Self::Head,
            axum::http::Method::OPTIONS => Self::Options,
            _ => Self::Other,
        }
    }
}

/// Bounded cache operation labels for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheOperationLabel {
    GetCount,
    BatchGetCounts,
    SetCount,
    SetBatchCounts,
    GetLikeStatus,
    SetLikeStatus,
    TryAcquireCountLock,
    ReleaseCountLock,
    GetToken,
    SetToken,
    GetContentExists,
    SetContentExists,
    CheckRateLimit,
    SetLeaderboard,
    GetLeaderboard,
}

impl CacheOperationLabel {
    fn as_str(self) -> &'static str {
        match self {
            Self::GetCount => "get_count",
            Self::BatchGetCounts => "batch_get_counts",
            Self::SetCount => "set_count",
            Self::SetBatchCounts => "set_batch_counts",
            Self::GetLikeStatus => "get_like_status",
            Self::SetLikeStatus => "set_like_status",
            Self::TryAcquireCountLock => "try_acquire_count_lock",
            Self::ReleaseCountLock => "release_count_lock",
            Self::GetToken => "get_token",
            Self::SetToken => "set_token",
            Self::GetContentExists => "get_content_exists",
            Self::SetContentExists => "set_content_exists",
            Self::CheckRateLimit => "check_rate_limit",
            Self::SetLeaderboard => "set_leaderboard",
            Self::GetLeaderboard => "get_leaderboard",
        }
    }
}

/// Bounded cache outcome labels for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheResultLabel {
    Hit,
    Miss,
    Error,
}

impl CacheResultLabel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Error => "error",
        }
    }
}

/// Bounded external service labels for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalServiceLabel {
    ContentApi,
    ProfileApi,
}

impl ExternalServiceLabel {
    fn as_str(self) -> &'static str {
        match self {
            Self::ContentApi => "content_api",
            Self::ProfileApi => "profile_api",
        }
    }
}

/// Bounded external call status labels for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExternalCallStatusLabel {
    Http(reqwest::StatusCode),
    Error,
}

impl ExternalCallStatusLabel {
    fn as_label(self) -> Cow<'static, str> {
        match self {
            Self::Http(status) => Cow::Owned(status.as_str().to_string()),
            Self::Error => Cow::Borrowed("error"),
        }
    }
}

/// Bounded circuit breaker states for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitBreakerMetricState {
    Closed,
    HalfOpen,
    Open,
}

impl CircuitBreakerMetricState {
    fn gauge_value(self) -> i64 {
        match self {
            Self::Closed => 0,
            Self::HalfOpen => 1,
            Self::Open => 2,
        }
    }
}

/// Bounded like operation labels for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LikeOperationLabel {
    Like,
    Unlike,
}

impl LikeOperationLabel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Like => "like",
            Self::Unlike => "unlike",
        }
    }
}

/// Application-wide Prometheus registry and metric instruments.
#[derive(Clone)]
pub struct AppMetrics {
    registry: Registry,
    http_requests_total: IntCounterVec,
    http_request_duration_seconds: HistogramVec,
    cache_operations_total: IntCounterVec,
    external_calls_total: IntCounterVec,
    external_call_duration_seconds: HistogramVec,
    circuit_breaker_state: IntGaugeVec,
    db_pool_connections: IntGaugeVec,
    sse_connections_active: IntGaugeVec,
    likes_total: IntCounterVec,
}

impl AppMetrics {
    /// Create and register the full application metric set.
    pub fn new() -> Self {
        let registry = Registry::new();

        let http_requests_total = IntCounterVec::new(
            prometheus::Opts::new(
                "social_api_http_requests_total",
                "Total number of HTTP requests",
            ),
            &["method", "path", "status"],
        )
        .expect("social_api_http_requests_total metric definition must be valid");

        let http_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "social_api_http_request_duration_seconds",
                "HTTP request duration in seconds",
            ),
            &["method", "path"],
        )
        .expect("social_api_http_request_duration_seconds metric definition must be valid");

        let cache_operations_total = IntCounterVec::new(
            prometheus::Opts::new(
                "social_api_cache_operations_total",
                "Total number of cache operations by operation and result",
            ),
            &["operation", "result"],
        )
        .expect("social_api_cache_operations_total metric definition must be valid");

        let external_calls_total = IntCounterVec::new(
            prometheus::Opts::new(
                "social_api_external_calls_total",
                "Total number of external service calls by service, method, and status",
            ),
            &["service", "method", "status"],
        )
        .expect("social_api_external_calls_total metric definition must be valid");

        let external_call_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "social_api_external_call_duration_seconds",
                "Duration of external service calls in seconds",
            ),
            &["service", "method"],
        )
        .expect("social_api_external_call_duration_seconds metric definition must be valid");

        let circuit_breaker_state = IntGaugeVec::new(
            prometheus::Opts::new(
                "social_api_circuit_breaker_state",
                "Circuit breaker state by service (0=closed, 1=half-open, 2=open)",
            ),
            &["service"],
        )
        .expect("social_api_circuit_breaker_state metric definition must be valid");

        let db_pool_connections = IntGaugeVec::new(
            prometheus::Opts::new(
                "social_api_db_pool_connections",
                "Database pool connections by state (active, idle, max)",
            ),
            &["state"],
        )
        .expect("social_api_db_pool_connections metric definition must be valid");

        let sse_connections_active = IntGaugeVec::new(
            prometheus::Opts::new(
                "social_api_sse_connections_active",
                "Number of active SSE connections",
            ),
            &[],
        )
        .expect("social_api_sse_connections_active metric definition must be valid");

        let likes_total = IntCounterVec::new(
            prometheus::Opts::new(
                "social_api_likes_total",
                "Total like and unlike operations by content type",
            ),
            &["content_type", "operation"],
        )
        .expect("social_api_likes_total metric definition must be valid");

        registry
            .register(Box::new(http_requests_total.clone()))
            .expect("social_api_http_requests_total metric registration must succeed");
        registry
            .register(Box::new(http_request_duration_seconds.clone()))
            .expect("social_api_http_request_duration_seconds metric registration must succeed");
        registry
            .register(Box::new(cache_operations_total.clone()))
            .expect("social_api_cache_operations_total metric registration must succeed");
        registry
            .register(Box::new(external_calls_total.clone()))
            .expect("social_api_external_calls_total metric registration must succeed");
        registry
            .register(Box::new(external_call_duration_seconds.clone()))
            .expect("social_api_external_call_duration_seconds metric registration must succeed");
        registry
            .register(Box::new(circuit_breaker_state.clone()))
            .expect("social_api_circuit_breaker_state metric registration must succeed");
        registry
            .register(Box::new(db_pool_connections.clone()))
            .expect("social_api_db_pool_connections metric registration must succeed");
        registry
            .register(Box::new(sse_connections_active.clone()))
            .expect("social_api_sse_connections_active metric registration must succeed");
        registry
            .register(Box::new(likes_total.clone()))
            .expect("social_api_likes_total metric registration must succeed");

        db_pool_connections.with_label_values(&["active"]).set(0);
        db_pool_connections.with_label_values(&["idle"]).set(0);
        db_pool_connections.with_label_values(&["max"]).set(0);
        sse_connections_active.with_label_values(&[] as &[&str]).set(0);

        Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            cache_operations_total,
            external_calls_total,
            external_call_duration_seconds,
            circuit_breaker_state,
            db_pool_connections,
            sse_connections_active,
            likes_total,
        }
    }

    /// Record a completed HTTP request by method, path, status, and duration.
    pub fn observe_http_request(
        &self,
        method: HttpMethodLabel,
        path: &str,
        status: u16,
        started_at: Instant,
    ) {
        let status = status.to_string();
        self.http_requests_total.with_label_values(&[method.as_str(), path, &status]).inc();
        self.http_request_duration_seconds
            .with_label_values(&[method.as_str(), path])
            .observe(started_at.elapsed().as_secs_f64());
    }

    /// Record a cache interaction outcome such as hit, miss, or error.
    pub fn observe_cache_operation(
        &self,
        operation: CacheOperationLabel,
        result: CacheResultLabel,
    ) {
        self.cache_operations_total.with_label_values(&[operation.as_str(), result.as_str()]).inc();
    }

    /// Record an outbound dependency call by service, method, status, and duration.
    pub fn observe_external_call(
        &self,
        service: ExternalServiceLabel,
        method: HttpMethodLabel,
        status: ExternalCallStatusLabel,
        started_at: Instant,
    ) {
        let status = status.as_label();
        self.external_calls_total
            .with_label_values(&[service.as_str(), method.as_str(), status.as_ref()])
            .inc();
        self.external_call_duration_seconds
            .with_label_values(&[service.as_str(), method.as_str()])
            .observe(started_at.elapsed().as_secs_f64());
    }

    /// Update the current circuit breaker state for a service.
    pub fn set_circuit_breaker_state(&self, service: &str, state: CircuitBreakerMetricState) {
        self.circuit_breaker_state.with_label_values(&[service]).set(state.gauge_value());
    }

    /// Update database pool gauges for active, idle, and maximum connections.
    pub fn set_db_pool_connections(&self, active: i64, idle: i64, max: i64) {
        self.db_pool_connections.with_label_values(&["active"]).set(active);
        self.db_pool_connections.with_label_values(&["idle"]).set(idle);
        self.db_pool_connections.with_label_values(&["max"]).set(max);
    }

    /// Increment the gauge tracking currently open SSE connections.
    pub fn increment_sse_connections(&self) {
        self.sse_connections_active.with_label_values(&[] as &[&str]).inc();
    }

    /// Decrement the gauge tracking currently open SSE connections.
    pub fn decrement_sse_connections(&self) {
        self.sse_connections_active.with_label_values(&[] as &[&str]).dec();
    }

    /// Record a like-domain mutation by content type and operation.
    pub fn observe_like_operation(&self, content_type: &str, operation: LikeOperationLabel) {
        self.likes_total.with_label_values(&[content_type, operation.as_str()]).inc();
    }

    /// Render the registry in Prometheus text exposition format.
    pub fn render(&self) -> Result<String, prometheus::Error> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;

        String::from_utf8(buffer).map_err(|error| {
            prometheus::Error::Msg(format!("metrics output was not valid UTF-8: {error}"))
        })
    }
}
