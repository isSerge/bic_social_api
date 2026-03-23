use std::time::Instant;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Registry, TextEncoder,
};

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
    pub fn observe_http_request(&self, method: &str, path: &str, status: u16, started_at: Instant) {
        let status = status.to_string();
        self.http_requests_total.with_label_values(&[method, path, &status]).inc();
        self.http_request_duration_seconds
            .with_label_values(&[method, path])
            .observe(started_at.elapsed().as_secs_f64());
    }

    /// Record a cache interaction outcome such as hit, miss, or error.
    pub fn observe_cache_operation(&self, operation: &str, result: &str) {
        self.cache_operations_total.with_label_values(&[operation, result]).inc();
    }

    /// Record an outbound dependency call by service, method, status, and duration.
    pub fn observe_external_call(
        &self,
        service: &str,
        method: &str,
        status: &str,
        started_at: Instant,
    ) {
        self.external_calls_total.with_label_values(&[service, method, status]).inc();
        self.external_call_duration_seconds
            .with_label_values(&[service, method])
            .observe(started_at.elapsed().as_secs_f64());
    }

    /// Update the current circuit breaker state for a service.
    pub fn set_circuit_breaker_state(&self, service: &str, state: i64) {
        self.circuit_breaker_state.with_label_values(&[service]).set(state);
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
    pub fn observe_like(&self, content_type: &str, operation: &str) {
        self.likes_total.with_label_values(&[content_type, operation]).inc();
    }

    /// Render the registry in Prometheus text exposition format.
    pub fn render(&self) -> Result<String, prometheus::Error> {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;

        String::from_utf8(buffer).map_err(|error| {
            prometheus::Error::Msg(format!("metrics output was not valid UTF-8: {}", error))
        })
    }
}
