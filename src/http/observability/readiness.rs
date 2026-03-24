use std::{borrow::Cow, collections::HashSet, sync::Arc, time::Instant};

use async_trait::async_trait;
use deadpool_redis::{Pool, redis::cmd};
use reqwest::Client;
use serde::Serialize;
use sqlx::PgPool;
use tokio::task::JoinSet;

use crate::config::ContentTypeRegistry;

use super::metrics::{AppMetrics, ExternalCallStatusLabel, ExternalServiceLabel, HttpMethodLabel};

/// A single dependency failure captured during readiness probing.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DependencyFailure {
    pub dependency: Cow<'static, str>,
    pub message: Cow<'static, str>,
}

/// Aggregated readiness result for the application and its external dependencies.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ReadinessReport {
    pub failures: Vec<DependencyFailure>,
}

impl ReadinessReport {
    /// Return true when no dependency failures were recorded.
    pub fn is_ready(&self) -> bool {
        self.failures.is_empty()
    }

    /// Add a dependency failure to the readiness report.
    pub fn record_failure(
        &mut self,
        dependency: impl Into<Cow<'static, str>>,
        message: impl Into<Cow<'static, str>>,
    ) {
        self.failures
            .push(DependencyFailure { dependency: dependency.into(), message: message.into() });
    }
}

/// Readiness probe abstraction used by the health handlers.
#[async_trait]
pub trait ReadinessProbe: Send + Sync {
    async fn probe(&self) -> ReadinessReport;
}

/// Production readiness probe backed by Postgres, Redis, and content API checks.
pub struct RealReadinessProbe {
    writer_pool: PgPool,
    reader_pool: PgPool,
    max_connections_total: i64,
    redis_pool: Pool,
    http_client: Client,
    content_type_registry: Arc<ContentTypeRegistry>,
    metrics: Arc<AppMetrics>,
}

/// Result of probing the content API.
enum ContentApiProbeResult {
    Healthy,
    Unhealthy(String),
}

impl RealReadinessProbe {
    /// Build a readiness probe with the real application dependencies.
    pub fn new(
        writer_pool: PgPool,
        reader_pool: PgPool,
        max_connections_total: i64,
        redis_pool: Pool,
        http_client: Client,
        content_type_registry: Arc<ContentTypeRegistry>,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        Self {
            writer_pool,
            reader_pool,
            max_connections_total,
            redis_pool,
            http_client,
            content_type_registry,
            metrics,
        }
    }

    /// Record a probe failure when a dependency check returns an error.
    fn record_probe_failure<E>(
        report: &mut ReadinessReport,
        dependency: &'static str,
        failure_prefix: &'static str,
        result: Result<(), E>,
    ) where
        E: std::fmt::Display,
    {
        if let Err(error) = result {
            report.record_failure(dependency, format!("{failure_prefix}: {error}"));
        }
    }

    /// Probe a Postgres database by executing a simple query. Returns an error if the connection or query fails.
    async fn probe_database(pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool).await.map(|_| ())
    }

    /// Probe Redis by sending a PING command and awaiting a response. Returns an error if the connection or command fails.
    async fn probe_redis(redis_pool: &Pool) -> Result<(), String> {
        match redis_pool.get().await {
            Ok(mut conn) => cmd("PING")
                .query_async::<String>(&mut conn)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Probe the content API by sending a GET request to the /health endpoint of each configured upstream. Records detailed success or failure information in the metrics system for observability.
    async fn probe_content_api(
        http_client: Client,
        metrics: Arc<AppMetrics>,
        base_url: String,
    ) -> ContentApiProbeResult {
        let probe_url = format!("{}/health", base_url.trim_end_matches('/'));
        let started_at = Instant::now();

        match http_client.get(&probe_url).send().await {
            Ok(response) if response.status().is_success() => {
                metrics.observe_external_call(
                    ExternalServiceLabel::ContentApi,
                    HttpMethodLabel::Get,
                    ExternalCallStatusLabel::Http(response.status()),
                    started_at,
                );
                ContentApiProbeResult::Healthy
            }
            Ok(response) => {
                metrics.observe_external_call(
                    ExternalServiceLabel::ContentApi,
                    HttpMethodLabel::Get,
                    ExternalCallStatusLabel::Http(response.status()),
                    started_at,
                );
                ContentApiProbeResult::Unhealthy(format!(
                    "{} returned {}",
                    probe_url,
                    response.status()
                ))
            }
            Err(error) => {
                metrics.observe_external_call(
                    ExternalServiceLabel::ContentApi,
                    HttpMethodLabel::Get,
                    ExternalCallStatusLabel::Error,
                    started_at,
                );
                ContentApiProbeResult::Unhealthy(format!("{probe_url} request failed: {error}"))
            }
        }
    }
}

#[async_trait]
impl ReadinessProbe for RealReadinessProbe {
    async fn probe(&self) -> ReadinessReport {
        let mut report = ReadinessReport::default();

        let total_size = self.writer_pool.size() as i64 + self.reader_pool.size() as i64;
        let total_idle = self.writer_pool.num_idle() as i64 + self.reader_pool.num_idle() as i64;
        let total_active = total_size - total_idle;
        self.metrics.set_db_pool_connections(total_active, total_idle, self.max_connections_total);

        // Probe databases and Redis in parallel to minimize total probe time, recording any failures in the report.
        let (writer_db_result, reader_db_result, redis_probe_result) = tokio::join!(
            Self::probe_database(&self.writer_pool),
            Self::probe_database(&self.reader_pool),
            Self::probe_redis(&self.redis_pool),
        );

        Self::record_probe_failure(
            &mut report,
            "writer_db",
            "writer database probe failed",
            writer_db_result,
        );

        Self::record_probe_failure(
            &mut report,
            "reader_db",
            "reader database probe failed",
            reader_db_result,
        );

        Self::record_probe_failure(&mut report, "redis", "redis probe failed", redis_probe_result);

        let upstreams: HashSet<String> =
            self.content_type_registry.upstream_urls().into_iter().collect();
        if upstreams.is_empty() {
            report.record_failure("content_api", "no content API upstreams configured");
            return report;
        }

        let mut any_content_api_healthy = false;
        let mut upstream_errors = Vec::new();

        let mut content_probe_tasks = JoinSet::new();
        for base_url in upstreams {
            content_probe_tasks.spawn(Self::probe_content_api(
                self.http_client.clone(),
                Arc::clone(&self.metrics),
                base_url,
            ));
        }

        while let Some(task_result) = content_probe_tasks.join_next().await {
            match task_result {
                Ok(ContentApiProbeResult::Healthy) => any_content_api_healthy = true,
                Ok(ContentApiProbeResult::Unhealthy(error)) => upstream_errors.push(error),
                Err(error) => {
                    upstream_errors.push(format!("content API probe task failed: {error}"));
                }
            }
        }

        if !any_content_api_healthy {
            report.record_failure("content_api", upstream_errors.join("; "));
        }

        report
    }
}

#[cfg(test)]
#[derive(Clone)]
pub struct StaticReadinessProbe {
    report: ReadinessReport,
}

#[cfg(test)]
impl StaticReadinessProbe {
    /// Create a static probe that always reports ready.
    pub fn ready() -> Self {
        Self { report: ReadinessReport::default() }
    }

    /// Create a static probe that always returns the provided report.
    pub fn with_report(report: ReadinessReport) -> Self {
        Self { report }
    }
}

#[cfg(test)]
#[async_trait]
impl ReadinessProbe for StaticReadinessProbe {
    async fn probe(&self) -> ReadinessReport {
        self.report.clone()
    }
}
