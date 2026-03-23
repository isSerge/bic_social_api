//! Observability primitives for the HTTP layer, split into metrics and readiness concerns.

mod metrics;
mod readiness;

pub use metrics::AppMetrics;
pub use readiness::{ReadinessProbe, ReadinessReport, RealReadinessProbe};

#[cfg(test)]
pub use readiness::StaticReadinessProbe;
