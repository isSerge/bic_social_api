pub mod error;
mod like;

pub use like::LikeRecord;

use std::sync::Arc;

/// Domain-specific types and logic for the Social API application.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(transparent, no_pg_array)]
pub struct ContentType(pub Arc<str>);
