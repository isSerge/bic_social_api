mod error;
mod like;

pub use error::DomainError;
pub use like::{LikeEvent, LikeEventKind, LikeRecord, PaginationCursor};
use serde::Serialize;

use std::sync::Arc;

/// Domain-specific types and logic for the Social API application.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type, Serialize)]
#[sqlx(transparent, no_pg_array)]
pub struct ContentType(pub Arc<str>);
