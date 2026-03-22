use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{ContentType, DomainError};

/// Persistent Like record stored in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeRecord {
    pub user_id: Uuid,
    pub content_type: ContentType,
    pub content_id: Uuid,
    pub created_at: DateTime<Utc>,
}

/// Event kind emitted to SSE subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LikeEventKind {
    Like,
    Unlike,
    Shutdown,
}

/// Event payload broadcast over SSE after like/unlike operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeEvent {
    pub event: LikeEventKind,
    pub user_id: Uuid,
    pub content_type: ContentType,
    pub content_id: Uuid,
    pub count: i64,
    pub timestamp: DateTime<Utc>,
}

/// Cursor used for pagination in like-related queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationCursor {
    /// The timestamp of the last like/unlike event in the current page.
    pub created_at: DateTime<Utc>,
    /// The unique identifier of the last like/unlike event in the current page.
    pub id: Uuid,
}

/// Internal struct for safe cursor encoding/decoding
#[derive(Serialize, Deserialize)]
struct CursorPayload {
    t: DateTime<Utc>,
    id: Uuid,
}

/// Converts a PaginationCursor into a URL-safe string for use in HTTP query parameters.
impl From<&PaginationCursor> for String {
    fn from(cursor: &PaginationCursor) -> Self {
        let payload = CursorPayload { t: cursor.created_at, id: cursor.id };
        let json = serde_json::to_string(&payload).expect("CursorPayload should always serialize");
        URL_SAFE_NO_PAD.encode(json)
    }
}

/// Converts a URL-safe string back into a PaginationCursor.
impl TryFrom<&str> for PaginationCursor {
    type Error = DomainError;

    fn try_from(encoded: &str) -> Result<Self, Self::Error> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| DomainError::InvalidCursor(encoded.to_string()))?;

        let payload: CursorPayload = serde_json::from_slice(&bytes)
            .map_err(|_| DomainError::InvalidCursor(encoded.to_string()))?;

        Ok(PaginationCursor { created_at: payload.t, id: payload.id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_serialization() {
        let cursor = PaginationCursor { created_at: Utc::now(), id: Uuid::new_v4() };

        let encoded: String = (&cursor).into();
        let decoded_json = URL_SAFE_NO_PAD.decode(encoded).expect("Should decode successfully");
        let decoded_payload: CursorPayload =
            serde_json::from_slice(&decoded_json).expect("Should deserialize successfully");

        assert_eq!(decoded_payload.t, cursor.created_at);
        assert_eq!(decoded_payload.id, cursor.id);
    }

    #[test]
    fn test_cursor_deserialization() {
        let cursor = PaginationCursor { created_at: Utc::now(), id: Uuid::new_v4() };
        let encoded: String = (&cursor).into();
        let decoded_cursor =
            PaginationCursor::try_from(encoded.as_str()).expect("Should deserialize successfully");

        assert_eq!(decoded_cursor.created_at, cursor.created_at);
        assert_eq!(decoded_cursor.id, cursor.id);
    }

    #[test]
    fn test_invalid_cursor() {
        let invalid_str = "not_a_valid_cursor";
        let result = PaginationCursor::try_from(invalid_str);
        assert!(matches!(result, Err(DomainError::InvalidCursor(_))));
    }
}
