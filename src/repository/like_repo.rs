use crate::domain::{ContentType, LikeRecord};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::error::RepoError;

/// Repository trait for managing likes in the application.
#[async_trait]
pub trait LikeRepository: Send + Sync {
    /// Inserts a like record into the repository.
    async fn insert_like(
        &self,
        record: &LikeRecord,
    ) -> Result<(bool, i64, DateTime<Utc>), RepoError>;

    /// Deletes a like record from the repository.
    async fn delete_like(
        &self,
        user_id: Uuid,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64), RepoError>;

    /// Retrieves the total like count for a specific content item.
    async fn get_count(
        &self,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<i64, RepoError>;

    /// Retrieves the like status for a specific user and content item.
    async fn get_status(
        &self,
        user_id: Uuid,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<Option<DateTime<Utc>>, RepoError>;

    // TODO: include cursor-based pagination for user likes retrieval
    /// Retrieves all likes for a specific user, optionally filtered by content type.
    async fn get_user_likes(
        &self,
        user_id: Uuid,
        content_type: Option<&ContentType>,
    ) -> Result<Vec<LikeRecord>, RepoError>;

    /// Retrieves like counts for multiple content items in a single batch operation.
    async fn batch_get_counts(&self, items: &[(ContentType, Uuid)]) -> Result<Vec<i64>, RepoError>;

    /// Retrieves like statuses for multiple content items for a specific user in a single batch operation.
    async fn batch_get_statuses(
        &self,
        user_id: Uuid,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<DateTime<Utc>>>, RepoError>;

    /// Retrieves the top liked content items, optionally filtered by content type and time range.
    async fn get_top_liked(
        &self,
        content_type: Option<&ContentType>,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, RepoError>;
}
