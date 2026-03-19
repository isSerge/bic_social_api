use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::{ContentType, DomainError, LikeRecord};
use crate::repository::like_repo::LikeRepository;

/// Orchestrates authentication/content validation and like repository operations.
pub struct LikeService<R: LikeRepository> {
    repo: Arc<R>,
    // TODO: include cache, clients and broadcaster
}

impl<R: LikeRepository> LikeService<R> {
    pub fn new(repo: Arc<R>) -> Self {
        Self { repo }
    }

    /// Handles a like operation: validates the token, checks content existence, and records the like.
    /// Returns (is_new_like, updated_count, timestamp) on success.
    pub async fn like(
        &self,
        user_id: Uuid,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64, DateTime<Utc>), DomainError> {
        // TODO: use content client to validate content existence before inserting like

        let (already_existed, new_count, timestamp) =
            self.repo.insert_like(user_id, content_type, content_id).await?;

        // TODO: update cache and broadcast event to SSE subscribers

        Ok((already_existed, new_count, timestamp))
    }

    /// Handles an unlike operation: validates the token and removes the like record.
    /// Returns (was_liked, updated_count) on success.
    pub async fn unlike(
        &self,
        user_id: Uuid,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64), DomainError> {
        let (was_liked, new_count) =
            self.repo.delete_like(user_id, content_type, content_id).await?;

        // TODO: update cache and broadcast

        Ok((was_liked, new_count))
    }

    /// Retrieves the total like count for a given content item.
    pub async fn get_count(
        &self,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<i64, DomainError> {
        // TODO: try cache

        let count = self.repo.get_count(content_type, content_id).await?;

        // TODO: populate cache

        Ok(count)
    }

    /// Retrieves the like status for a specific user and content item.
    /// Returns the timestamp of when the user liked the item, or None if not liked.
    /// Does not cache user-specific like status
    pub async fn get_status(
        &self,
        user_id: Uuid,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<Option<DateTime<Utc>>, DomainError> {
        let status = self.repo.get_status(user_id, content_type, content_id).await?;

        Ok(status)
    }

    // TODO: implement pagination
    /// Retrieves a list of content items that the authenticated user has liked, optionally filtered by content type.
    pub async fn get_user_likes(
        &self,
        user_id: Uuid,
        content_type: Option<&ContentType>,
    ) -> Result<Vec<LikeRecord>, DomainError> {
        let likes = self.repo.get_user_likes(user_id, content_type).await?;

        Ok(likes)
    }

    /// Batch retrieves like counts for multiple content items in a single request.
    pub async fn batch_get_counts(
        &self,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<i64>, DomainError> {
        let counts = self.repo.batch_get_counts(items).await?;

        Ok(counts)
    }

    /// Batch retrieves like statuses for multiple content items for the authenticated user.
    pub async fn batch_get_statuses(
        &self,
        user_id: Uuid,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<DateTime<Utc>>>, DomainError> {
        let statuses = self.repo.batch_get_statuses(user_id, items).await?;

        Ok(statuses)
    }

    /// Retrieves the top liked content items, optionally filtered by content type and time range.
    pub async fn get_top_liked(
        &self,
        content_type: Option<&ContentType>,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, DomainError> {
        // TODO: try cache
        let top_liked = self.repo.get_top_liked(content_type, since, limit).await?;

        Ok(top_liked)
    }
}

// TODO: add tests for LikeService with a mock LikeRepository implementation
