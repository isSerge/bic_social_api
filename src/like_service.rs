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
        content_type: ContentType,
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
        content_type: ContentType,
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
        content_type: ContentType,
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
        content_type: ContentType,
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
        content_type: Option<ContentType>,
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
        content_type: Option<ContentType>,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, DomainError> {
        // TODO: try cache
        let top_liked = self.repo.get_top_liked(content_type, since, limit).await?;

        Ok(top_liked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::error::RepoError;
    use crate::repository::like_repo::MockLikeRepository;
    use chrono::TimeZone;
    use mockall::predicate::eq;
    use sqlx::Error as SqlxError;
    use std::sync::Arc;

    // Helper to create a dummy ContentType
    fn content_type(raw: &str) -> ContentType {
        ContentType(Arc::from(raw.to_string()))
    }

    #[tokio::test]
    async fn test_like_success() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = content_type("post");
        let content_id = Uuid::new_v4();
        let expected_time = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();

        let mut mock_repo = MockLikeRepository::new();

        mock_repo
            .expect_insert_like()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
            )
            .times(1)
            .returning(move |_, _, _| Ok((false, 42, expected_time))); // Returns: (already_existed, count, timestamp)

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.like(user_id, ct, content_id).await;

        // Assert
        assert!(result.is_ok());
        let (already_existed, count, timestamp) = result.unwrap();
        assert_eq!(already_existed, false);
        assert_eq!(count, 42);
        assert_eq!(timestamp, expected_time);
    }

    #[tokio::test]
    async fn test_like_repo_error_bubbles_up() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = content_type("post");
        let content_id = Uuid::new_v4();

        let mut mock_repo = MockLikeRepository::new();

        // Force the repo to return a Database error
        mock_repo
            .expect_insert_like()
            .times(1)
            .returning(|_, _, _| Err(RepoError::Db(SqlxError::RowNotFound)));

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.like(user_id, ct, content_id).await;

        // Assert
        assert!(result.is_err());
        assert!(matches!(result.err().unwrap(), DomainError::Repository(RepoError::Db(SqlxError::RowNotFound))));
    }

    #[tokio::test]
    async fn test_unlike_success() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct = content_type("post");
        let content_id = Uuid::new_v4();

        let mut mock_repo = MockLikeRepository::new();

        mock_repo
            .expect_delete_like()
            .with(
                eq(user_id),
                eq(ct.clone()),
                eq(content_id),
            )
            .times(1)
            .returning(|_, _, _| Ok((true, 41))); // Returns: (was_liked, new_count)

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.unlike(user_id, ct, content_id).await;

        // Assert
        assert!(result.is_ok());
        let (was_liked, count) = result.unwrap();
        assert_eq!(was_liked, true);
        assert_eq!(count, 41);
    }

    #[tokio::test]
    async fn test_get_count_success() {
        // Arrange
        let ct = content_type("bonus_hunter");
        let content_id = Uuid::new_v4();

        let mut mock_repo = MockLikeRepository::new();

        mock_repo
            .expect_get_count()
            .with(eq(ct.clone()), eq(content_id))
            .times(1)
            .returning(|_, _| Ok(99));

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.get_count(ct, content_id).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 99);
    }

    #[tokio::test]
    async fn test_get_status_liked() {
        // Arrange
        let user_id = Uuid::new_v4();
        let content_type = content_type("post");
        let content_id = Uuid::new_v4();
        let expected_time = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_status()
            .with(eq(user_id), eq(content_type.clone()), eq(content_id))
            .times(1)
            .returning(move |_, _, _| Ok(Some(expected_time)));

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.get_status(user_id, content_type, content_id).await;

        // Assert
        assert_eq!(result.unwrap(), Some(expected_time));
    }

    // TODO: add pagination tests

    #[tokio::test]
    async fn test_batch_get_counts() {
        // Arrange
        let ct1 = content_type("post");
        let ct2 = content_type("bonus_hunter");
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let items = vec![(ct1.clone(), id1), (ct2.clone(), id2)];

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_batch_get_counts()
            .with(eq(items.clone()))
            .times(1)
            .returning(|_| Ok(vec![100, 200]));

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.batch_get_counts(&items).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![100, 200]);
    }

    #[tokio::test]
    async fn test_batch_get_statuses() {
        // Arrange
        let user_id = Uuid::new_v4();
        let ct1 = content_type("post");
        let id1 = Uuid::new_v4();
        let ts = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();

        let items = vec![(ct1.clone(), id1)];

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_batch_get_statuses()
            .with(eq(user_id), eq(items.clone()))
            .times(1)
            .returning(move |_, _| Ok(vec![Some(ts)]));

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.batch_get_statuses(user_id, &items).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![Some(ts)]);
    }

    #[tokio::test]
    async fn test_get_top_liked() {
        // Arrange
        let content_type = content_type("post");
        let since = Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap();
        let limit = 10;
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let ct_clone1 = content_type.clone();
        let ct_clone2 = content_type.clone();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_top_liked()
            .with(eq(Some(content_type.clone())), eq(Some(since)), eq(limit))
            .times(1)
            .returning(move |_, _, _| Ok(vec![
                (ct_clone1.clone(), id1, 1500),
                (ct_clone2.clone(), id2, 900),
            ]));

        let service = LikeService::new(Arc::new(mock_repo));

        // Act
        let result = service.get_top_liked(Some(content_type), Some(since), limit).await;

        // Assert
        assert!(result.is_ok());
        let top = result.unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].2, 1500);
        assert_eq!(top[1].2, 900);
    }
}
