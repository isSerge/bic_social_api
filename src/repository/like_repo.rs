use crate::domain::{ContentType, LikeRecord, PaginationCursor};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

use super::error::RepoError;

/// Repository trait for managing likes in the application.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait LikeRepository: Send + Sync {
    /// Inserts a like record into the repository.
    /// Returns a tuple indicating whether the like already existed, the new total like count, and the timestamp of the like (either newly created or existing).
    async fn insert_like(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64, DateTime<Utc>), RepoError>;

    /// Deletes a like record from the repository.
    /// Returns a tuple indicating whether a like was actually deleted and the new total like count after deletion.
    async fn delete_like(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64), RepoError>;

    /// Retrieves the total like count for a specific content item.
    async fn get_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<i64, RepoError>;

    /// Retrieves the like status for a specific user and content item.
    /// Returns the timestamp of when the user liked the item, or None if the user has not liked it.
    async fn get_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<DateTime<Utc>>, RepoError>;

    /// Retrieves all likes for a specific user, optionally filtered by content type.
    /// Returns a list of LikeRecord objects representing the user's likes
    async fn get_user_likes(
        &self,
        user_id: Uuid,
        content_type: Option<ContentType>,
        cursor: Option<PaginationCursor>,
        limit: i64,
    ) -> Result<Vec<LikeRecord>, RepoError>;

    /// Retrieves like counts for multiple content items in a single batch operation.
    async fn batch_get_counts(&self, items: &[(ContentType, Uuid)]) -> Result<Vec<i64>, RepoError>;

    /// Retrieves like statuses for multiple content items for a specific user in a single batch operation.
    /// Returns a list of Option<DateTime<Utc>> where each element corresponds to the input item at the same index, containing the like timestamp if the user liked that item or None if not.
    async fn batch_get_statuses(
        &self,
        user_id: Uuid,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<DateTime<Utc>>>, RepoError>;

    /// Retrieves the top liked content items, optionally filtered by content type and time range.
    async fn get_top_liked(
        &self,
        content_type: Option<ContentType>,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, RepoError>;
}

/// PostgreSQL implementation of the LikeRepository trait.
#[derive(Clone)]
pub struct PgLikeRepository {
    writer_pool: PgPool,
    reader_pool: PgPool,
}

impl PgLikeRepository {
    pub fn new(writer_pool: PgPool, reader_pool: PgPool) -> Self {
        Self { writer_pool, reader_pool }
    }
}

/// Helper function to convert raw string from database into ContentType.
fn to_content_type(raw: String) -> ContentType {
    ContentType(Arc::from(raw))
}

#[async_trait]
impl LikeRepository for PgLikeRepository {
    #[instrument(skip(self), level = "debug", err)]
    async fn insert_like(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64, DateTime<Utc>), RepoError> {
        let mut tx = self.writer_pool.begin().await?;

        // Attempt to insert the like record. If it already exists, we will get None back.
        let inserted_created_at = sqlx::query_scalar!(
            r#"
            INSERT INTO likes (user_id, content_type, content_id)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            RETURNING created_at
            "#,
            user_id,
            content_type.0.as_ref(),
            content_id,
        )
        .fetch_optional(&mut *tx)
        .await?;

        // If the like was newly inserted, we need to increment the count. If it already existed, we just fetch the existing count.
        let (already_existed, created_at, count) = if let Some(created_at) = inserted_created_at {
            let count = sqlx::query_scalar!(
                r#"
                INSERT INTO like_counts (content_type, content_id, count)
                VALUES ($1, $2, 1)
                ON CONFLICT (content_type, content_id)
                DO UPDATE SET
                    count = like_counts.count + 1,
                    updated_at = NOW()
                RETURNING count
                "#,
                content_type.0.as_ref(),
                content_id,
            )
            .fetch_one(&mut *tx)
            .await?;

            (false, created_at, count)
        } else {
            let created_at = sqlx::query_scalar!(
                r#"
                SELECT created_at
                FROM likes
                WHERE user_id = $1 AND content_type = $2 AND content_id = $3
                "#,
                user_id,
                content_type.0.as_ref(),
                content_id,
            )
            .fetch_one(&mut *tx)
            .await?;

            let count = sqlx::query_scalar!(
                r#"
                SELECT count
                FROM like_counts
                WHERE content_type = $1 AND content_id = $2
                "#,
                content_type.0.as_ref(),
                content_id,
            )
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0);

            (true, created_at, count)
        };

        tx.commit().await?;
        Ok((already_existed, count, created_at))
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn delete_like(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(bool, i64), RepoError> {
        let mut tx = self.writer_pool.begin().await?;

        // Attempt to delete the like record. If it doesn't exist, deleted_rows will be 0.
        let deleted_rows = sqlx::query!(
            r#"
            DELETE FROM likes
            WHERE user_id = $1 AND content_type = $2 AND content_id = $3
            "#,
            user_id,
            content_type.0.as_ref(),
            content_id,
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let was_liked = deleted_rows > 0;

        // If a like was deleted, we need to decrement the count. If there was no like to delete, we just fetch the existing count.
        let count = if was_liked {
            sqlx::query_scalar!(
                r#"
                UPDATE like_counts
                SET count = GREATEST(count - 1, 0),
                    updated_at = NOW()
                WHERE content_type = $1 AND content_id = $2
                RETURNING count
                "#,
                content_type.0.as_ref(),
                content_id,
            )
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0)
        } else {
            sqlx::query_scalar!(
                r#"
                SELECT count
                FROM like_counts
                WHERE content_type = $1 AND content_id = $2
                "#,
                content_type.0.as_ref(),
                content_id,
            )
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0)
        };

        tx.commit().await?;
        Ok((was_liked, count))
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn get_count(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<i64, RepoError> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT count
            FROM like_counts
            WHERE content_type = $1 AND content_id = $2
            "#,
            content_type.0.as_ref(),
            content_id,
        )
        .fetch_optional(&self.reader_pool)
        .await?
        .unwrap_or(0);

        Ok(count)
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn get_status(
        &self,
        user_id: Uuid,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<Option<DateTime<Utc>>, RepoError> {
        let created_at = sqlx::query_scalar!(
            r#"
            SELECT created_at
            FROM likes
            WHERE user_id = $1 AND content_type = $2 AND content_id = $3
            "#,
            user_id,
            content_type.0.as_ref(),
            content_id,
        )
        .fetch_optional(&self.reader_pool)
        .await?;

        Ok(created_at)
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn get_user_likes(
        &self,
        user_id: Uuid,
        content_type: Option<ContentType>,
        cursor: Option<PaginationCursor>,
        limit: i64,
    ) -> Result<Vec<LikeRecord>, RepoError> {
        let content_type_filter: Option<&str> = content_type.as_ref().map(|ct| ct.0.as_ref());

        // Include cursor values in the query parameters if provided, otherwise pass NULL to ignore the cursor condition
        let (cursor_created_at, cursor_id) =
            if let Some(c) = cursor { (Some(c.created_at), Some(c.id)) } else { (None, None) };

        let rows = sqlx::query!(
            r#"
            SELECT user_id, content_type, content_id, created_at
            FROM likes
            WHERE user_id = $1
              AND ($2::text IS NULL OR content_type = $2)
              AND ($3::timestamptz IS NULL OR (created_at, content_id) < ($3, $4))
            ORDER BY created_at DESC, content_id DESC
            LIMIT $5
            "#,
            user_id,
            content_type_filter,
            cursor_created_at,
            cursor_id,
            limit,
        )
        .fetch_all(&self.reader_pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| LikeRecord {
                user_id: row.user_id,
                content_type: to_content_type(row.content_type),
                content_id: row.content_id,
                created_at: row.created_at,
            })
            .collect())
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn batch_get_counts(&self, items: &[(ContentType, Uuid)]) -> Result<Vec<i64>, RepoError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare the content types and IDs for the query
        let (content_types, content_ids): (Vec<String>, Vec<Uuid>) =
            items.iter().map(|(ct, id)| (ct.0.as_ref().to_string(), *id)).unzip();

        // Use UNNEST to join the input list with the like_counts table, preserving input order
        let rows = sqlx::query!(
            r#"
            SELECT COALESCE(lc.count, 0) AS "count!: i64"
            FROM UNNEST($1::text[], $2::uuid[]) WITH ORDINALITY AS u(content_type, content_id, ord)
            LEFT JOIN like_counts lc
              ON lc.content_type = u.content_type
             AND lc.content_id = u.content_id
            ORDER BY u.ord
            "#,
            &content_types,
            &content_ids,
        )
        .fetch_all(&self.reader_pool)
        .await?;

        Ok(rows.into_iter().map(|row| row.count).collect())
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn batch_get_statuses(
        &self,
        user_id: Uuid,
        items: &[(ContentType, Uuid)],
    ) -> Result<Vec<Option<DateTime<Utc>>>, RepoError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare the content types and IDs for the query
        let (content_types, content_ids): (Vec<String>, Vec<Uuid>) =
            items.iter().map(|(ct, id)| (ct.0.as_ref().to_string(), *id)).unzip();

        // Use UNNEST to join the input list with the likes table, preserving input order, and fetch the created_at timestamps for liked items
        let rows = sqlx::query!(
            r#"
            SELECT l.created_at AS "created_at?: DateTime<Utc>"
            FROM UNNEST($1::text[], $2::uuid[]) WITH ORDINALITY AS u(content_type, content_id, ord)
            LEFT JOIN likes l
              ON l.user_id = $3
             AND l.content_type = u.content_type
             AND l.content_id = u.content_id
            ORDER BY u.ord
            "#,
            &content_types,
            &content_ids,
            user_id,
        )
        .fetch_all(&self.reader_pool)
        .await?;

        Ok(rows.into_iter().map(|row| row.created_at).collect())
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn get_top_liked(
        &self,
        content_type: Option<ContentType>,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<(ContentType, Uuid, i64)>, RepoError> {
        let content_type_filter: Option<&str> = content_type.as_ref().map(|ct| ct.0.as_ref());

        // If a time window is specified, we need to query the likes table directly to filter by created_at. If no time window is specified, we can query the like_counts table which is optimized for all-time counts.
        if let Some(since_date) = since {
            // Time-windowed query
            let rows = sqlx::query!(
                r#"
                SELECT l.content_type, l.content_id, COUNT(*)::bigint AS "like_count!: i64"
                FROM likes l
                WHERE ($1::text IS NULL OR l.content_type = $1)
                  AND l.created_at >= $2
                GROUP BY l.content_type, l.content_id
                ORDER BY COUNT(*) DESC, l.content_type ASC, l.content_id ASC
                LIMIT $3
                "#,
                content_type_filter,
                since_date,
                limit,
            )
            .fetch_all(&self.reader_pool)
            .await?;

            Ok(rows
                .into_iter()
                .map(|row| (to_content_type(row.content_type), row.content_id, row.like_count))
                .collect())
        } else {
            // All-time query
            let rows = sqlx::query!(
                r#"
                SELECT content_type, content_id, count AS "like_count!: i64"
                FROM like_counts
                WHERE ($1::text IS NULL OR content_type = $1)
                ORDER BY count DESC, content_type ASC, content_id ASC
                LIMIT $2
                "#,
                content_type_filter,
                limit,
            )
            .fetch_all(&self.reader_pool)
            .await?;

            Ok(rows
                .into_iter()
                .map(|row| (to_content_type(row.content_type), row.content_id, row.like_count))
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    struct TestContext {
        repo: PgLikeRepository,
        writer_pool: PgPool,
        reader_pool: PgPool,
        admin_url: String,
        db_name: String,
    }

    fn replace_database_in_url(url: &str, db_name: &str) -> String {
        let (base, query) = match url.split_once('?') {
            Some((b, q)) => (b, Some(q)),
            None => (url, None),
        };

        let last_slash =
            base.rfind('/').expect("DATABASE_URL must include a database path segment");
        let mut rebuilt = format!("{}{db_name}", &base[..=last_slash]);

        if let Some(q) = query {
            rebuilt.push('?');
            rebuilt.push_str(q);
        }

        rebuilt
    }

    async fn setup_test_context() -> TestContext {
        const DEFAULT_TEST_DB_URL: &str =
            "postgres://social:social_password@localhost:55432/social_api";

        let base_url = env::var("TEST_DATABASE_URL")
            .or_else(|_| env::var("DATABASE_URL"))
            .unwrap_or_else(|_| DEFAULT_TEST_DB_URL.to_string());

        let admin_url = replace_database_in_url(&base_url, "postgres");
        let db_name = format!("test_like_repo_{}", Uuid::new_v4().simple());

        let admin_pool = PgPool::connect(&admin_url).await.expect("connect admin pool");
        let create_sql = format!(r#"CREATE DATABASE "{db_name}""#);
        sqlx::query(&create_sql).execute(&admin_pool).await.expect("create test database");

        let test_db_url = replace_database_in_url(&base_url, &db_name);
        let writer_pool = PgPool::connect(&test_db_url).await.expect("connect writer pool");
        let reader_pool = PgPool::connect(&test_db_url).await.expect("connect reader pool");

        let migration_sql = include_str!("../../migrations/20260319022923_init.sql");
        for statement in migration_sql.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(statement).execute(&writer_pool).await.expect("apply migration statement");
        }

        TestContext {
            repo: PgLikeRepository::new(writer_pool.clone(), reader_pool.clone()),
            writer_pool,
            reader_pool,
            admin_url,
            db_name,
        }
    }

    async fn teardown_test_context(ctx: TestContext) {
        let TestContext { writer_pool, reader_pool, admin_url, db_name, .. } = ctx;

        writer_pool.close().await;
        reader_pool.close().await;

        let admin_pool =
            PgPool::connect(&admin_url).await.expect("connect admin pool for teardown");
        let drop_sql = format!(r#"DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)"#);
        sqlx::query(&drop_sql).execute(&admin_pool).await.expect("drop test database");
    }

    #[tokio::test]
    async fn insert_like_is_idempotent() {
        let ctx = setup_test_context().await;
        let repo = ctx.repo.clone();
        let user_id = Uuid::new_v4();
        let content_type = ContentType::from("post");
        let content_id = Uuid::new_v4();

        let (already_existed_1, count_1, _) = repo
            .insert_like(user_id, content_type.clone(), content_id)
            .await
            .expect("first insert");
        let (already_existed_2, count_2, _) = repo
            .insert_like(user_id, content_type.clone(), content_id)
            .await
            .expect("second insert");

        assert!(!already_existed_1);
        assert_eq!(count_1, 1);
        assert!(already_existed_2);
        assert_eq!(count_2, 1);

        teardown_test_context(ctx).await;
    }

    #[tokio::test]
    async fn delete_like_is_idempotent() {
        let ctx = setup_test_context().await;
        let repo = ctx.repo.clone();

        let user_id = Uuid::new_v4();
        let ct = ContentType::from("bonus_hunter");
        let content_id = Uuid::new_v4();

        repo.insert_like(user_id, ct.clone(), content_id).await.expect("seed insert");

        let (was_liked_1, count_1) =
            repo.delete_like(user_id, ct.clone(), content_id).await.expect("first delete");
        let (was_liked_2, count_2) =
            repo.delete_like(user_id, ct.clone(), content_id).await.expect("second delete");

        assert!(was_liked_1);
        assert_eq!(count_1, 0);
        assert!(!was_liked_2);
        assert_eq!(count_2, 0);

        teardown_test_context(ctx).await;
    }

    #[tokio::test]
    async fn get_count_returns_zero_for_unknown_content() {
        let ctx = setup_test_context().await;
        let repo = ctx.repo.clone();

        let count = repo
            .get_count(ContentType::from("top_picks"), Uuid::new_v4())
            .await
            .expect("get count");
        assert_eq!(count, 0);

        teardown_test_context(ctx).await;
    }

    #[tokio::test]
    async fn batch_get_counts_preserves_input_order() {
        let ctx = setup_test_context().await;
        let repo = ctx.repo.clone();

        let ct = ContentType::from("post");
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let user_id = Uuid::new_v4();
        repo.insert_like(user_id, ct.clone(), id1).await.expect("seed insert");

        let counts = repo
            .batch_get_counts(&[(ct.clone(), id2), (ct.clone(), id1)])
            .await
            .expect("batch counts");

        assert_eq!(counts, vec![0, 1]);

        teardown_test_context(ctx).await;
    }

    #[tokio::test]
    async fn batch_get_statuses_returns_like_timestamps_or_none() {
        let ctx = setup_test_context().await;
        let repo = ctx.repo.clone();

        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");
        let liked_id = Uuid::new_v4();
        let unliked_id = Uuid::new_v4();

        repo.insert_like(user_id, ct.clone(), liked_id).await.expect("seed insert");

        let statuses = repo
            .batch_get_statuses(user_id, &[(ct.clone(), liked_id), (ct.clone(), unliked_id)])
            .await
            .expect("batch statuses");

        assert_eq!(statuses.len(), 2);
        assert!(statuses[0].is_some());
        assert!(statuses[1].is_none());

        teardown_test_context(ctx).await;
    }

    #[tokio::test]
    async fn get_user_likes_returns_paginated_results() {
        let ctx = setup_test_context().await;
        let repo = ctx.repo.clone();

        let user_id = Uuid::new_v4();
        let ct = ContentType::from("post");

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        repo.insert_like(user_id, ct.clone(), id1).await.expect("seed insert 1");
        repo.insert_like(user_id, ct.clone(), id2).await.expect("seed insert 2");
        repo.insert_like(user_id, ct.clone(), id3).await.expect("seed insert 3");

        let page_1 = repo.get_user_likes(user_id, Some(ct.clone()), None, 2).await.expect("page 1");

        assert_eq!(page_1.len(), 2);
        assert!(page_1[0].created_at >= page_1[1].created_at);

        let cursor =
            PaginationCursor { created_at: page_1[1].created_at, id: page_1[1].content_id };

        let page_2 = repo.get_user_likes(user_id, Some(ct), Some(cursor), 2).await.expect("page 2");

        assert_eq!(page_2.len(), 1);
        assert_ne!(page_2[0].content_id, page_1[0].content_id);
        assert_ne!(page_2[0].content_id, page_1[1].content_id);

        teardown_test_context(ctx).await;
    }

    #[tokio::test]
    async fn test_concurrent_likes_race_condition() {
        let ctx = setup_test_context().await;
        let repo = ctx.repo.clone();
        let ct = ContentType::from("post");
        let content_id = Uuid::new_v4();

        // Spawn 100 concurrent like requests for the same content item from different users
        let mut set = tokio::task::JoinSet::new();

        for _ in 0..100 {
            let repo_clone = repo.clone();
            let ct_clone = ct.clone();
            let user_id = Uuid::new_v4(); // Different user every time

            // Spawn tasks directly into the JoinSet
            set.spawn(async move {
                repo_clone.insert_like(user_id, ct_clone, content_id).await.unwrap();
            });
        }

        // Wait for all 100 concurrent requests to finish
        while let Some(res) = set.join_next().await {
            res.expect("Task panicked"); // Handles tokio task panics
        }

        // Verify the DB count is EXACTLY 100
        let final_count = repo.get_count(ct, content_id).await.unwrap();
        assert_eq!(final_count, 100, "Race condition detected: Count should be exactly 100");

        teardown_test_context(ctx).await;
    }
}
