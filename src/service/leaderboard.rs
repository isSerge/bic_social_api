use std::{sync::Arc, time::Duration};

use tokio::task::JoinSet;

use crate::{
    config::{AppConfig, ContentTypeRegistry},
    domain::ContentType,
    repository::{cache_repo::CacheRepository, like_repo::LikeRepository},
};

/// Worker responsible for periodically calculating and updating the leaderboard in the cache.
#[derive(Clone)]
pub struct LeaderboardWorker {
    /// Application configuration for controlling worker behavior (e.g., update interval).
    config: Arc<AppConfig>,
    /// Registry for content types to ensure valid content type handling.
    registry: Arc<ContentTypeRegistry>,
    /// Repository for accessing like data from the database.
    db: Arc<dyn LikeRepository>,
    /// Repository for caching leaderboard data.
    cache: Arc<dyn CacheRepository>,
}

impl LeaderboardWorker {
    pub fn new(
        config: Arc<AppConfig>,
        registry: Arc<ContentTypeRegistry>,
        db: Arc<dyn LikeRepository>,
        cache: Arc<dyn CacheRepository>,
    ) -> Self {
        Self { config, registry, db, cache }
    }

    /// Main loop for the worker, periodically calculating and updating the leaderboard.
    pub async fn start(self) {
        let interval = Duration::from_secs(self.config.app.leaderboard_refresh_interval_secs);
        let max_limit = self.config.limits.max_top_liked_limit as i64;

        tracing::info!(
            service = "social-api",
            interval_secs = interval.as_secs(),
            "Starting Leaderboard background worker",
        );

        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            self.refresh_all_leaderboards(max_limit).await;
        }
    }

    /// Refreshes all leaderboards (all time windows and content types) by querying the database for the most liked content and updating the cache.
    async fn refresh_all_leaderboards(&self, max_limit: i64) {
        let windows = vec![
            ("24h", Some(chrono::Utc::now() - chrono::Duration::hours(24))),
            ("7d", Some(chrono::Utc::now() - chrono::Duration::days(7))),
            ("30d", Some(chrono::Utc::now() - chrono::Duration::days(30))),
            ("all_time", None),
        ];

        // Use a JoinSet to run all refresh tasks in parallel
        let mut refresh_tasks = JoinSet::new();

        // Refresh the global leaderboard for each time window in parallel.
        for (window_name, since) in &windows {
            let worker = self.clone();
            let window_name = *window_name;
            let since = *since;
            refresh_tasks.spawn(async move {
                worker.refresh_single_leaderboard(None, window_name, since, max_limit).await;
            });
        }

        // Refresh each content-type/time-window combination in parallel.
        for content_type in self.registry.get_all_content_types() {
            for (window_name, since) in &windows {
                let worker = self.clone();
                let content_type = content_type.clone();
                let window_name = *window_name;
                let since = *since;
                refresh_tasks.spawn(async move {
                    worker
                        .refresh_single_leaderboard(
                            Some(content_type),
                            window_name,
                            since,
                            max_limit,
                        )
                        .await;
                });
            }
        }

        // Await all refresh tasks to complete before exiting the function, ensuring that all leaderboards are updated before the next interval. Log any task panics as errors.
        while let Some(task_result) = refresh_tasks.join_next().await {
            if let Err(error) = task_result {
                tracing::error!(error = %error, "Leaderboard refresh task panicked");
            }
        }
    }

    /// Refreshes a single leaderboard for a specific content type and time window by querying the database and updating the cache.
    /// If content_type is None, it refreshes the overall leaderboard for that time window regardless of content type.
    /// If since is None, it calculates the all-time leaderboard.
    /// Handles database errors gracefully by logging and skipping the refresh cycle, and logs cache update failures as warnings since the read API can fall back to DB or stale cache in that case.
    async fn refresh_single_leaderboard(
        &self,
        content_type: Option<ContentType>,
        window_name: &str,
        since: Option<chrono::DateTime<chrono::Utc>>,
        max_limit: i64,
    ) {
        // Query the database for the top liked content of the specified type and time window
        let top_liked = self.db.get_top_liked(content_type.clone(), since, max_limit).await;

        let items = match top_liked {
            Ok(items) => items,
            Err(e) => {
                tracing::error!(
                  error = %e,
                  window = %window_name,
                  content_type = ?content_type.as_ref().map(|c| c.0.as_ref()),
                  "Failed to query DB for leaderboard refresh"
                );
                // On DB error, skip this refresh cycle but keep the worker running for the next interval
                return;
            }
        };

        // Update the cache with the new leaderboard data
        if let Err(e) = self.cache.set_leaderboard(content_type.clone(), window_name, items).await {
            tracing::warn!(
              error = %e,
              window = %window_name,
              content_type = ?content_type.as_ref().map(|c| c.0.as_ref()),
              "Failed to update Redis leaderboard. Read API will fall back to DB or stale cache."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use mockall::predicate::eq;
    use std::io::{Error, ErrorKind};
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::config::{AppConfig, ContentTypeRegistry};
    use crate::repository::{
        cache_repo::MockCacheRepository, error::RepoError, like_repo::MockLikeRepository,
    };

    // Helper to create a test worker with mocked dependencies
    fn setup_worker(
        mock_db: MockLikeRepository,
        mock_cache: MockCacheRepository,
    ) -> LeaderboardWorker {
        let config = Arc::new(AppConfig::default());
        let registry = Arc::new(ContentTypeRegistry::default());

        LeaderboardWorker::new(config, registry, Arc::new(mock_db), Arc::new(mock_cache))
    }

    // TODO: use common helper
    fn content_type(name: &str) -> ContentType {
        ContentType(Arc::from(name.to_string()))
    }

    #[tokio::test]
    async fn test_refresh_single_leaderboard_success() {
        let content_type = content_type("post");
        let window = "24h";
        // Create a fixed timestamp so we can assert exact matching
        let since = Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap();
        let limit = 50;

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let ct_clone1 = content_type.clone();
        let ct_clone2 = content_type.clone();

        // 1. Expect the worker to query the database
        let mut mock_db = MockLikeRepository::new();
        mock_db
            .expect_get_top_liked()
            .with(eq(Some(content_type.clone())), eq(Some(since)), eq(limit))
            .times(1)
            .returning(move |_, _, _| {
                Ok(vec![(ct_clone1.clone(), id1, 1500), (ct_clone2.clone(), id2, 900)])
            });

        // 2. Expect the worker to push the mapped results to Redis
        let expected_redis_items =
            vec![(content_type.clone(), id1, 1500), (content_type.clone(), id2, 900)];
        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_set_leaderboard()
            .with(eq(Some(content_type.clone())), eq(window), eq(expected_redis_items))
            .times(1)
            .returning(|_, _, _| Ok(()));

        let worker = setup_worker(mock_db, mock_cache);

        // 3. Act: Run the single refresh function (bypassing the infinite loop)
        worker.refresh_single_leaderboard(Some(content_type), window, Some(since), limit).await;
    }

    #[tokio::test]
    async fn test_refresh_single_leaderboard_db_error_aborts_cleanly() {
        let content_type = content_type("post");
        let window = "24h";
        let since = Utc::now() - Duration::hours(24);
        let limit = 50;

        // 1. Force the database to return an error
        let mut mock_db = MockLikeRepository::new();
        mock_db
            .expect_get_top_liked()
            .times(1)
            .returning(|_, _, _| Err(RepoError::Db(sqlx::Error::PoolTimedOut)));

        // 2. Expect Redis to NEVER be called because the DB failed
        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_set_leaderboard().times(0);

        let worker = setup_worker(mock_db, mock_cache);

        // 3. Act: The worker must catch the error, log it, and return cleanly without panicking.
        worker.refresh_single_leaderboard(Some(content_type), window, Some(since), limit).await;
    }

    #[tokio::test]
    async fn test_refresh_single_leaderboard_redis_error_completes_cleanly() {
        let content_type = content_type("post");
        let window = "24h";
        let since = Utc::now() - Duration::hours(24);
        let limit = 50;
        let id1 = Uuid::new_v4();

        // 1. The database successfully returns data
        let mut mock_db = MockLikeRepository::new();
        mock_db
            .expect_get_top_liked()
            .times(1)
            .returning(move |c, _, _| Ok(vec![(c.unwrap(), id1, 100)]));

        // 2. Redis returns a connection error
        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_set_leaderboard().times(1).returning(|_, _, _| {
            Err(RepoError::CachePool(deadpool_redis::PoolError::Backend(
                Error::new(ErrorKind::ConnectionRefused, "Redis offline").into(),
            )))
        });

        let worker = setup_worker(mock_db, mock_cache);

        // 3. Act: The worker must catch the Redis error, log a warning, and return cleanly.
        worker.refresh_single_leaderboard(Some(content_type), window, Some(since), limit).await;
    }

    #[tokio::test]
    async fn test_refresh_all_leaderboards_refreshes_every_window_and_content_type() {
        let registry = ContentTypeRegistry::from_base_urls([
            ("post".to_string(), "http://content.local".to_string()),
            ("bonus_hunter".to_string(), "http://content.local".to_string()),
        ]);
        let content_type_count = registry.get_all_content_types().len();
        let expected_refreshes = 4 + (4 * content_type_count);

        // Expect the worker to refresh each content-type/time-window combination, plus the global leaderboards for each time window. We don't care about the exact input parameters in this test, just that the correct number of refreshes happen to cover all combinations.
        let mut mock_db = MockLikeRepository::new();
        mock_db.expect_get_top_liked().times(expected_refreshes).returning(|content_type, _, _| {
            Ok(content_type
                .into_iter()
                .map(|content_type| (content_type, Uuid::new_v4(), 100))
                .collect())
        });

        // Expect the worker to attempt to update Redis for each refresh.
        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_set_leaderboard().times(expected_refreshes).returning(|_, _, _| Ok(()));

        let worker = LeaderboardWorker::new(
            Arc::new(AppConfig::default()),
            Arc::new(registry),
            Arc::new(mock_db),
            Arc::new(mock_cache),
        );

        worker.refresh_all_leaderboards(50).await;
    }
}
