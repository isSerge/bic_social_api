use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use crate::domain::ContentType;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContentTypeRegistryError {
    #[error("Unknown content type: {0}")]
    UnknownContentType(String),
}

/// Registry for content types and their associated base URLs, loaded from environment variables at startup.
#[derive(Debug, Clone)]
pub struct ContentTypeRegistry {
    // Key is the normalized type string, Value is the base URL
    base_urls: HashMap<String, String>,
}

// Default impl for testing purposes only, real registry should always be created from env vars
#[cfg(test)]
impl Default for ContentTypeRegistry {
    fn default() -> Self {
        let mut base_urls = HashMap::new();
        base_urls.insert("post".to_string(), "http://mock".to_string());
        base_urls.insert("bonus_hunter".to_string(), "http://mock".to_string());
        base_urls.insert("top_picks".to_string(), "http://mock".to_string());
        Self { base_urls }
    }
}

impl ContentTypeRegistry {
    /// Scans env vars for CONTENT_API_*_URL at startup
    pub fn from_env() -> Self {
        let mut base_urls = HashMap::new();
        for (key, value) in std::env::vars() {
            if let Some(stripped) = key.strip_prefix("CONTENT_API_") {
                if let Some(type_str) = stripped.strip_suffix("_URL") {
                    base_urls.insert(type_str.to_lowercase(), value);
                }
            }
        }
        ContentTypeRegistry { base_urls }
    }

    /// Converts a raw string from an HTTP path into a validated Domain type
    pub fn validate(&self, raw: &str) -> Result<ContentType, ContentTypeRegistryError> {
        let normalized = raw.to_lowercase();
        if self.base_urls.contains_key(&normalized) {
            Ok(ContentType(Arc::from(normalized)))
        } else {
            Err(ContentTypeRegistryError::UnknownContentType(raw.to_string()))
        }
    }

    /// Used by the Content API HTTP Client
    pub fn get_url(&self, ct: &ContentType) -> &str {
        self.base_urls.get(ct.0.as_ref()).expect("ContentType is guaranteed to exist")
    }

    /// Returns a list of all registered content types
    pub fn get_all_content_types(&self) -> Vec<ContentType> {
        self.base_urls.keys().map(|k| ContentType(Arc::from(k.clone()))).collect()
    }

    /// Helper for tests to create a registry from a list of content type to URL mappings, bypassing env vars
    #[cfg(test)]
    pub fn from_base_urls<I, K, V>(entries: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let base_urls = entries
            .into_iter()
            .map(|(content_type, url)| (content_type.into(), url.into()))
            .collect();

        Self { base_urls }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    static ENV_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn from_env_discovers_content_api_keys() {
        let _guard = ENV_TEST_LOCK.lock().expect("env lock poisoned");

        unsafe {
            std::env::set_var("CONTENT_API_REGISTRY_TEST_POST_URL", "http://post-service:8081");
            std::env::set_var(
                "CONTENT_API_REGISTRY_TEST_NEWS_ARTICLE_URL",
                "http://news-service:8082",
            );
        }

        let registry = ContentTypeRegistry::from_env();

        let post = registry
            .validate("registry_test_post")
            .expect("expected test post type to be discovered");
        let news = registry
            .validate("registry_test_news_article")
            .expect("expected test news type to be discovered");

        assert_eq!(registry.get_url(&post), "http://post-service:8081");
        assert_eq!(registry.get_url(&news), "http://news-service:8082");

        unsafe {
            std::env::remove_var("CONTENT_API_REGISTRY_TEST_POST_URL");
            std::env::remove_var("CONTENT_API_REGISTRY_TEST_NEWS_ARTICLE_URL");
        }
    }

    #[test]
    fn validate_is_case_insensitive() {
        let mut urls = HashMap::new();
        urls.insert("bonus_hunter".to_string(), "http://bonus:8080".to_string());
        let registry = ContentTypeRegistry { base_urls: urls };

        let ct = registry.validate("BoNuS_HuNtEr").expect("mixed case input should validate");

        assert_eq!(ct.0.as_ref(), "bonus_hunter");
        assert_eq!(registry.get_url(&ct), "http://bonus:8080");
    }

    #[test]
    fn validate_rejects_unknown_content_type() {
        let registry = ContentTypeRegistry { base_urls: HashMap::new() };

        let err =
            registry.validate("does_not_exist").expect_err("unknown content type must be rejected");

        assert_eq!(err, ContentTypeRegistryError::UnknownContentType("does_not_exist".to_string()));
    }

    #[test]
    fn get_url_returns_registered_url() {
        let mut urls = HashMap::new();
        urls.insert("top_picks".to_string(), "http://top-picks:8083".to_string());
        let registry = ContentTypeRegistry { base_urls: urls };

        let ct = ContentType(Arc::from("top_picks"));
        assert_eq!(registry.get_url(&ct), "http://top-picks:8083");
    }

    #[test]
    fn get_all_content_types_returns_all_registered_types() {
        let mut urls = HashMap::new();
        urls.insert("top_picks".to_string(), "http://top-picks:8083".to_string());
        urls.insert("bonus_hunter".to_string(), "http://bonus:8080".to_string());
        let registry = ContentTypeRegistry { base_urls: urls };

        let content_types = registry.get_all_content_types();
        let content_type_names: Vec<_> = content_types.iter().map(|ct| ct.0.as_ref()).collect();

        assert!(content_type_names.contains(&"top_picks"));
        assert!(content_type_names.contains(&"bonus_hunter"));
    }
}
