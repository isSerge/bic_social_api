use axum::{extract::Request, http::HeaderValue, middleware::Next, response::Response};
use uuid::Uuid;

/// A strongly typed wrapper for the request ID.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

impl RequestId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Generates a request ID, injects it into the request context for handlers to use,
/// and adds it to the response headers.
pub async fn request_id(mut request: Request, next: Next) -> Response {
    // The spec uses "req_..." format (e.g., "req_a1b2c3d4")
    let req_id = format!("req_{}", Uuid::new_v4().simple());
    let request_id = RequestId(req_id.clone());

    // 1. Inject down into the app
    request.extensions_mut().insert(request_id);

    if let Ok(value) = HeaderValue::from_str(&req_id) {
        request.headers_mut().insert("x-request-id", value);
    }

    // 2. Process the request
    let mut response = next.run(request).await;

    // 3. Add to the outbound response headers
    if let Ok(value) = HeaderValue::from_str(&req_id) {
        response.headers_mut().insert("x-request-id", value);
    }

    response
}
