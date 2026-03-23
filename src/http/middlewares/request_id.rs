use axum::{
    body::{Body, to_bytes},
    extract::Request,
    http::{HeaderMap, HeaderValue, header::CONTENT_TYPE},
    middleware::Next,
    response::Response,
};
use serde_json::Value;
use uuid::Uuid;

const REQUEST_ID_HEADER: &str = "x-request-id";

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

    // 1. Inject down into the app
    request.extensions_mut().insert(RequestId(req_id.clone()));

    // 2. Process the request
    let mut response = next.run(request).await;

    response = annotate_error_response(response, &req_id).await;

    // 3. Add to the outbound response headers
    set_request_id_header(response.headers_mut(), &req_id);

    response
}

/// Helper to set the request ID header on the response
fn set_request_id_header(headers: &mut HeaderMap, request_id: &str) {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        headers.insert(REQUEST_ID_HEADER, value);
    }
}

/// If the response is an error with a JSON body, annotate it with the request ID
async fn annotate_error_response(response: Response, request_id: &str) -> Response {
    // Only annotate if it's an error response with a JSON body
    if !response.status().is_client_error() && !response.status().is_server_error() {
        return response;
    }

    // Check if Content-Type is application/json
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.starts_with("application/json"))
        .unwrap_or(false);

    if !is_json {
        return response;
    }

    // Try to parse the body as JSON and inject the request ID
    let (parts, body) = response.into_parts();
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return Response::from_parts(parts, Body::empty()),
    };

    // If body is not valid JSON, return original body without annotation
    let patched_body = match serde_json::from_slice::<Value>(&bytes) {
        Ok(mut json) => {
            if let Some(error) = json.get_mut("error").and_then(Value::as_object_mut) {
                error.insert("request_id".to_string(), Value::String(request_id.to_string()));
            }

            match serde_json::to_vec(&json) {
                Ok(buffer) => Body::from(buffer),
                Err(_) => Body::from(bytes),
            }
        }
        Err(_) => Body::from(bytes),
    };

    Response::from_parts(parts, patched_body)
}

#[cfg(test)]
mod tests {
    use super::{REQUEST_ID_HEADER, request_id};
    use axum::{
        Json, Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        middleware,
        response::IntoResponse,
        routing::get,
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    fn app() -> Router {
        Router::new()
            .route("/ok", get(|| async { Json(json!({ "status": "ok" })) }))
            .route(
                "/error-json",
                get(|| async {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": {
                                "code": "INVALID_TIME_WINDOW",
                                "message": "Invalid time window parameter: 5m",
                                "details": {
                                    "window": "5m"
                                }
                            }
                        })),
                    )
                }),
            )
            .route(
                "/error-text",
                get(|| async {
                    (StatusCode::INTERNAL_SERVER_ERROR, "plain error").into_response()
                }),
            )
            .layer(middleware::from_fn(request_id))
    }

    async fn body_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
        serde_json::from_slice(&bytes).expect("valid json")
    }

    #[tokio::test]
    async fn success_response_sets_request_id_header() {
        let response = app()
            .oneshot(Request::builder().uri("/ok").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);

        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("request id header");

        assert!(request_id.starts_with("req_"));
    }

    #[tokio::test]
    async fn json_error_response_is_annotated_with_request_id() {
        let response = app()
            .oneshot(Request::builder().uri("/error-json").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let header_request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("request id header")
            .to_string();

        let body = body_json(response).await;
        assert_eq!(body["error"]["code"], "INVALID_TIME_WINDOW");
        assert_eq!(body["error"]["details"]["window"], "5m");
        assert_eq!(body["error"]["request_id"], header_request_id);
    }

    #[tokio::test]
    async fn non_json_error_response_keeps_body_unchanged() {
        let response = app()
            .oneshot(Request::builder().uri("/error-text").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().get(REQUEST_ID_HEADER).is_some());

        let bytes = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
        assert_eq!(bytes, "plain error");
    }
}
