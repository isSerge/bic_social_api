use axum::Router;
use std::net::SocketAddr;

/// Resolves the port from the PORT environment variable or falls back to the default.
pub fn resolve_port(default_port: u16) -> u16 {
    std::env::var("PORT").ok().and_then(|v| v.parse::<u16>().ok()).unwrap_or(default_port)
}

/// A simple server runner for Mocks
pub async fn serve_mock(app: Router, default_port: u16, service_name: &str) {
    let port = resolve_port(default_port);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind listener for {service_name}: {e}"));

    tracing::info!("{} listening on {}", service_name, addr);

    axum::serve(listener, app)
        .await
        .unwrap_or_else(|e| panic!("{service_name} server failed: {e}"));
}

/// Spawns an Axum router on a random ephemeral port and returns the base URL.
/// Reusable for integration tests across both Mocks and the main App.
#[cfg(test)]
pub async fn spawn_test_server(app: Router) -> String {
    // Explicitly bind to IPv4 localhost
    let listener =
        tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("Failed to bind test listener");

    let addr = listener.local_addr().expect("Failed to get local addr");

    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("Test server failed to run");
    });

    format!("http://{addr}")
}
