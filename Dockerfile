# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM rust:1-bookworm AS builder

WORKDIR /build

# Cache dependency compilation separately from application source.
# A dummy main.rs is used so Cargo can resolve and compile all dependencies
# before the real source is copied, keeping this layer cacheable.
# SQLX_OFFLINE=true tells sqlx macros to use the .sqlx cache instead of a live DB.
COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
ENV SQLX_OFFLINE=true
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --bin social-api \
    && rm -rf src

# Build the real binary. Touching main.rs forces Cargo to relink.
# .sqlx is already present and SQLX_OFFLINE is already set from the step above.
# migrations/ must be present because sqlx::migrate!() embeds them at compile time.
COPY src ./src
COPY migrations ./migrations
RUN touch src/main.rs \
    && cargo build --release --bin social-api

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# ca-certificates is required for outbound TLS (rustls uses the system trust store).
# curl is used by the HEALTHCHECK instruction below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Run as a dedicated non-root user.
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --no-create-home --shell /sbin/nologin appuser

COPY --from=builder /build/target/release/social-api /usr/local/bin/social-api

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8080/health/live || exit 1

CMD ["/usr/local/bin/social-api"]
