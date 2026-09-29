# syntax=docker/dockerfile:1.4
# ==============================================================================
# Chain-Market-Price Multi-Stage Production Container
# Hardware Tier: Auto-Negotiated (Tier 1 Workstation / Tier 2 Cloud Virtualized)
# ==============================================================================

# Stage 1: Build stage
FROM rust:1.82-bookworm AS builder
WORKDIR /app

# Cache layer: copy Cargo definitions
COPY Cargo.toml ./
COPY crates/core-engine/Cargo.toml crates/core-engine/
COPY crates/ingest-models/Cargo.toml crates/ingest-models/
COPY crates/distribution/Cargo.toml crates/distribution/
COPY crates/cefi-ingest/Cargo.toml crates/cefi-ingest/
COPY crates/amm-virtualizer/Cargo.toml crates/amm-virtualizer/
COPY crates/onchain-ingest/Cargo.toml crates/onchain-ingest/

# Copy actual source trees
COPY crates/ crates/

# Compile production release binaries
RUN cargo build --release --bin cmp-calibrate

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user setup (UID 10001)
RUN groupadd -g 10001 cmp && \
    useradd -u 10001 -g cmp -s /bin/bash -m cmp

WORKDIR /app

COPY --from=builder /app/target/release/cmp-calibrate /app/cmp-calibrate
COPY deploy/docker/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh /app/cmp-calibrate && \
    chown -R cmp:cmp /app

USER 10001:10001

EXPOSE 9001 9002

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/app/cmp-calibrate", "full"]
