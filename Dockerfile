# Multi-stage build for any workspace binary, selected via BIN. Railway passes
# service variables as build args only when declared with ARG — the future
# ingester service just sets BIN=indexer-ingester.
# Keep the rust tag in lockstep with rust-toolchain.toml and CI.
FROM lukemathwalker/cargo-chef:latest-rust-1.90 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# ARGs declared after `cook` so the dependency layer is shared across binaries.
ARG BIN=indexer-api
ARG RAILWAY_GIT_COMMIT_SHA=""
ENV GIT_SHA=$RAILWAY_GIT_COMMIT_SHA
RUN cargo build --release --bin ${BIN} && cp target/release/${BIN} /app/bin

# debian-slim (not distroless yet): same Debian release as the chef image so
# glibc matches, and a shell for `railway ssh` debugging.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 app
COPY --from=builder /app/bin /usr/local/bin/app
USER app
CMD ["app"]
