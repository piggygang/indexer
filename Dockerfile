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
# `dist` is what the runtime stage overlays onto /app. Only indexer-admin needs
# config/ at run time (`seed` reads it from disk); every other binary gets an
# empty dist, so the api image stays config-free. CI asserts the slug below
# still matches services/admin/Cargo.toml.
RUN cargo build --release --bin ${BIN} && cp target/release/${BIN} /app/bin \
    && mkdir -p /app/dist \
    && if [ "${BIN}" = "indexer-admin" ]; then cp -r config /app/dist/config; fi

# debian-slim (not distroless yet): same Debian release as the chef image so
# glibc matches, and a shell for `railway ssh` debugging.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 app
# Re-declared: ARG does not cross stages. After the apt layer so that layer
# stays cacheable regardless of which binary is being built.
ARG BIN=indexer-api
# The binary keeps its own name — `ps`, `railway ssh` and the start command all
# say `indexer-api` / `indexer-admin` rather than a generic `app`.
COPY --from=builder /app/bin /usr/local/bin/${BIN}
COPY --from=builder /app/dist/ /app/
# Exec-form CMD cannot expand a build arg, so persist it and exec through a
# shell; `exec` hands PID 1 to the binary itself, so signals reach it directly.
# Deliberately no ENTRYPOINT: a Railway start command replaces the ENTRYPOINT,
# and whether an image CMD is then appended as arguments is undocumented.
ENV APP_BIN=${BIN}
# `indexer-admin seed` defaults to the relative path config/collections.toml,
# and resolves each mint list relative to that file — so CWD must be /app.
WORKDIR /app
USER app
CMD ["/bin/sh", "-c", "exec \"$APP_BIN\""]
