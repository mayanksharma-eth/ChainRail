# Multi-stage build.
#
# The dependency layer is built separately from the source layer so that editing
# application code does not rebuild librdkafka and the alloy tree, which
# dominate build time.

FROM rust:1-bookworm AS builder

# librdkafka builds from source (cmake), and rustls needs no OpenSSL.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake \
        libsasl2-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# --- dependency layer -------------------------------------------------------
# Copy only manifests, then build stub targets so Cargo resolves and compiles
# every dependency into a cached layer.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/common/Cargo.toml            crates/common/
COPY crates/database/Cargo.toml          crates/database/
COPY crates/observability/Cargo.toml     crates/observability/
COPY crates/rpc/Cargo.toml               crates/rpc/
COPY crates/signer/Cargo.toml            crates/signer/
COPY crates/chains-evm/Cargo.toml        crates/chains-evm/
COPY crates/ledger/Cargo.toml            crates/ledger/
COPY crates/events/Cargo.toml            crates/events/
COPY crates/risk/Cargo.toml              crates/risk/
COPY crates/deposits/Cargo.toml          crates/deposits/
COPY crates/withdrawals/Cargo.toml       crates/withdrawals/
COPY crates/api/Cargo.toml               crates/api/
COPY apps/server/Cargo.toml              apps/server/
COPY apps/worker/Cargo.toml              apps/worker/
COPY tests/integration/Cargo.toml        tests/integration/
COPY tests/load/Cargo.toml               tests/load/

RUN set -eux; \
    for d in crates/common crates/database crates/observability crates/rpc crates/signer \
             crates/chains-evm crates/ledger crates/events crates/risk crates/deposits \
             crates/withdrawals crates/api tests/integration tests/load; do \
        mkdir -p "$d/src"; echo "" > "$d/src/lib.rs"; \
    done; \
    for d in apps/server apps/worker; do \
        mkdir -p "$d/src"; echo "fn main() {}" > "$d/src/main.rs"; \
    done
RUN cargo build --release --bin chainrail-server --bin chainrail-worker

# --- source layer -----------------------------------------------------------
COPY crates ./crates
COPY apps ./apps
COPY tests ./tests
COPY migrations ./migrations

# Touch the roots so Cargo rebuilds the workspace crates but not their deps.
RUN find crates apps -name '*.rs' -newermt '1970-01-01' -exec touch {} + \
    && cargo build --release --bin chainrail-server --bin chainrail-worker \
    && strip target/release/chainrail-server target/release/chainrail-worker

# --- runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsasl2-2 \
    && rm -rf /var/lib/apt/lists/* \
    # Run unprivileged: a container that cannot write its own filesystem is one
    # less lever for an attacker who achieves code execution.
    && useradd --system --uid 10001 --create-home --shell /usr/sbin/nologin chainrail

COPY --from=builder /build/target/release/chainrail-server /usr/local/bin/chainrail-server
COPY --from=builder /build/target/release/chainrail-worker /usr/local/bin/chainrail-worker

WORKDIR /app
USER 10001:10001

ENV RUST_BACKTRACE=1 \
    CHAINRAIL_CONFIG_DIR=/app/config

EXPOSE 8080 9090

CMD ["/usr/local/bin/chainrail-server"]
