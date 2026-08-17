#!/usr/bin/env bash
# Everything CI runs, in the order that fails fastest.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> cargo fmt"
cargo fmt --all -- --check

echo "==> cargo clippy"
cargo clippy --all-targets --all-features -- -D warnings

echo "==> cargo test"
# A dedicated test database: the harness truncates every table between tests.
export TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgres://chainrail:chainrail@127.0.0.1:55432/chainrail_test}"
if ! psql "$TEST_DATABASE_URL" -c 'select 1' >/dev/null 2>&1; then
    echo "    no test database at $TEST_DATABASE_URL -- integration tests will skip"
    unset TEST_DATABASE_URL
fi
cargo test --all

echo "==> all checks passed"
