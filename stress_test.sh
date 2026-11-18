#!/usr/bin/env bash
# ==============================================================================
# Last Server Standing — Native Progressive RPS Saturation Benchmark
#
# Tests exact request-per-second rates:
# - Stage 1:  1,000 req/sec
# - Stage 2:  5,000 req/sec
# - Stage 3:  8,000 req/sec
# - Stage 4: 12,000 req/sec
# - Stage 5: 16,000 req/sec
# - Stage 6: 20,000 req/sec
# - Stage 7: 24,000 req/sec
# - Stage 8: 28,000 req/sec
# - Stage 9: 32,000 req/sec
#
# Runs each stage for 5 seconds and tests until the server reaches saturation.
# ==============================================================================

set -e

HOST="${1:-127.0.0.1}"
PORT="${2:-8080}"

echo "Building native high-performance RPS test runner..."
cargo build --release --bin rps_test

echo "Executing progressive RPS saturation test against http://${HOST}:${PORT}..."
./target/release/rps_test "${HOST}" "${PORT}"
