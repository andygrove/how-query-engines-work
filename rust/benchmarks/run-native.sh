#!/usr/bin/env bash
export BENCH_MODE=local
export BENCH_PATH=/mnt/nyctaxi/csv/year=2019
export BENCH_RESULT_FILE=rust-debug.txt

# run natively
cargo run --release
