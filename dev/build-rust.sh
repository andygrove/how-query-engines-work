#!/bin/bash

set -e

pushd rust
cargo fmt
cargo test
popd

docker build -t ballistacompute/ballista-rust -f docker/rust-executor.dockerfile .
