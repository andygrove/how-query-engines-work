#!/usr/bin/env bash

set -e

rm -rf temp 2>/dev/null

# copy protobuf
mkdir -p temp/proto
cp -rf ../../proto/* temp/proto/

# copy Ballista source
mkdir -p temp/rust/src
cp ../../rust/build.rs temp/rust/
cp -rf ../../rust/src/* temp/rust/src/
cp -rf ../../rust/Cargo.* temp/rust/

docker build -t ballistacompute/parallel-aggregate-rs .