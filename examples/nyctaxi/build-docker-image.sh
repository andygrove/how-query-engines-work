#!/usr/bin/env bash
mkdir -p tmp/ballista/src
mkdir -p tmp/ballista/proto
cp -rf ../../Cargo.* tmp/ballista
cp -rf ../../build.rs tmp/ballista
cp -rf ../../src/* tmp/ballista/src
cp -rf ../../proto/* tmp/ballista/proto
docker build -t ballista-nyctaxi .
