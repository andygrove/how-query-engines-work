#!/bin/bash

set -e

docker tag ballistacompute/ballista-rust:0.2.4 ballistacompute/ballista-rust:latest
docker tag ballistacompute/ballista-jvm:0.2.4 ballistacompute/ballista-jvm:latest
docker tag ballistacompute/ballista-spark:0.2.4 ballistacompute/ballista-spark:latest

docker tag ballistacompute/rust-benchmarks:0.2.4 ballistacompute/rust-benchmarks:latest
docker tag ballistacompute/jvm-benchmarks:0.2.4 ballistacompute/jvm-benchmarks:latest
docker tag ballistacompute/spark-benchmarks:0.2.4 ballistacompute/spark-benchmarks:latest

docker push ballistacompute/ballista-rust:0.2.4
docker push ballistacompute/ballista-jvm:0.2.4
docker push ballistacompute/ballista-spark:0.2.4
docker push ballistacompute/ballista-rust:latest
docker push ballistacompute/ballista-jvm:latest
docker push ballistacompute/ballista-spark:latest

docker push ballistacompute/rust-benchmarks:0.2.4
docker push ballistacompute/jvm-benchmarks:0.2.4
docker push ballistacompute/spark-benchmarks:0.2.4
docker push ballistacompute/rust-benchmarks:latest
docker push ballistacompute/jvm-benchmarks:latest
docker push ballistacompute/spark-benchmarks:latest
