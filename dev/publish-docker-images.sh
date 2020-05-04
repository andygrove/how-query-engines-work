#!/bin/bash

set -e

docker tag ballistacompute/ballista-rust:0.2.5-SNAPSHOT ballistacompute/ballista-rust:latest
docker tag ballistacompute/ballista-jvm:0.2.5-SNAPSHOT ballistacompute/ballista-jvm:latest
docker tag ballistacompute/ballista-spark:0.2.5-SNAPSHOT ballistacompute/ballista-spark:latest

docker tag ballistacompute/rust-benchmarks:0.2.5-SNAPSHOT ballistacompute/rust-benchmarks:latest
docker tag ballistacompute/jvm-benchmarks:0.2.5-SNAPSHOT ballistacompute/jvm-benchmarks:latest
docker tag ballistacompute/spark-benchmarks:0.2.5-SNAPSHOT ballistacompute/spark-benchmarks:latest

docker push ballistacompute/ballista-rust:0.2.5-SNAPSHOT
docker push ballistacompute/ballista-jvm:0.2.5-SNAPSHOT
docker push ballistacompute/ballista-spark:0.2.5-SNAPSHOT
docker push ballistacompute/ballista-rust:latest
docker push ballistacompute/ballista-jvm:latest
docker push ballistacompute/ballista-spark:latest

docker push ballistacompute/rust-benchmarks:0.2.5-SNAPSHOT
docker push ballistacompute/jvm-benchmarks:0.2.5-SNAPSHOT
docker push ballistacompute/spark-benchmarks:0.2.5-SNAPSHOT
docker push ballistacompute/rust-benchmarks:latest
docker push ballistacompute/jvm-benchmarks:latest
docker push ballistacompute/spark-benchmarks:latest
