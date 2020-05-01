#!/bin/bash

set -e

docker tag ballistacompute/ballista-rust:latest ballistacompute/ballista-rust:$BALLISTA_VERSION
docker push ballistacompute/ballista-rust:$BALLISTA_VERSION
docker push ballistacompute/ballista-rust:latest

docker tag ballistacompute/ballista-jvm:latest ballistacompute/ballista-jvm:$BALLISTA_VERSION
docker push ballistacompute/ballista-jvm:$BALLISTA_VERSION
docker push ballistacompute/ballista-jvm:latest

docker tag ballistacompute/ballista-spark:latest ballistacompute/ballista-spark:$BALLISTA_VERSION
docker push ballistacompute/ballista-spark:$BALLISTA_VERSION
docker push ballistacompute/ballista-spark:latest

