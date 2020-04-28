#!/bin/bash

set -e

pushd spark
./gradlew assemble
popd

docker build -t ballistacompute/ballista-spark -f docker/spark-executor.dockerfile spark


