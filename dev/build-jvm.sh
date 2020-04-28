#!/bin/bash

set -e

pushd jvm
./gradlew assemble
popd

docker build -t ballistacompute/ballista-jvm -f docker/jvm-executor.dockerfile jvm/executor

