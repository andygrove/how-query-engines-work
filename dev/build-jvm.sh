#!/bin/bash

set -e

pushd jvm
./gradlew clean assemble
popd

docker build -t ballistacompute/ballista-jvm -f docker/jvm-executor.dockerfile jvm/executor

