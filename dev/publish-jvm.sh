#!/bin/bash

set -e

pushd jvm
./gradlew clean publish
popd

