#!/bin/bash

set -e

pushd rust/ballista
cargo publish
popd

