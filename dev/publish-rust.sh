#!/bin/bash

set -e

pushd rust
cargo publish
popd

