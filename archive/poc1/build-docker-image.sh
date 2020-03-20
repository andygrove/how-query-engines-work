#!/usr/bin/env bash

REPO=andygrove

docker build -t ${REPO}/ballista-platform -f docker/rust-base.dockerfile .
docker build -t ${REPO}/ballista -f docker/rust.dockerfile --build-arg REPO=${REPO} .
