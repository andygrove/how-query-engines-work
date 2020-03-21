# Ballista Docker Images

Pre-built docker images are available from [Docker Hub](https://hub.docker.com/orgs/ballistacompute/repositories) but here are the commands to build the images from source.

Run these commands from the root directory of the project.

```bash
docker build -t ballistacompute/rust-base -f docker/rust-base.dockerfile .
docker build -t ballistacompute/rust-cached-deps -f docker/rust-cached-deps.dockerfile .
docker build -t ballistacompute/rust -f docker/rust.dockerfile .
```

## Publishing Docker Images

Project maintainers can use the following commands to publish versioned images to Docker hub.

```bash
export BALLISTA_VERSION=0.2.0
docker tag ballistacompute/rust-base ballistacompute/rust-base:$BALLISTA_VERSION
docker tag ballistacompute/rust-cached-deps ballistacompute/rust-cached-deps:$BALLISTA_VERSION
docker tag ballistacompute/rust ballistacompute/rust:$BALLISTA_VERSION
docker push ballistacompute/rust-base:$BALLISTA_VERSION
docker push ballistacompute/rust-cached-deps:$BALLISTA_VERSION
docker push ballistacompute/rust:$BALLISTA_VERSION

export BALLISTA_VERSION=latest
docker tag ballistacompute/rust-base ballistacompute/rust-base:$BALLISTA_VERSION
docker tag ballistacompute/rust-cached-deps ballistacompute/rust-cached-deps:$BALLISTA_VERSION
docker tag ballistacompute/rust ballistacompute/rust:$BALLISTA_VERSION
docker push ballistacompute/rust-base:$BALLISTA_VERSION
docker push ballistacompute/rust-cached-deps:$BALLISTA_VERSION
docker push ballistacompute/rust:$BALLISTA_VERSION
```