FROM rustlang/rust:nightly-slim AS build

USER root

RUN apt update && apt -y install libssl-dev openssl

# fake build
WORKDIR /tmp
RUN USER=root cargo new --bin ballista
WORKDIR /tmp/ballista
COPY Cargo.fake ./
RUN cp Cargo.fake Cargo.toml
RUN cargo build --release
RUN rm -rf src

# Copy Ballista sources
RUN mkdir -p /tmp/ballista/src
RUN mkdir -p /tmp/ballista/proto
COPY Cargo.toml /tmp/ballista
COPY Cargo.lock /tmp/ballista
COPY build.rs /tmp/ballista
COPY src /tmp/ballista/src
COPY proto /tmp/ballista/proto

# compile
RUN cargo install --path .
RUN rm -rf target

EXPOSE 50051

CMD ["ballista-server"]
