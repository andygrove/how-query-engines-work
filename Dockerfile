FROM rustlang/rust:nightly AS build

USER root

RUN apt update && apt -y install musl musl-dev musl-tools

# Download the target for static linking.
RUN rustup target add x86_64-unknown-linux-musl

# add submodules
RUN mkdir -p /tmp/ballista/arrow/rust
ADD arrow/rust /tmp/ballista/arrow/rust

RUN mkdir -p /tmp/ballista/tower-grpc
ADD tower-grpc /tmp/ballista/tower-grpc

# Copy Ballista sources
RUN mkdir -p /tmp/ballista/src
RUN mkdir -p /tmp/ballista/proto
COPY Cargo.toml /tmp/ballista
COPY Cargo.lock /tmp/ballista
COPY build.rs /tmp/ballista
COPY src /tmp/ballista/src
COPY proto /tmp/ballista/proto

# compile
WORKDIR /tmp/ballista
RUN cargo install --target x86_64-unknown-linux-musl --path .

# Copy the statically-linked binary into a scratch container.
FROM scratch
COPY --from=build /usr/local/cargo/bin/ballista-server .
USER 1000

EXPOSE 50051

CMD ["./ballista-server"]
