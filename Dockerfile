FROM rustlang/rust:nightly

RUN mkdir -p /opt/ballista/arrow/rust
ADD arrow/rust /opt/ballista/arrow/rust

RUN mkdir -p /opt/ballista/tower-grpc
ADD tower-grpc /opt/ballista/tower-grpc

# Copy Ballista sources
RUN mkdir -p /opt/ballista/src
RUN mkdir -p /opt/ballista/proto
COPY Cargo.toml /opt/ballista
COPY Cargo.lock /opt/ballista
COPY build.rs /opt/ballista
COPY src /opt/ballista/src
COPY proto /opt/ballista/proto

WORKDIR /opt/ballista

EXPOSE 50051

CMD cargo run --bin server