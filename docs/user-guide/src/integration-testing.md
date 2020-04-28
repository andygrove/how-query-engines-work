# Integration Testing

## Start the executors

The integration-tests directory contains a `docker-compose.yml` defining the three executors.

These executors can be started by running `docker-compose up` and can be terminated by running `docker-compose down`.

```
version: '2.0'
services:
  ballista-rust:
    image: ballistacompute/ballista-rust
    ports:
      - "50051:50051"
  ballista-jvm:
    image: ballistacompute/ballista-jvm
    ports:
      - "50052:50051"
  ballista-spark:
    image: ballistacompute/ballista-spark
    ports:
      - "50053:50051"
```

## Run the tests

Currently, there is a single integration test, implemented in Rust.

```bash
cd rust
cargo run
```
