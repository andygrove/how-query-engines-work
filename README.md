# Ballista

Ballista ~is~ will be a proof-of-concept distributed compute platform based on Kubernetes and the Rust implementation of [Apache Arrow](https://arrow.apache.org/).

This is not my first attempt at building something like this. I originally wanted [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion) to be a distributed compute platform but this was overly ambitious at the time, and it ended up becoming an in-memory query execution engine for the Rust implementation of Apache Arrow. However, DataFusion now provides a good foundation to have another attempt at building a [modern distributed compute platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) in Rust.

My goal is to use this repo to move fast and try out ideas that eventually can be contributed back to Apache Arrow and to help drive requirements for Apache Arrow and DataFusion.

I will be working on this project in my spare time, which is limited, so progress will likely be slow. 

# PoC Status

- [X] README describing project
- [X] Define service and minimal query plan in protobuf file
- [X] Generate code from protobuf file
- [X] Implement skeleton gRPC server
- [X] Implement skeleton gRPC client
- [X] Client can send query plan
- [X] Server can receive query plan
- [X] Server can translate protobuf query plan to DataFusion query plan
- [X] Server can execute query plan using DataFusion
- [X] Create Dockerfile for server
- [ ] Server can write results to CSV files
- [ ] CLI to create cluster using Kubernetes
- [ ] Benchmarks
- [ ] Implement Flight protocol
- [ ] Server can stream Arrow data back to client
- [ ] Implement support for all DataFusion logical plan and expressions

# Building

The project currently uses git submodules for the arrow and tower-grpc dependencies.

```bash
git submodule update --init
cargo build
```

# Run server locally

```bash
cargo run --bin server
```

# Run example client

```bash
cargo run --example example1
```

# Build Docker image for server

```bash
docker build -t ballista .
```







 


