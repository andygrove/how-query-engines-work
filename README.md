# Ballista

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/ballista.svg)](https://crates.io/crates/ballista)
[![Gitter Chat](https://badges.gitter.im/ballista-rs/community.svg)](https://gitter.im/ballista-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Overview

Ballista is an experimental distributed compute platform, powered by Apache Arrow, with support for Rust and JVM (Java, Kotlin, and Scala).

The foundational technologies in Ballista are:

- **Apache Arrow Flight** protocol for efficient data transfer between processes.
- **Google Protocol Buffers** for serializing query plans.
- **Docker** for packaging up executors along with user-defined code.
- **Kubernetes** for deployment and management of the executor docker containers.

Ballista supports a number of query engines that can be deployed as executors. The executors are responsible for executing query plans.

- **Ballista JVM Executor**: This is a Kotlin-native query engine built on Apache Arrow, and is based on the design in the book [How Query Engines Work](https://leanpub.com/how-query-engines-work).
- **Ballista Rust Executor**: This is a Rust-native query engine built on Apache Arrow and [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion).
- **Apache Spark Executor for Ballista**: This is a wrapper around Apache Spark and allows Ballista queries to be executed by Apache Spark.

Ballista provides DataFrame APIs for Rust and Kotlin. The Kotlin DataFrame API can be used from Java, Kotlin, and Scala because Kotlin is 100% compatible with Java.

## Examples

The following examples should help illustrate the current capabilities of Ballista

- [Rust bindings for Apache Spark](https://github.com/ballista-compute/ballista/tree/master/rust/examples/apache-spark-rust-bindings)
- [Distributed query execution in Rust](https://github.com/ballista-compute/ballista/tree/master/rust/examples/parallel-aggregate)

## Status

- [x] Implement a JVM Executor
- [x] Implement a Rust Executor
- [x] Implement an Apache Spark Executor
- [x] Implement example or Rust invoking a Spark query
- [ ] Implement example of distributed execution in Rust
- [ ] Implement distributed query planner and scheduler
- [ ] Expand capabilities of JVM Executor
- [ ] Expand capabilities of Rust Executor
- [ ] Expand capabilities of Apache Spark Executor
- [ ] Benchmarks
- [ ] Automated integration tests

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.





