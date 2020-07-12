# Ballista

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/ballista.svg)](https://crates.io/crates/ballista)
[![Gitter Chat](https://badges.gitter.im/ballista-rs/community.svg)](https://gitter.im/ballista-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Overview

Ballista is a distributed compute platform implemented in Rust using Apache Arrow as the memory model. It is built on
an architecture that allows other programming languages to be supported as first-class citizens.

The foundational technologies in Ballista are:

- **Apache Arrow** memory model and compute kernels for efficient processing of data.
- **Apache Arrow Flight** protocol for efficient data transfer between processes.
- **Google Protocol Buffers** for serializing query plans.
- **Docker** for packaging up executors along with user-defined code.
- **Kubernetes** for deployment and management of the executor docker containers.

## How is Ballista similar to Apache Spark?

Ballista can be considered a partial fork of Apache Spark. The query planning, optimization, scheduling, and 
execution are heavily influenced by Spark's design (although only a tiny subset of operators and expressions are
currently supported).

## How is Ballista different to Apache Spark?

There are two major differences between Ballista and Apache Spark:

1. Ballista is columnar, and Spark is generally row-based (although it does have some columnar support).
2. Ballista uses Apache Arrow as the memory model.

Having a common memory model removes the overhead associated with supporting multiple programming languages. It makes 
it possible for high-level languages such as Python and Java to delegate operations to lower-level languages such as 
C++ and Rust without the need to serialize or copy data within the same process (pointers to memory can be passed 
instead).

The common memory model also makes it possible to transfer data extremely efficiently between processes (regardless of 
implementation programming language) because the memory format is also the serialization format. These network 
transfers could be between processes on the same node (or in the same Kubernetes pod), or between different processes 
within a cluster.

There are different value propositions for different audiences.

## Ballista for Rustaceans

Ballista will provide a distributed compute environment where it will be possible for all processing, including 
user-defined code, to happen in Rust.

However, Ballista will also provide interoperability with other ecosystems, including Apache Spark, allowing Rust to 
be introduced gradually into existing pipelines.

## Ballista for JVM Developers

Ballista provides a JVM query engine (implemented in Kotlin) as well as interoperability with Apache Spark (implemented 
in Scala) . Ballista will also provide support for JNI integration with C++ and/or Rust compute kernels (such as 
delegating to Gandiva or DataFusion).

This will allow JVM developers to leverage their investment in existing code and ecosystem whilst taking advantage of 
the memory efficiency and increased performance from offloading certain operations to lower-level languages.

## Examples

The following examples should help illustrate the current capabilities of Ballista

- [Distributed query execution in Rust](https://github.com/ballista-compute/ballista/tree/main/rust/examples/distributed-query)
- [Rust bindings for Apache Spark](https://github.com/ballista-compute/ballista/tree/main/rust/examples/apache-spark-rust-bindings)

## Documentation

The [Ballista User Guide](https://ballistacompute.org/docs/) is hosted on the 
[Ballista website](https://ballistacompute.org/), along with the [Ballista Blog](https://ballistacompute.org/) where 
news and release notes are posted.
## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.





