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

## Why Ballista?

Ballista is at a very early stage of development and therefore has little value currently, but the hope is to demonstrate a number of benefits due to the choice of Apache Arrow as the memory model.

Having a common memory model removes the overhead associated with supporting multiple programming languages. It makes it possible for high-level languages such as Python and Java to delegate operations to lower-level languages such as C++ and Rust without the need to serialize or copy data within the same process (pointers to memory can be passed instead).

The common memory model also makes it possible to transfer data extremely efficiently between processes (regardless of implementation programming language) because the memory format is also the serialization format. These network transfers could be between processes on the same node (or in the same Kubernetes pod), or between different pods within a cluster.

There are different value propositions for different audiences.

## Ballista for Rustaceans

Ballista will provide a distributed compute environment where it will be possible for all processing, including user-defined code, to happen in Rust.

However, Ballista will also provide interoperability with other ecosystems, including Apache Spark, allowing Rust to be introduced gradually into existing pipelines.

## Ballista for JVM Developers

Ballista provides a JVM query engine (implemented in Kotlin) as well as interoperability with Apache Spark (implemented in Scala) . Ballista will also provide support for JNI integration with C++ and/or Rust compute kernels (such as delegating to Gandiva or DataFusion).

This will allow JVM developers to leverage their investment in existing code and ecosystem whilst taking advantage of the memory efficiency and increased performance from offloading certain operations to lower-level languages.

## Examples

The following examples should help illustrate the current capabilities of Ballista

- [Rust bindings for Apache Spark](https://github.com/ballista-compute/ballista/tree/master/rust/examples/apache-spark-rust-bindings)
- [Distributed query execution in Rust](https://github.com/ballista-compute/ballista/tree/master/rust/examples/parallel-aggregate)

## Status

- It is possible to manually execute distributed hash aggregate queries and simple filters and projections in Rust and JVM.
- The distributed scheduler work is being designed with the hope of this being available in the Summer of 2002.

## Documentation

The [Ballista User Guide](https://ballistacompute.org/docs/) is hosted on the [Ballista website](https://ballistacompute.org/), along with the [Ballista Blog](https://ballistacompute.org/) where news and release notes are posted.
## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.





