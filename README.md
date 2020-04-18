# Ballista

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/ballista.svg)](https://crates.io/crates/ballista)
[![Gitter Chat](https://badges.gitter.im/ballista-rs/community.svg)](https://gitter.im/ballista-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

## Overview

Ballista is an experimental distributed compute platform based on [Kubernetes](https://kubernetes.io/) and [Apache Arrow](https://arrow.apache.org/) that I am developing in my spare time as a way to learn more about distributed data processing. I am documenting the design in my book [How Query Engines Work](https://leanpub.com/how-query-engines-work) as I learn.

Ballista aims to be language-agnostic with an architecture that is capable of supporting any language supported by Apache Arrow, with an initial focus on supporting Rust and JVM (Java, Kotlin, and Scala). 

## Architecture

- Query plans are defined in Google Protocol Buffer format
- Data is exchanged using [Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) protocol
- Query Executors can be built in any language supported by Arrow
- Clients can be built in any language supported by Arrow
- Ballista will provide distributed query planning and orchestration functionality 
- Seamless integration with Apache Spark and other platforms with provided connectors and drivers

## Examples

The following examples should help illustrate the current capabilities of Ballista

- [Rust bindings for Apache Spark](https://github.com/ballista-compute/ballista/tree/master/rust/examples/apache-spark-rust-bindings)
- [Distributed query execution in Rust](https://github.com/ballista-compute/ballista/tree/master/rust/examples/parallel-aggregate)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.




