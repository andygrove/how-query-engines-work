# Ballista

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/ballista.svg)](https://crates.io/crates/ballista)
[![Gitter Chat](https://badges.gitter.im/ballista-rs/community.svg)](https://gitter.im/ballista-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Overview

Ballista is an experimental distributed compute platform based on [Kubernetes](https://kubernetes.io/) and [Apache Arrow](https://arrow.apache.org/) that I am developing in my spare time as a way to learn more about distributed data processing. It is largely inspired by Apache Spark.

Ballista aims to be language-agnostic with an architecture that is capable of supporting any language supported by Apache Arrow, which currently includes C, C++, C#, Go, Java, JavaScript, MATLAB, Python, R, Ruby, and Rust. 

# Architecture

- Query plans are defined in Google Protocol Buffer format
- Data is exchanged using Apache Arrow Flight protocol
- Executors can be built in any language
- Clients can be built in any language
- Ballista will orchestrate execution
- Seamless integration with Apache Spark and other platforms with provided connectors

# Status

I have recently re-started this project. Here is the current plan.

- [ ] Implement Logical Plan and DataFrame API for Rust

- [ ] Implement Logical Plan and DataFrame API for JVM (Java, Kotlin, Scala)

- [ ] Implement Executor in Kotlin

- [ ] Implement Executor in Rust (based on DataFusion)

- [ ] Implement Executor in Scala (wrapping Apache Spark)

- [ ] Implement a JDBC driver that can execute a SQL statement against an executor

  

# PoC #1

The following content is from the original PoC that I built in July 2019. See my [blog post](https://andygrove.io/2019/07/announcing-ballista/) for more information.

This demo shows a Ballista cluster being created in Minikube and then shows the [nyctaxi example](examples/nyctaxi) being executed, causing a distributed query to run in the cluster, with each executor pod performing an aggregate query on one partition of the data, and then the driver merges the results and runs a secondary aggregate query to get the final result. 

[![asciicast](https://asciinema.org/a/SArI3f8PVFjgc45wHubEQQnca.svg)](https://asciinema.org/a/UCdmelZpxeACYVSeAlGHSWBRr)

Here are the commands being run, with some explanation:

```bash
# create a cluster with 12 executors
cargo run --bin ballista -- create-cluster --name nyctaxi --num-executors 12 --template examples/nyctaxi/templates/executor.yaml

# check status
kubectl get pods

# run the nyctaxi example application, that executes queries using the executors
cargo run --bin ballista -- run --name nyctaxi --template examples/nyctaxi/templates/application.yaml

# check status again to find the name of the application pod
kubectl get pods

# watch progress of the application
kubectl logs -f ballista-nyctaxi-app-n5kxl
```

Note that PoC #1 is now archived [here](archive/poc1).

# Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.




