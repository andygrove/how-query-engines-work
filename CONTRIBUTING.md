# Contributing to Ballista

# First steps

Please read my article [How to build a modern distributed compute platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) since it is a good introduction to how I think Ballista (and other distributed compute platforms) should work. This article is a work in progress that I update from time to time, as I learn more about this subject, or when I feel motivated to write.

There is also a [wiki](https://github.com/andygrove/ballista/wiki) with a list of interesting reading material.

This project depends on some existing technologies, so it is a good idea to learn a little about those too:

- Apache Arrow
- DataFusion
- Kubernetes
- gRPC

# Introduce yourself!

We have a [Gitter IM room](https://gitter.im/ballista-rs/community) for discussing this project. 

# Issues

See https://github.com/andygrove/ballista/issues for a list of open issues. I recommend starting here when contributing because there is a plan in place for delivering useful point solutions along the way as the project heads towards a v1.0 release. For example, working on a distributed query planner is fun but doesn't make sense until we have the ability to execute a hand-written plan, and Ballista has value even if users have to hand-write plans.

# Creating Pull Requests

This project uses the standard [GitHub Forking Workflow](https://gist.github.com/Chaser324/ce0505fbed06b947d962).

# Code Formatting

To make sure your build passes the checks in Travis CI, run these commands before creating a pull request. Builds will fail if the code is not formatted correctly.

```
cargo fmt --all
cargo clippy --all
cargo test
```