# ballista

Ballista is a proof-of-concept distributed compute platform based on Kubernetes and the Rust implementation of Apache Arrow.

This is not my first attempt at building something like this. I originally wanted DataFusion to be a distributed compute platform but this was overly ambitious at the time, and it ended up becoming an in-memory query execution engine for the Rust implementation of Apache Arrow. However, DataFusion now provides a good foundation to have another attempt at building a modern distributed compute platform in Rust.

My goal is to use this repo to move fast and try out ideas that eventually can be contributed back to Apache Arrow and to help drive requirements for Apache Arrow and DataFusion.

I will be working on this project in my spare time, which is limited, so progress will likely be slow. 

# PoC Status

- [X] README describing project
- [ ] Protobuf file defining simple gRPC service and query plan
- [ ] Generate code from protobuf file
- [ ] Implement skeleton gRPC server
- [ ] Implement skeleton gRPC client
- [ ] CLI to create cluster using Kubernetes
- [ ] Example client to create query plan
- [ ] Client can send query plan
- [ ] Server can receive query plan
- [ ] Server can execute query plan using DataFusion
- [ ] Server can write results to CSV files
- [ ] Server can stream Arrow data back to client
- [ ] Benchmarks
- [ ] Implement Flight protocol








 


