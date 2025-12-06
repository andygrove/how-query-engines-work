# How Query Engines Work

This is the companion repository for the book [How Query Engines Work](https://leanpub.com/how-query-engines-work)
and contains source code for a simple in-memory query engine implemented in Kotlin.

The query engine is designed to be easy to learn and hack on rather than being optimized for 
performance, scalability, or robustness.

The query engine contains the following components:

- DataFrame API
- SQL Parser
- SQL Query Planner
- Logical Plan
- Query Optimizer
- Physical Plan
- Server
- JDBC Driver  
  
The following operators are supported:

- Table Scan (Parquet and CSV)
- Projection
- Filter
- Hash Aggregate 

The following expressions are supported:

- Literals
- Attributes
- Simple Aggregates (Min, Max, Sum)
- Cast
- Boolean expressions (AND, OR, NOT)
- Simple math expressions (+, -, *, /)

## Building

```bash
cd jvm
./gradlew build
```

Run tests:
```bash
./gradlew test
```

Install to local Maven repository:
```bash
./gradlew publishToMavenLocal
``` 

## Running the Flight Server Example

The query engine includes an Arrow Flight server that allows remote query execution via gRPC.

**Start the executor (server):**
```bash
cd jvm
./gradlew :executor:run
```

This starts the Flight server listening on `0.0.0.0:50051`.

**Client usage:**

The client module provides a `Client` class that can connect to the executor and submit logical plans:

```kotlin
val client = Client("localhost", 50051)
client.execute(logicalPlan)
```

The client serializes the logical plan to protobuf, sends it to the server via Arrow Flight, and receives results as Arrow record batches.

## Running Benchmarks

The benchmark runs an aggregate SQL query against NYC taxi trip CSV files in parallel.

Build the benchmark:
```bash
cd jvm
./gradlew :benchmarks:installDist
```

Run the benchmark:
```bash
BENCH_PATH=/path/to/csv/files \
BENCH_RESULT_FILE=/path/to/results.csv \
./benchmarks/build/install/benchmarks/bin/benchmarks
```

- `BENCH_PATH` - Directory containing NYC taxi trip CSV files
- `BENCH_RESULT_FILE` - Output file for benchmark timing results

There is also a Docker-based benchmark setup in the `docker/` directory.

## Sample Data

Some of the examples in the book use the `yellow_tripdata_2019-12.csv` data set. The original data set
is no longer available from the [original location] (because the data is now, quite sensibly, provided
in Parquet format), but copies can be still be found on the internet.

[original location]: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

As of December 2025, copies can be found at the following locations:

- https://github.com/DataTalksClub/nyc-tlc-data/releases
- https://catalog.data.gov/dataset/2019-yellow-taxi-trip-data
- https://www.kaggle.com/code/haydenbailey/newyork-yellow-taxi