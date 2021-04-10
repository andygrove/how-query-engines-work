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

## Installing Locally

```bash
cd jvm
./gradlew publishToMavenLocal
``` 

## Sample Data

Some of the examples in the book use the following data set.

```bash
wget https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2019-12.csv
```