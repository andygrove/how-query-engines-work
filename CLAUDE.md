# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is the companion repository for the book "How Query Engines Work" - an educational in-memory query engine implemented in Kotlin called KQuery (also known as "Ballista JVM"). The engine is designed for learning, not production use.

## Build Commands

```bash
# Build and install to local Maven repository
cd jvm
./gradlew publishToMavenLocal

# Run all tests
cd jvm
./gradlew test

# Run tests for a specific subproject
cd jvm
./gradlew :execution:test
./gradlew :sql:test

# Run a single test class
cd jvm
./gradlew :execution:test --tests "io.andygrove.kquery.execution.ExecutionTest"

# Format code with ktfmt
cd jvm
./gradlew spotlessApply
```

**Note:** Requires protobuf compiler (protoc) to be installed for building the protobuf module.

## Architecture

The query engine follows a standard architecture with distinct phases:

### Query Flow
1. **SQL Parsing** (`sql` module): Tokenizer → Pratt parser → SQL AST (`SqlExpr`)
2. **SQL Planning** (`sql` module): SQL AST → Logical Plan
3. **Optimization** (`optimizer` module): Applies optimization rules (currently projection pushdown)
4. **Physical Planning** (`query-planner` module): Logical Plan → Physical Plan
5. **Execution** (`physical-plan` module): Physical Plan produces `Sequence<RecordBatch>`

### Key Abstractions

- **`LogicalPlan`** (`logical-plan` module): Declarative representation of queries (Scan, Selection, Projection, Aggregate)
- **`LogicalExpr`** (`logical-plan` module): Expressions in logical plans (Column, Literal, BinaryExpr, aggregates)
- **`PhysicalPlan`** (`physical-plan` module): Executable plan that produces record batches
- **`Expression`** (`physical-plan` module): Physical expressions that evaluate against record batches
- **`DataFrame`** (`logical-plan` module): Fluent API wrapping logical plans
- **`ExecutionContext`** (`execution` module): Entry point for registering tables and executing queries

### Module Dependencies
```
datatypes (Arrow types, Schema, RecordBatch, ColumnVector)
    ↓
datasource (DataSource interface, CSV/Parquet readers)
    ↓
logical-plan (LogicalPlan, LogicalExpr, DataFrame)
    ↓
sql (SqlTokenizer, SqlParser, SqlPlanner)
optimizer (ProjectionPushDownRule)
    ↓
physical-plan (PhysicalPlan, physical Expression implementations)
    ↓
query-planner (LogicalPlan → PhysicalPlan translation)
    ↓
execution (ExecutionContext - ties everything together)
```

### Data Representation
Uses Apache Arrow for in-memory columnar data:
- `RecordBatch`: Collection of `ColumnVector`s with a `Schema`
- `ColumnVector`: Interface for accessing column data (backed by Arrow vectors or literals)
- `Schema`/`Field`: Metadata describing column names and Arrow types
