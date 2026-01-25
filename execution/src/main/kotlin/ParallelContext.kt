// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.andygrove.kquery.execution

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.datasource.DataSource
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.logical.DataFrame
import io.andygrove.kquery.logical.DataFrameImpl
import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.optimizer.Optimizer
import io.andygrove.kquery.physical.AggregateMode
import io.andygrove.kquery.physical.HashAggregateExec
import io.andygrove.kquery.physical.PhysicalPlan
import io.andygrove.kquery.planner.QueryPlanner
import io.andygrove.kquery.sql.SqlParser
import io.andygrove.kquery.sql.SqlPlanner
import io.andygrove.kquery.sql.SqlSelect
import io.andygrove.kquery.sql.SqlTokenizer
import java.util.concurrent.ConcurrentLinkedQueue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking

/**
 * Execution context with parallel processing support.
 *
 * For aggregate queries, this context uses multiple workers to process data in
 * parallel:
 * 1. Input batches are distributed across workers using round-robin
 * 2. Each worker runs a partial aggregate on its batches
 * 3. Results are merged using a final aggregate
 *
 * @param parallelism Number of parallel workers (defaults to available
 *   processors)
 * @param settings Additional configuration settings
 */
class ParallelContext(
    val parallelism: Int = Runtime.getRuntime().availableProcessors(),
    val settings: Map<String, String> = mapOf()
) {

  val batchSize: Int =
      settings.getOrDefault("kquery.csv.batchSize", "1024").toInt()

  private val tables = mutableMapOf<String, DataFrame>()

  /** Create a DataFrame for the given SQL Select */
  fun sql(sql: String): DataFrame {
    val tokens = SqlTokenizer(sql).tokenize()
    val ast = SqlParser(tokens).parse() as SqlSelect
    val df = SqlPlanner().createDataFrame(ast, tables)
    return DataFrameImpl(df.logicalPlan())
  }

  /** Get a DataFrame representing the specified CSV file */
  fun csv(filename: String): DataFrame {
    return DataFrameImpl(
        Scan(
            filename, CsvDataSource(filename, null, true, batchSize), listOf()))
  }

  /** Register a DataFrame with the context */
  fun register(tablename: String, df: DataFrame) {
    tables[tablename] = df
  }

  /** Register a data source with the context */
  fun registerDataSource(tablename: String, datasource: DataSource) {
    register(tablename, DataFrameImpl(Scan(tablename, datasource, listOf())))
  }

  /** Register a CSV data source with the context */
  fun registerCsv(tablename: String, filename: String) {
    register(tablename, csv(filename))
  }

  /** Execute the logical plan represented by a DataFrame */
  fun execute(df: DataFrame): Sequence<RecordBatch> {
    return execute(df.logicalPlan())
  }

  /** Execute the provided logical plan with parallel processing */
  fun execute(plan: LogicalPlan): Sequence<RecordBatch> {
    val optimizedPlan = Optimizer().optimize(plan)
    val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)
    return executeParallel(physicalPlan)
  }
  /**
   * Execute a physical plan with parallel processing where applicable.
   *
   * For HashAggregateExec, this will:
   * 1. Run the input plan and distribute batches to N workers
   * 2. Each worker runs a partial aggregate
   * 3. Results are merged with a final aggregate
   */
  private fun executeParallel(plan: PhysicalPlan): Sequence<RecordBatch> {
    return when (plan) {
      is HashAggregateExec -> executeParallelAggregate(plan)
      else -> plan.execute()
    }
  }

  private fun executeParallelAggregate(
      aggregate: HashAggregateExec
  ): Sequence<RecordBatch> {
    // If parallelism is 1, just run normally
    if (parallelism <= 1) {
      return aggregate.execute()
    }

    // Create queues for each worker to receive batches
    val workerQueues =
        (0 until parallelism).map { ConcurrentLinkedQueue<RecordBatch>() }

    // Collect all input batches and distribute round-robin
    val inputBatches = aggregate.input.execute().toList()
    inputBatches.forEachIndexed { index, batch ->
      workerQueues[index % parallelism].add(batch)
    }

    // Run partial aggregates in parallel
    val partialResults =
        runBlocking(Dispatchers.Default) {
          val deferredResults =
              workerQueues.map { queue ->
                async {
                  if (queue.isEmpty()) {
                    emptyList()
                  } else {
                    executePartialAggregate(aggregate, queue.toList())
                  }
                }
              }
          deferredResults.flatMap { it.await() }
        }

    // If no results, return empty
    if (partialResults.isEmpty()) {
      return emptySequence()
    }

    // Run final aggregate to merge partial results
    return executeFinalAggregate(aggregate, partialResults)
  }
  /** Execute a partial aggregate on a subset of batches. */
  private fun executePartialAggregate(
      aggregate: HashAggregateExec,
      batches: List<RecordBatch>
  ): List<RecordBatch> {
    // Create a partial aggregate with the batches as input
    val partialAggregate =
        HashAggregateExec(
            input = InMemoryPlan(aggregate.input.schema(), batches),
            groupExpr = aggregate.groupExpr,
            aggregateExpr = aggregate.aggregateExpr,
            schema = aggregate.schema,
            mode = AggregateMode.PARTIAL)

    return partialAggregate.execute().toList()
  }

  /** Execute a final aggregate to merge partial results. */
  private fun executeFinalAggregate(
      aggregate: HashAggregateExec,
      partialBatches: List<RecordBatch>
  ): Sequence<RecordBatch> {
    val finalAggregate =
        HashAggregateExec(
            input = InMemoryPlan(aggregate.schema, partialBatches),
            groupExpr = aggregate.groupExpr,
            aggregateExpr = aggregate.aggregateExpr,
            schema = aggregate.schema,
            mode = AggregateMode.FINAL)

    return finalAggregate.execute()
  }
}

/**
 * Simple in-memory physical plan that yields pre-loaded batches. Used to feed
 * batches into partial/final aggregates.
 */
private class InMemoryPlan(
    private val planSchema: io.andygrove.kquery.datatypes.Schema,
    private val batches: List<RecordBatch>
) : PhysicalPlan {

  override fun schema(): io.andygrove.kquery.datatypes.Schema = planSchema

  override fun children(): List<PhysicalPlan> = listOf()

  override fun execute(): Sequence<RecordBatch> = batches.asSequence()

  override fun toString(): String = "InMemoryPlan: ${batches.size} batches"
}
