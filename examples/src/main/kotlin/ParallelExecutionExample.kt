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

package io.andygrove.kquery.examples

import io.andygrove.kquery.execution.ExecutionContext
import io.andygrove.kquery.execution.ParallelContext
import java.io.File

/**
 * Example demonstrating parallel query execution.
 *
 * This example compares sequential and parallel execution of an aggregate
 * query, showing how ParallelContext automatically parallelizes aggregation
 * using multiple workers.
 *
 * The parallel execution works by:
 * 1. Distributing input batches across N workers (round-robin)
 * 2. Each worker runs a partial aggregate on its batches
 * 3. Results are merged using a final aggregate
 */
fun main() {
  val testDataDir = "../testdata"
  val employeeCsv = File(testDataDir, "employee.csv").absolutePath

  val sql =
      "SELECT state, SUM(CAST(salary AS double)) FROM employee GROUP BY state"

  println("=== Parallel Execution Example ===\n")
  println("Query: $sql\n")

  // Sequential execution
  println("--- Sequential Execution ---")
  val seqCtx = ExecutionContext(mapOf())
  seqCtx.registerCsv("employee", employeeCsv)

  val seqStart = System.currentTimeMillis()
  val seqResults = seqCtx.execute(seqCtx.sql(sql)).toList()
  val seqTime = System.currentTimeMillis() - seqStart

  println("Sequential execution completed in ${seqTime}ms")
  printResults(seqResults)
  println()

  // Parallel execution with 4 workers
  println("--- Parallel Execution (4 workers) ---")
  val parCtx = ParallelContext(parallelism = 4)
  parCtx.registerCsv("employee", employeeCsv)

  val parStart = System.currentTimeMillis()
  val parResults = parCtx.execute(parCtx.sql(sql)).toList()
  val parTime = System.currentTimeMillis() - parStart

  println("Parallel execution completed in ${parTime}ms")
  printResults(parResults)
  println()

  // Verify results match
  println("--- Verification ---")
  val seqMap = extractResults(seqResults)
  val parMap = extractResults(parResults)

  if (seqMap == parMap) {
    println("Results match between sequential and parallel execution")
  } else {
    println("WARNING: Results differ!")
    println("Sequential: $seqMap")
    println("Parallel: $parMap")
  }

  println("\n=== Example Complete ===")
}

private fun printResults(
    batches: List<io.andygrove.kquery.datatypes.RecordBatch>
) {
  batches.forEach { batch ->
    (0 until batch.rowCount()).forEach { row ->
      val state = batch.field(0).getValue(row)
      val sum = batch.field(1).getValue(row)
      val stateStr =
          when (state) {
            is ByteArray -> String(state)
            else -> state.toString()
          }
      println("  $stateStr: $sum")
    }
  }
}

private fun extractResults(
    batches: List<io.andygrove.kquery.datatypes.RecordBatch>
): Map<String, Any?> {
  val map = mutableMapOf<String, Any?>()
  batches.forEach { batch ->
    (0 until batch.rowCount()).forEach { row ->
      val key = batch.field(0).getValue(row)
      val value = batch.field(1).getValue(row)
      val keyStr =
          when (key) {
            is ByteArray -> String(key)
            else -> key.toString()
          }
      map[keyStr] = value
    }
  }
  return map
}
