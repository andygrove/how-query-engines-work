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

import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.distributed.DistributedConfig
import io.andygrove.kquery.distributed.DistributedContext
import io.andygrove.kquery.distributed.ExecutorClient
import io.andygrove.kquery.distributed.ExecutorConfig
import io.andygrove.kquery.physical.ShuffleLocation
import io.andygrove.kquery.physical.ShuffleManager
import io.andygrove.kquery.physical.ShuffleWriterExec
import io.andygrove.kquery.physical.Task
import java.io.File

/**
 * Example demonstrating distributed query execution concepts.
 *
 * This example shows how a query is split into stages and how tasks are
 * distributed across executors. For simplicity, this runs everything in a
 * single JVM but demonstrates the distributed execution model.
 *
 * To run a real distributed query, you would:
 * 1. Start multiple executor processes on different ports: java -jar
 *    executor.jar --port 50051 java -jar executor.jar --port 50052 java -jar
 *    executor.jar --port 50053
 * 2. Configure the DistributedContext with executor addresses
 * 3. Run your query: ctx.sql("SELECT state, SUM(salary) FROM employee GROUP BY
 *    state")
 */
fun main() {
  val testDataDir = "../testdata"
  val employeeCsv = File(testDataDir, "employee.csv").absolutePath

  println("=== Distributed Query Execution Example ===\n")

  // Configure a cluster with 3 executors
  val config =
      DistributedConfig(
          executors =
              listOf(
                  ExecutorConfig("exec-1", "localhost", 50051),
                  ExecutorConfig("exec-2", "localhost", 50052),
                  ExecutorConfig("exec-3", "localhost", 50053)))

  println("Configured cluster with ${config.executors.size} executors:")
  config.executors.forEach { println("  - ${it.id} at ${it.host}:${it.port}") }
  println()

  // Create a local executor client for demonstration
  // In a real deployment, this would use Arrow Flight to communicate
  val executorClient = LocalExecutorClient()

  // Create the distributed context
  val ctx = DistributedContext(config, executorClient)

  // Register our test data
  ctx.registerCsv("employee", employeeCsv)

  println("Executing: SELECT state, SUM(salary) FROM employee GROUP BY state\n")

  // Execute the query
  val results =
      ctx.sql("SELECT state, SUM(salary) FROM employee GROUP BY state")

  // Print results
  println("Results:")
  results.forEach { batch ->
    (0 until batch.rowCount()).forEach { row ->
      val state = batch.field(0).getValue(row)
      val sum = batch.field(1).getValue(row)
      println("  $state: $sum")
    }
  }

  println("\n=== Execution Complete ===")
}

/**
 * A local executor client that executes tasks in the same JVM. This is useful
 * for testing and demonstration purposes.
 */
class LocalExecutorClient : ExecutorClient {
  private val shuffleManager = ShuffleManager()

  override fun executeTask(
      executor: ExecutorConfig,
      task: Task
  ): List<ShuffleLocation> {
    println(
        "Executor ${executor.id}: Executing task ${task.taskId} (stage ${task.stageId})")

    return when (val plan = task.plan) {
      is ShuffleWriterExec -> {
        plan.executeAndWriteShuffle(
            executor.id, executor.host, executor.port, shuffleManager)
      }
      else -> {
        plan.execute().forEach { _ -> /* consume results */ }
        listOf()
      }
    }
  }

  override fun executeFinalTask(
      executor: ExecutorConfig,
      task: Task
  ): Sequence<RecordBatch> {
    println("Executor ${executor.id}: Executing final task ${task.taskId}")
    return task.plan.execute()
  }

  override fun fetchShuffle(
      executor: ExecutorConfig,
      location: ShuffleLocation
  ): Sequence<RecordBatch> {
    return shuffleManager.readPartition(
        location.jobUuid, location.stageId, location.partitionId)
  }
}
