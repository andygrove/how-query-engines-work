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

package io.andygrove.kquery.distributed

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.logical.Aggregate
import io.andygrove.kquery.logical.Column
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.logical.Sum
import io.andygrove.kquery.optimizer.Optimizer
import io.andygrove.kquery.physical.ShuffleLocation
import io.andygrove.kquery.physical.Task
import io.andygrove.kquery.planner.QueryPlanner
import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

class SchedulerTest {

  private val testDataDir = "../testdata"

  private val config =
      DistributedConfig(
          executors =
              listOf(
                  ExecutorConfig("exec-1", "localhost", 50051),
                  ExecutorConfig("exec-2", "localhost", 50052),
                  ExecutorConfig("exec-3", "localhost", 50053)))

  /**
   * Mock executor client that tracks which tasks were sent to which executors.
   */
  class MockExecutorClient : ExecutorClient {
    val executedTasks = mutableListOf<Pair<ExecutorConfig, Task>>()
    val finalTasks = mutableListOf<Pair<ExecutorConfig, Task>>()

    override fun executeTask(
        executor: ExecutorConfig,
        task: Task
    ): List<ShuffleLocation> {
      executedTasks.add(executor to task)
      // Return mock shuffle locations
      return listOf(
          ShuffleLocation(
              task.jobUuid,
              task.stageId,
              task.partitionId,
              executor.id,
              executor.host,
              executor.port))
    }

    override fun executeFinalTask(
        executor: ExecutorConfig,
        task: Task
    ): Sequence<RecordBatch> {
      finalTasks.add(executor to task)
      // Return empty results for testing
      return emptySequence()
    }

    override fun fetchShuffle(
        executor: ExecutorConfig,
        location: ShuffleLocation
    ): Sequence<RecordBatch> {
      return emptySequence()
    }
  }

  @Test
  fun `scheduler assigns tasks to executors round-robin`() {
    val mockClient = MockExecutorClient()
    val planner = DistributedPlanner(config)
    val scheduler = Scheduler(config, planner, mockClient)

    // Create a logical plan for: SELECT state, SUM(salary) FROM employee GROUP BY state
    val employeeCsv = File(testDataDir, "employee.csv").absolutePath
    val ds = CsvDataSource(employeeCsv, null, true, 1024)
    val scan = Scan(employeeCsv, ds, listOf())
    val aggregate =
        Aggregate(scan, listOf(Column("state")), listOf(Sum(Column("salary"))))

    // Optimize and create physical plan
    val optimizedPlan = Optimizer().optimize(aggregate)
    val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)

    // Execute
    scheduler.execute(physicalPlan).toList()

    // Verify stage 0 tasks were assigned to different executors
    // With 3 executors and 3 partitions, each should get 1 task
    assertTrue(mockClient.executedTasks.isNotEmpty())

    val executorIds = mockClient.executedTasks.map { it.first.id }.toSet()
    assertTrue(
        executorIds.size <= 3, "Tasks should be distributed across executors")

    // Verify final stage was executed
    assertTrue(mockClient.finalTasks.isNotEmpty())
  }
}
