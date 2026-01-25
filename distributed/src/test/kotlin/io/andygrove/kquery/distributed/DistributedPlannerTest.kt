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
import io.andygrove.kquery.logical.Aggregate
import io.andygrove.kquery.logical.Column
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.logical.Sum
import io.andygrove.kquery.optimizer.Optimizer
import io.andygrove.kquery.physical.AggregateMode
import io.andygrove.kquery.physical.HashAggregateExec
import io.andygrove.kquery.physical.ShuffleWriterExec
import io.andygrove.kquery.planner.QueryPlanner
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class DistributedPlannerTest {

  private val testDataDir = "../testdata"

  private val config =
      DistributedConfig(
          executors =
              listOf(
                  ExecutorConfig("exec-1", "localhost", 50051),
                  ExecutorConfig("exec-2", "localhost", 50052),
                  ExecutorConfig("exec-3", "localhost", 50053)))

  @Test
  fun `plan aggregate query into two stages`() {
    // Create a logical plan for: SELECT state, SUM(salary) FROM employee GROUP BY state
    val employeeCsv = File(testDataDir, "employee.csv").absolutePath
    val ds = CsvDataSource(employeeCsv, null, true, 1024)
    val scan = Scan(employeeCsv, ds, listOf())
    val aggregate =
        Aggregate(scan, listOf(Column("state")), listOf(Sum(Column("salary"))))

    // Optimize and create physical plan
    val optimizedPlan = Optimizer().optimize(aggregate)
    val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)

    // Plan for distributed execution
    val planner = DistributedPlanner(config)
    val stages = planner.plan(physicalPlan, "test-job-123")

    // Should produce 2 stages
    assertEquals(2, stages.size)

    // Stage 0: Partial aggregate with shuffle write
    val stage0 = stages[0]
    assertEquals(0, stage0.stageId)
    assertEquals(3, stage0.partitionCount) // 3 executors
    assertEquals(false, stage0.isFinalStage)
    assertIs<ShuffleWriterExec>(stage0.plan)

    val shuffleWriter = stage0.plan as ShuffleWriterExec
    assertIs<HashAggregateExec>(shuffleWriter.input)
    val partialAgg = shuffleWriter.input as HashAggregateExec
    assertEquals(AggregateMode.PARTIAL, partialAgg.mode)

    // Stage 1: Final aggregate
    val stage1 = stages[1]
    assertEquals(1, stage1.stageId)
    assertEquals(listOf(0), stage1.dependencies)
    assertEquals(true, stage1.isFinalStage)
    assertIs<HashAggregateExec>(stage1.plan)

    val finalAgg = stage1.plan as HashAggregateExec
    assertEquals(AggregateMode.FINAL, finalAgg.mode)
  }

  @Test
  fun `non-aggregate query produces single stage`() {
    // Create a simple scan plan
    val employeeCsv = File(testDataDir, "employee.csv").absolutePath
    val ds = CsvDataSource(employeeCsv, null, true, 1024)
    val scan = Scan(employeeCsv, ds, listOf())

    // Create physical plan
    val physicalPlan = QueryPlanner().createPhysicalPlan(scan)

    // Plan for distributed execution
    val planner = DistributedPlanner(config)
    val stages = planner.plan(physicalPlan, "test-job-456")

    // Should produce 1 stage
    assertEquals(1, stages.size)
    assertEquals(0, stages[0].stageId)
    assertEquals(true, stages[0].isFinalStage)
  }
}
