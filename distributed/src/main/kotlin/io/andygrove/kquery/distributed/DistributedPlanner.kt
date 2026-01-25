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

import io.andygrove.kquery.physical.AggregateMode
import io.andygrove.kquery.physical.HashAggregateExec
import io.andygrove.kquery.physical.PhysicalPlan
import io.andygrove.kquery.physical.ShuffleLocation
import io.andygrove.kquery.physical.ShuffleReaderExec
import io.andygrove.kquery.physical.ShuffleWriterExec

/**
 * Converts a single-node physical plan into a distributed execution plan
 * consisting of multiple stages separated by shuffle boundaries.
 *
 * Currently supports two-stage aggregation:
 * - Stage 0: Partial aggregation on each partition, followed by shuffle write
 * - Stage 1: Shuffle read, followed by final aggregation
 */
class DistributedPlanner(private val config: DistributedConfig) {

  /**
   * Plan a physical plan for distributed execution.
   *
   * @param plan The physical plan to distribute
   * @param jobUuid Unique identifier for this job
   * @return List of query stages to execute
   */
  fun plan(plan: PhysicalPlan, jobUuid: String): List<QueryStage> {
    return when (plan) {
      is HashAggregateExec -> planAggregate(plan, jobUuid)
      else -> {
        // For non-aggregate plans, just wrap in a single stage
        listOf(QueryStage(stageId = 0, plan = plan, isFinalStage = true))
      }
    }
  }

  /**
   * Plan a two-stage aggregation.
   *
   * Stage 0: Scan + Partial Aggregate + Shuffle Write Stage 1: Shuffle Read +
   * Final Aggregate
   */
  private fun planAggregate(
      aggregate: HashAggregateExec,
      jobUuid: String
  ): List<QueryStage> {
    val partitionCount = config.getPartitionCount()

    // Stage 0: Partial aggregation with shuffle write
    val partialAggregate =
        HashAggregateExec(
            input = aggregate.input,
            groupExpr = aggregate.groupExpr,
            aggregateExpr = aggregate.aggregateExpr,
            schema = aggregate.schema,
            mode = AggregateMode.PARTIAL)

    val shuffleWriter =
        ShuffleWriterExec(
            input = partialAggregate,
            partitionExpr = aggregate.groupExpr, // Partition by group keys
            jobUuid = jobUuid,
            stageId = 0,
            partitionCount = partitionCount)

    val stage0 =
        QueryStage(
            stageId = 0,
            plan = shuffleWriter,
            partitionCount = partitionCount,
            isFinalStage = false)

    // Stage 1: Shuffle read with final aggregation
    // Create a placeholder shuffle reader - actual locations will be filled in
    // by the scheduler after stage 0 completes
    val shuffleReader =
        ShuffleReaderExec(
            shuffleSchema = aggregate.schema, shuffleLocations = listOf())

    val finalAggregate =
        HashAggregateExec(
            input = shuffleReader,
            groupExpr = aggregate.groupExpr,
            aggregateExpr = aggregate.aggregateExpr,
            schema = aggregate.schema,
            mode = AggregateMode.FINAL)

    val stage1 =
        QueryStage(
            stageId = 1,
            plan = finalAggregate,
            dependencies = listOf(0),
            partitionCount = 1,
            isFinalStage = true)

    return listOf(stage0, stage1)
  }

  /**
   * Update a stage's shuffle reader with actual shuffle locations. This is
   * called after the previous stage completes.
   */
  fun updateShuffleLocations(
      stage: QueryStage,
      locations: List<ShuffleLocation>
  ): QueryStage {
    val updatedPlan = updatePlanShuffleLocations(stage.plan, locations)
    return stage.copy(plan = updatedPlan)
  }

  private fun updatePlanShuffleLocations(
      plan: PhysicalPlan,
      locations: List<ShuffleLocation>
  ): PhysicalPlan {
    return when (plan) {
      is HashAggregateExec -> {
        val updatedInput = updatePlanShuffleLocations(plan.input, locations)
        HashAggregateExec(
            input = updatedInput,
            groupExpr = plan.groupExpr,
            aggregateExpr = plan.aggregateExpr,
            schema = plan.schema,
            mode = plan.mode)
      }
      is ShuffleReaderExec -> {
        ShuffleReaderExec(
            shuffleSchema = plan.schema(), shuffleLocations = locations)
      }
      else -> plan
    }
  }
}
