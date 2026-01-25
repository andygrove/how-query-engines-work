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

import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.physical.PhysicalPlan
import io.andygrove.kquery.physical.ShuffleLocation
import io.andygrove.kquery.physical.Task
import java.util.UUID
import java.util.logging.Logger

/** Interface for sending tasks to executors and receiving results. */
interface ExecutorClient {
  /**
   * Execute a task on the specified executor.
   *
   * @return List of shuffle locations produced by the task
   */
  fun executeTask(executor: ExecutorConfig, task: Task): List<ShuffleLocation>

  /** Execute a final stage task and return the result batches. */
  fun executeFinalTask(
      executor: ExecutorConfig,
      task: Task
  ): Sequence<RecordBatch>

  /** Fetch shuffle data from an executor. */
  fun fetchShuffle(
      executor: ExecutorConfig,
      location: ShuffleLocation
  ): Sequence<RecordBatch>
}

/** Schedules and coordinates distributed query execution across executors. */
class Scheduler(
    private val config: DistributedConfig,
    private val planner: DistributedPlanner,
    private val executorClient: ExecutorClient
) {
  private val logger = Logger.getLogger(Scheduler::class.java.name)

  /**
   * Execute a physical plan and return the results.
   *
   * @param plan The physical plan to execute
   * @return Sequence of result batches
   */
  fun execute(plan: PhysicalPlan): Sequence<RecordBatch> {
    val jobUuid = UUID.randomUUID().toString()
    logger.info("Starting job $jobUuid")

    // Plan the query into stages
    val stages = planner.plan(plan, jobUuid)
    logger.info("Job $jobUuid has ${stages.size} stages")

    // Track shuffle locations from each stage
    val shuffleLocationsByStage = mutableMapOf<Int, List<ShuffleLocation>>()

    // Execute stages in dependency order
    for (stage in stages.sortedBy { it.stageId }) {
      logger.info(
          "Executing stage ${stage.stageId} (final=${stage.isFinalStage})")

      // Wait for dependencies
      for (depId in stage.dependencies) {
        if (!shuffleLocationsByStage.containsKey(depId)) {
          throw IllegalStateException(
              "Stage ${stage.stageId} depends on stage $depId which hasn't completed")
        }
      }

      // Get shuffle locations from dependencies
      val inputLocations =
          stage.dependencies.flatMap { shuffleLocationsByStage[it] ?: listOf() }

      // Update stage plan with shuffle locations if needed
      val updatedStage =
          if (inputLocations.isNotEmpty()) {
            planner.updateShuffleLocations(stage, inputLocations)
          } else {
            stage
          }

      if (stage.isFinalStage) {
        // For final stage, execute and return results
        return executeFinalStage(jobUuid, updatedStage)
      } else {
        // For intermediate stages, execute and collect shuffle locations
        val locations = executeStage(jobUuid, updatedStage)
        shuffleLocationsByStage[stage.stageId] = locations
        logger.info(
            "Stage ${stage.stageId} produced ${locations.size} shuffle locations")
      }
    }

    // Should not reach here
    return emptySequence()
  }

  /** Execute an intermediate stage and return shuffle locations. */
  private fun executeStage(
      jobUuid: String,
      stage: QueryStage
  ): List<ShuffleLocation> {
    val allLocations = mutableListOf<ShuffleLocation>()

    // Create tasks - one per partition
    val tasks = createTasks(jobUuid, stage)

    // Assign tasks to executors (round-robin)
    tasks.forEachIndexed { index, task ->
      val executor = config.executors[index % config.executors.size]
      logger.info("Assigning task ${task.taskId} to executor ${executor.id}")

      val locations = executorClient.executeTask(executor, task)
      allLocations.addAll(locations)
    }

    return allLocations
  }

  /** Execute the final stage and return results. */
  private fun executeFinalStage(
      jobUuid: String,
      stage: QueryStage
  ): Sequence<RecordBatch> {
    // For final stage, we run a single task on one executor
    val task =
        Task(
            jobUuid = jobUuid,
            stageId = stage.stageId,
            taskId = 0,
            partitionId = 0,
            plan = stage.plan)

    // Use first executor for final stage
    val executor = config.executors.first()
    logger.info("Executing final stage on executor ${executor.id}")

    return executorClient.executeFinalTask(executor, task)
  }

  /** Create tasks for a stage. */
  private fun createTasks(jobUuid: String, stage: QueryStage): List<Task> {
    return (0 until stage.partitionCount).map { partitionId ->
      Task(
          jobUuid = jobUuid,
          stageId = stage.stageId,
          taskId = partitionId,
          partitionId = partitionId,
          plan = stage.plan)
    }
  }
}
