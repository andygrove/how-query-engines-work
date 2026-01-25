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

package io.andygrove.kquery.physical

import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema

/**
 * Reads shuffle data from one or more shuffle locations.
 *
 * This operator is used at the start of a stage that consumes shuffle output
 * from a previous stage. It:
 * 1. Reads from local files if the data is on this executor
 * 2. Fetches via Arrow Flight from remote executors if needed
 * 3. Concatenates all batches into a single sequence
 */
class ShuffleReaderExec(
    private val shuffleSchema: Schema,
    val shuffleLocations: List<ShuffleLocation>
) : PhysicalPlan {

  override fun schema(): Schema {
    return shuffleSchema
  }

  override fun children(): List<PhysicalPlan> {
    return listOf()
  }

  override fun execute(): Sequence<RecordBatch> {
    // Standard execute() requires context for local vs remote reads
    throw UnsupportedOperationException(
        "ShuffleReaderExec.execute() must be called via the distributed executor. " +
            "Use executeWithContext() instead.")
  }

  /**
   * Execute the shuffle read operation with execution context.
   *
   * @param localExecutorId The ID of the current executor, used to determine if
   *   data can be read locally
   * @param shuffleManager The shuffle manager for local file reads
   * @param flightClientFactory Optional factory to create Flight clients for
   *   remote reads. Takes (host, port) and returns a sequence of batches.
   */
  fun executeWithContext(
      localExecutorId: String,
      shuffleManager: ShuffleManager,
      flightClientFactory:
          ((String, Int, ShuffleLocation) -> Sequence<RecordBatch>)? =
          null
  ): Sequence<RecordBatch> {
    return sequence {
      for (location in shuffleLocations) {
        if (location.executorId == localExecutorId) {
          // Read from local shuffle files
          val batches =
              shuffleManager.readPartition(
                  location.jobUuid, location.stageId, location.partitionId)
          yieldAll(batches)
        } else {
          // Fetch from remote executor via Arrow Flight
          if (flightClientFactory == null) {
            throw IllegalStateException(
                "No Flight client factory available for remote shuffle read from " +
                    "${location.executorHost}:${location.executorPort}")
          }
          val batches =
              flightClientFactory(
                  location.executorHost, location.executorPort, location)
          yieldAll(batches)
        }
      }
    }
  }

  override fun toString(): String {
    return "ShuffleReaderExec: schema=$shuffleSchema, locations=${shuffleLocations.size}"
  }
}
