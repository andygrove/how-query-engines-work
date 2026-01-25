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

import io.andygrove.kquery.datatypes.ArrowFieldVector
import io.andygrove.kquery.datatypes.ArrowVectorBuilder
import io.andygrove.kquery.datatypes.FieldVectorFactory
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.physical.expressions.Expression

/**
 * Executes the input plan and writes shuffle output partitioned by hash of
 * specified columns.
 *
 * This operator is used at shuffle boundaries in distributed execution. It:
 * 1. Executes the input plan
 * 2. Partitions output rows by hash of the partition expressions
 * 3. Writes each partition to local Arrow IPC files
 * 4. Returns metadata about the shuffle locations
 */
class ShuffleWriterExec(
    val input: PhysicalPlan,
    val partitionExpr: List<Expression>,
    val jobUuid: String,
    val stageId: Int,
    val partitionCount: Int
) : PhysicalPlan {

  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    // Standard execute() is not supported for shuffle writer
    // Use executeAndWriteShuffle() instead
    throw UnsupportedOperationException(
        "ShuffleWriterExec.execute() must be called via the distributed executor. " +
            "Use executeAndWriteShuffle() instead.")
  }

  /**
   * Execute the shuffle write operation and return shuffle locations. This is
   * called by the distributed executor instead of execute().
   */
  fun executeAndWriteShuffle(
      executorId: String,
      executorHost: String,
      executorPort: Int,
      shuffleManager: ShuffleManager
  ): List<ShuffleLocation> {
    // Partition data by hash of partition expressions
    val partitionedData = mutableMapOf<Int, MutableList<RecordBatch>>()

    // Process all input batches
    input.execute().forEach { batch ->
      try {
        // Evaluate partition expressions
        val partitionVectors = partitionExpr.map { it.evaluate(batch) }

        // Group rows by partition - we'll create separate batches per partition
        val rowsByPartition = mutableMapOf<Int, MutableList<Int>>()

        for (rowIdx in 0 until batch.rowCount()) {
          // Compute hash of partition key values for this row
          val hashCode =
              partitionVectors
                  .map { vec ->
                    val value = vec.getValue(rowIdx)
                    value?.hashCode() ?: 0
                  }
                  .fold(0) { acc, hash -> 31 * acc + hash }

          val partition = Math.floorMod(hashCode, partitionCount)
          rowsByPartition.getOrPut(partition) { mutableListOf() }.add(rowIdx)
        }

        // Create a batch for each partition with the selected rows
        for ((partition, rowIndices) in rowsByPartition) {
          val partitionBatch = extractRows(batch, rowIndices)
          partitionedData
              .getOrPut(partition) { mutableListOf() }
              .add(partitionBatch)
        }
      } finally {
        batch.close()
      }
    }

    // Write each partition to shuffle files and return locations
    val locations = mutableListOf<ShuffleLocation>()

    for ((partitionId, batches) in partitionedData) {
      shuffleManager.writePartition(jobUuid, stageId, partitionId, batches)
      locations.add(
          ShuffleLocation(
              jobUuid,
              stageId,
              partitionId,
              executorId,
              executorHost,
              executorPort))
    }

    return locations
  }

  /** Extract specific rows from a batch to create a new smaller batch. */
  private fun extractRows(
      batch: RecordBatch,
      rowIndices: List<Int>
  ): RecordBatch {
    val schema = batch.schema
    val columns =
        schema.fields.mapIndexed { colIdx, field ->
          val sourceVector = batch.field(colIdx)
          val targetVector =
              FieldVectorFactory.create(field.dataType, rowIndices.size)
          val builder = ArrowVectorBuilder(targetVector)

          rowIndices.forEachIndexed { newIdx, oldIdx ->
            builder.set(newIdx, sourceVector.getValue(oldIdx))
          }
          targetVector.valueCount = rowIndices.size
          ArrowFieldVector(targetVector)
        }

    return RecordBatch(schema, columns)
  }

  override fun toString(): String {
    return "ShuffleWriterExec: jobUuid=$jobUuid, stageId=$stageId, " +
        "partitionCount=$partitionCount, partitionExpr=$partitionExpr"
  }
}
