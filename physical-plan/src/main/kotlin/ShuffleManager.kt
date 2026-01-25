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

import io.andygrove.kquery.datatypes.ArrowAllocator
import io.andygrove.kquery.datatypes.ArrowFieldVector
import io.andygrove.kquery.datatypes.ArrowVectorBuilder
import io.andygrove.kquery.datatypes.FieldVectorFactory
import io.andygrove.kquery.datatypes.RecordBatch
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter

/**
 * Manages local shuffle data storage for distributed query execution.
 *
 * Shuffle data is organized as:
 * {baseDir}/{jobUuid}/{stageId}/partition_{partitionId}.arrow
 */
class ShuffleManager(private val baseDir: String = "/tmp/kquery-shuffle") {

  /**
   * Write a partition's data to local shuffle storage using Arrow IPC format.
   */
  fun writePartition(
      jobUuid: String,
      stageId: Int,
      partitionId: Int,
      batches: List<RecordBatch>
  ) {
    if (batches.isEmpty()) {
      return
    }

    val dir = getPartitionDir(jobUuid, stageId)
    dir.mkdirs()
    val file = File(dir, "partition_${partitionId}.arrow")

    // Get schema from first batch
    val schema = batches.first().schema

    // Create Arrow schema and VectorSchemaRoot
    val arrowSchema = schema.toArrow()
    val root =
        VectorSchemaRoot.create(arrowSchema, ArrowAllocator.rootAllocator)

    FileOutputStream(file).use { fos ->
      ArrowFileWriter(root, null, fos.channel).use { writer ->
        writer.start()

        // Write each batch
        for (batch in batches) {
          // Allocate vectors for this batch
          root.allocateNew()
          root.rowCount = batch.rowCount()

          // Copy data from RecordBatch to VectorSchemaRoot
          for (colIdx in 0 until batch.schema.fields.size) {
            val sourceVector = batch.field(colIdx)
            val targetVector = root.fieldVectors[colIdx]
            val builder = ArrowVectorBuilder(targetVector)

            for (rowIdx in 0 until batch.rowCount()) {
              builder.set(rowIdx, sourceVector.getValue(rowIdx))
            }
            targetVector.valueCount = batch.rowCount()
          }

          writer.writeBatch()
          root.clear()
        }

        writer.end()
      }
    }
  }

  /** Read a partition's data from local shuffle storage. */
  fun readPartition(
      jobUuid: String,
      stageId: Int,
      partitionId: Int
  ): Sequence<RecordBatch> {
    val file = getPartitionFile(jobUuid, stageId, partitionId)
    if (!file.exists()) {
      throw IllegalStateException(
          "Shuffle file not found: ${file.absolutePath}")
    }

    return sequence {
      FileInputStream(file).use { fis ->
        ArrowFileReader(fis.channel, ArrowAllocator.rootAllocator).use { reader
          ->
          val root = reader.vectorSchemaRoot

          while (reader.loadNextBatch()) {
            // Convert VectorSchemaRoot to RecordBatch
            val schema =
                io.andygrove.kquery.datatypes.SchemaConverter.fromArrow(
                    root.schema)
            val columns =
                root.fieldVectors.map { vec ->
                  // Create a copy of the vector data since the reader reuses vectors
                  val copy =
                      FieldVectorFactory.create(vec.field.type, vec.valueCount)
                  val builder = ArrowVectorBuilder(copy)
                  for (i in 0 until vec.valueCount) {
                    val value =
                        when (vec) {
                          is org.apache.arrow.vector.VarCharVector -> vec.get(i)
                          is org.apache.arrow.vector.IntVector ->
                              if (vec.isNull(i)) null else vec.get(i)
                          is org.apache.arrow.vector.BigIntVector ->
                              if (vec.isNull(i)) null else vec.get(i)
                          is org.apache.arrow.vector.Float4Vector ->
                              if (vec.isNull(i)) null else vec.get(i)
                          is org.apache.arrow.vector.Float8Vector ->
                              if (vec.isNull(i)) null else vec.get(i)
                          is org.apache.arrow.vector.TinyIntVector ->
                              if (vec.isNull(i)) null else vec.get(i)
                          is org.apache.arrow.vector.SmallIntVector ->
                              if (vec.isNull(i)) null else vec.get(i)
                          is org.apache.arrow.vector.BitVector ->
                              if (vec.isNull(i)) null else vec.get(i) == 1
                          else ->
                              throw IllegalStateException(
                                  "Unsupported vector type: ${vec.javaClass}")
                        }
                    builder.set(i, value)
                  }
                  copy.valueCount = vec.valueCount
                  ArrowFieldVector(copy)
                }

            yield(RecordBatch(schema, columns))
          }
        }
      }
    }
  }

  /** Get the file path for a shuffle partition. */
  fun getPartitionFile(jobUuid: String, stageId: Int, partitionId: Int): File {
    return File(
        getPartitionDir(jobUuid, stageId), "partition_${partitionId}.arrow")
  }

  /** Get the directory for a job/stage. */
  private fun getPartitionDir(jobUuid: String, stageId: Int): File {
    return File(baseDir, "$jobUuid/$stageId")
  }

  /** Clean up all shuffle data for a job. */
  fun cleanupJob(jobUuid: String) {
    val jobDir = File(baseDir, jobUuid)
    if (jobDir.exists()) {
      jobDir.deleteRecursively()
    }
  }

  /** Clean up all shuffle data. */
  fun cleanupAll() {
    val dir = File(baseDir)
    if (dir.exists()) {
      dir.deleteRecursively()
    }
  }
}
