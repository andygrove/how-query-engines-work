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
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.logical.JoinType
import org.apache.arrow.vector.VectorSchemaRoot

/**
 * Hash join physical operator.
 *
 * Builds a hash table from the right (build) side, then probes it with the left (probe) side.
 */
class HashJoinExec(
    val left: PhysicalPlan,
    val right: PhysicalPlan,
    val joinType: JoinType,
    val leftKeys: List<Int>,
    val rightKeys: List<Int>,
    val schema: Schema,
    val rightColumnsToExclude: Set<Int>
) : PhysicalPlan {

  override fun schema(): Schema = schema

  override fun children(): List<PhysicalPlan> = listOf(left, right)

  override fun toString(): String =
      "HashJoinExec: joinType=$joinType, leftKeys=$leftKeys, rightKeys=$rightKeys"

  override fun execute(): Sequence<RecordBatch> {
    // Build phase: load all right-side rows into a hash table
    val hashTable = HashMap<List<Any?>, MutableList<List<Any?>>>()

    val rightSchema = right.schema()
    right.execute().forEach { batch ->
      try {
        for (rowIndex in 0 until batch.rowCount()) {
          val key =
              rightKeys.map { keyIndex -> normalizeValue(batch.field(keyIndex).getValue(rowIndex)) }
          val row = (0 until batch.columnCount()).map { batch.field(it).getValue(rowIndex) }
          hashTable.getOrPut(key) { mutableListOf() }.add(row)
        }
      } finally {
        batch.close()
      }
    }

    // Probe phase: iterate through left side and find matches
    return sequence {
      left.execute().forEach { leftBatch ->
        try {
          val outputRows = mutableListOf<List<Any?>>()

          for (leftRowIndex in 0 until leftBatch.rowCount()) {
            val probeKey =
                leftKeys.map { keyIndex ->
                  normalizeValue(leftBatch.field(keyIndex).getValue(leftRowIndex))
                }
            val leftRow =
                (0 until leftBatch.columnCount()).map { leftBatch.field(it).getValue(leftRowIndex) }
            val matchedRows = hashTable[probeKey]

            when (joinType) {
              JoinType.Inner -> {
                if (matchedRows != null) {
                  for (rightRow in matchedRows) {
                    outputRows.add(combineRows(leftRow, rightRow))
                  }
                }
              }
              JoinType.Left -> {
                if (matchedRows != null) {
                  for (rightRow in matchedRows) {
                    outputRows.add(combineRows(leftRow, rightRow))
                  }
                } else {
                  // No match: include left row with nulls for right columns
                  val nullRightRow = List(rightSchema.fields.size) { null }
                  outputRows.add(combineRows(leftRow, nullRightRow))
                }
              }
              JoinType.Right -> {
                // Right join is handled after probe phase
                if (matchedRows != null) {
                  for (rightRow in matchedRows) {
                    outputRows.add(combineRows(leftRow, rightRow))
                  }
                }
              }
            }
          }

          if (outputRows.isNotEmpty()) {
            yield(createBatch(outputRows))
          }
        } finally {
          leftBatch.close()
        }
      }

      // For right join, emit unmatched right rows
      if (joinType == JoinType.Right) {
        // Track which right keys were matched during probe
        val matchedRightKeys = mutableSetOf<List<Any?>>()

        // Re-probe to find matched keys (simplified approach)
        left.execute().forEach { leftBatch ->
          try {
            for (leftRowIndex in 0 until leftBatch.rowCount()) {
              val probeKey =
                  leftKeys.map { keyIndex ->
                    normalizeValue(leftBatch.field(keyIndex).getValue(leftRowIndex))
                  }
              if (hashTable.containsKey(probeKey)) {
                matchedRightKeys.add(probeKey)
              }
            }
          } finally {
            leftBatch.close()
          }
        }

        val unmatchedRows = mutableListOf<List<Any?>>()
        val leftSchema = left.schema()
        for ((key, rows) in hashTable) {
          if (!matchedRightKeys.contains(key)) {
            val nullLeftRow = List(leftSchema.fields.size) { null }
            for (rightRow in rows) {
              unmatchedRows.add(combineRows(nullLeftRow, rightRow))
            }
          }
        }

        if (unmatchedRows.isNotEmpty()) {
          yield(createBatch(unmatchedRows))
        }
      }
    }
  }

  /** Normalize values for hash key comparison (ByteArray to String for consistency) */
  private fun normalizeValue(value: Any?): Any? {
    return when (value) {
      is ByteArray -> String(value)
      else -> value
    }
  }

  /** Combine left and right rows, excluding duplicate join keys from right side */
  private fun combineRows(leftRow: List<Any?>, rightRow: List<Any?>): List<Any?> {
    val result = mutableListOf<Any?>()
    result.addAll(leftRow)

    // Add right columns, skipping those that should be excluded (duplicate keys)
    for (i in rightRow.indices) {
      if (!rightColumnsToExclude.contains(i)) {
        result.add(rightRow[i])
      }
    }
    return result
  }

  private fun createBatch(rows: List<List<Any?>>): RecordBatch {
    val root = VectorSchemaRoot.create(schema.toArrow(), ArrowAllocator.rootAllocator)
    root.allocateNew()
    root.rowCount = rows.size

    val builders = root.fieldVectors.map { ArrowVectorBuilder(it) }

    rows.forEachIndexed { rowIndex, row ->
      row.forEachIndexed { colIndex, value -> builders[colIndex].set(rowIndex, value) }
    }

    return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
  }
}
