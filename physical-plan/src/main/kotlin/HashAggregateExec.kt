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
import io.andygrove.kquery.physical.expressions.Accumulator
import io.andygrove.kquery.physical.expressions.AggregateExpression
import io.andygrove.kquery.physical.expressions.Expression
import org.apache.arrow.vector.VectorSchemaRoot

class HashAggregateExec(
    val input: PhysicalPlan,
    val groupExpr: List<Expression>,
    val aggregateExpr: List<AggregateExpression>,
    val schema: Schema
) : PhysicalPlan {

  override fun schema(): Schema {
    return schema
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }
  // snippet-omit-start

  override fun toString(): String {
    return "HashAggregateExec: groupExpr=$groupExpr, aggrExpr=$aggregateExpr"
  }
  // snippet-omit-end

  override fun execute(): Sequence<RecordBatch> {
    val map = HashMap<List<Any?>, List<Accumulator>>()

    // Process all input batches
    input.execute().forEach { batch ->
      // snippet-omit-start
      try {
        // snippet-omit-end
        val groupKeys = groupExpr.map { it.evaluate(batch) }
        val aggrInputs =
            aggregateExpr.map { it.inputExpression().evaluate(batch) }

        // For each row, update accumulators
        (0 until batch.rowCount()).forEach { rowIndex ->
          // snippet-omit-start
          val rowKey =
              groupKeys.map {
                val value = it.getValue(rowIndex)
                when (value) {
                  is ByteArray -> String(value)
                  else -> value
                }
              }
          // snippet-omit-end
          val accumulators =
              map.getOrPut(rowKey) {
                aggregateExpr.map { it.createAccumulator() }
              }

          accumulators.withIndex().forEach { accum ->
            val value = aggrInputs[accum.index].getValue(rowIndex)
            accum.value.accumulate(value)
          }
        }
        // snippet-omit-start
      } finally {
        batch.close()
      }
      // snippet-omit-end
    }

    // Build output batch from accumulated results
    // snippet-omit-start
    val root =
        VectorSchemaRoot.create(schema.toArrow(), ArrowAllocator.rootAllocator)
    root.allocateNew()
    root.rowCount = map.size

    val builders = root.fieldVectors.map { ArrowVectorBuilder(it) }

    map.entries.withIndex().forEach { entry ->
      val rowIndex = entry.index
      val groupingKey = entry.value.key
      val accumulators = entry.value.value
      groupExpr.indices.forEach { builders[it].set(rowIndex, groupingKey[it]) }
      aggregateExpr.indices.forEach {
        builders[groupExpr.size + it].set(
            rowIndex, accumulators[it].finalValue())
      }
    }

    val outputBatch =
        RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
    return listOf(outputBatch).asSequence()
    // snippet-omit-end
  }
}
