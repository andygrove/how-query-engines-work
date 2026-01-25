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
import io.andygrove.kquery.datatypes.ColumnVector
import io.andygrove.kquery.datatypes.FieldVectorFactory
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.physical.expressions.Expression
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector

/** Execute a selection. */
class SelectionExec(val input: PhysicalPlan, val expr: Expression) :
    PhysicalPlan {

  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    val input = input.execute()
    return input.map { batch ->
      try {
        val result =
            (expr.evaluate(batch) as ArrowFieldVector).field as BitVector
        val columnCount = batch.schema.fields.size
        val filteredFields =
            (0 until columnCount).map { filter(batch.field(it), result) }
        val fields = filteredFields.map { ArrowFieldVector(it) }
        RecordBatch(batch.schema, fields)
      } finally {
        batch.close()
      }
    }
  }

  private fun filter(v: ColumnVector, selection: BitVector): FieldVector {
    // Count selected rows first to allocate correct capacity
    var count = 0
    (0 until selection.valueCount).forEach {
      if (selection.get(it) == 1) {
        count++
      }
    }

    // Create vector of the same type as input
    val filteredVector = FieldVectorFactory.create(v.getType(), count)
    val builder = ArrowVectorBuilder(filteredVector)

    var index = 0
    (0 until selection.valueCount).forEach {
      if (selection.get(it) == 1) {
        builder.set(index, v.getValue(it))
        index++
      }
    }
    filteredVector.valueCount = count
    return filteredVector
  }
}
