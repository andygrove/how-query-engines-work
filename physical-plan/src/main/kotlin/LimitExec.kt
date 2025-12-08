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

/** Execute a limit operation. */
class LimitExec(val input: PhysicalPlan, val limit: Int) : PhysicalPlan {

  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> = sequence {
    var remaining = limit
    for (batch in input.execute()) {
      if (remaining <= 0) {
        batch.close()
        break
      }

      if (batch.rowCount() <= remaining) {
        remaining -= batch.rowCount()
        yield(batch)
      } else {
        // Need to truncate this batch
        val schema = batch.schema
        val truncatedFields =
            (0 until schema.fields.size).map { i ->
              val sourceVector = batch.field(i)
              val truncatedVector = FieldVectorFactory.create(sourceVector.getType(), remaining)
              val builder = ArrowVectorBuilder(truncatedVector)
              (0 until remaining).forEach { row -> builder.set(row, sourceVector.getValue(row)) }
              truncatedVector.valueCount = remaining
              ArrowFieldVector(truncatedVector)
            }
        batch.close()
        remaining = 0
        yield(RecordBatch(schema, truncatedFields))
      }
    }
  }

  override fun toString(): String {
    return "LimitExec: limit=$limit"
  }
}
