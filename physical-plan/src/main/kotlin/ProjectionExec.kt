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
import io.andygrove.kquery.physical.expressions.Expression

/** Execute a projection. */
class ProjectionExec(
    val input: PhysicalPlan,
    val schema: Schema,
    val expr: List<Expression>
) : PhysicalPlan {

  override fun schema(): Schema {
    return schema
  }

  override fun children(): List<PhysicalPlan> {
    return listOf(input)
  }

  override fun execute(): Sequence<RecordBatch> {
    return input.execute().map { batch ->
      val columns = expr.map { it.evaluate(batch) }
      RecordBatch(schema, columns)
    }
  }
  // snippet-omit-start

  override fun toString(): String {
    return "ProjectionExec: $expr"
  }
  // snippet-omit-end
}
