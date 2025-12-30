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

package io.andygrove.kquery.physical.expressions

import io.andygrove.kquery.datatypes.ArrowTypes
import io.andygrove.kquery.datatypes.ColumnVector
import io.andygrove.kquery.datatypes.RecordBatch
import java.math.BigDecimal
import org.apache.arrow.vector.types.pojo.ArrowType

/**
 * For binary expressions we need to evaluate the left and right input
 * expressions and then evaluate the specific binary operator against those
 * input values, so we can use this base class to simplify the implementation
 * for each operator.
 */
abstract class BinaryExpression(val l: Expression, val r: Expression) :
    Expression {
  override fun evaluate(input: RecordBatch): ColumnVector {
    val ll = l.evaluate(input)
    val rr = r.evaluate(input)
    assert(ll.size() == rr.size())
    // snippet-omit-start
    if (ll.getType() != rr.getType()) {
      // Attempt type coercion for numeric types
      val (coercedL, coercedR) = coerceTypes(ll, rr)
      return evaluate(coercedL, coercedR)
    }
    // snippet-omit-end
    return evaluate(ll, rr)
  }
  // snippet-omit-start

  private fun coerceTypes(
      ll: ColumnVector,
      rr: ColumnVector
  ): Pair<ColumnVector, ColumnVector> {
    val leftType = ll.getType()
    val rightType = rr.getType()

    // If both are numeric, coerce to Double
    if (isNumeric(leftType) && isNumeric(rightType)) {
      return Pair(coerceToDouble(ll), coerceToDouble(rr))
    }

    throw IllegalStateException(
        "Binary expression operands do not have the same type and cannot be coerced: " +
            "$leftType != $rightType")
  }

  private fun isNumeric(type: ArrowType): Boolean {
    return type is ArrowType.Int ||
        type is ArrowType.FloatingPoint ||
        type is ArrowType.Decimal
  }

  private fun coerceToDouble(col: ColumnVector): ColumnVector {
    if (col.getType() == ArrowTypes.DoubleType) {
      return col
    }
    return CoercedDoubleVector(col)
  }
  // snippet-omit-end

  abstract fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector
}
/** A column vector that coerces values to Double on access. */
private class CoercedDoubleVector(private val inner: ColumnVector) :
    ColumnVector {
  override fun getType(): ArrowType = ArrowTypes.DoubleType

  override fun getValue(i: Int): Any? {
    val value = inner.getValue(i) ?: return null
    return when (value) {
      is Double -> value
      is Float -> value.toDouble()
      is Long -> value.toDouble()
      is Int -> value.toDouble()
      is Short -> value.toDouble()
      is Byte -> value.toDouble()
      is BigDecimal -> value.toDouble()
      is Number -> value.toDouble()
      else ->
          throw IllegalStateException(
              "Cannot coerce ${value.javaClass.name} to Double")
    }
  }

  override fun size(): Int = inner.size()

  override fun close() {
    // Don't close inner - it's managed elsewhere
  }
}
