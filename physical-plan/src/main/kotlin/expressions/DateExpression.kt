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

import io.andygrove.kquery.datatypes.*

/**
 * Expression for subtracting an interval (days) from a date. Returns a date.
 */
class DateSubtractIntervalExpression(
    val dateExpr: Expression,
    val intervalExpr: Expression
) : Expression {

  override fun evaluate(input: RecordBatch): ColumnVector {
    val dateCol = dateExpr.evaluate(input)
    val intervalCol = intervalExpr.evaluate(input)

    val fieldVector =
        FieldVectorFactory.create(ArrowTypes.DateDayType, dateCol.size())
    val builder = ArrowVectorBuilder(fieldVector)

    (0 until dateCol.size()).forEach { i ->
      val dateValue = dateCol.getValue(i)
      val intervalValue = intervalCol.getValue(i)

      if (dateValue == null || intervalValue == null) {
        builder.set(i, null)
      } else {
        val dateDays = (dateValue as Number).toInt()
        val intervalDays = (intervalValue as Number).toLong()
        builder.set(i, dateDays - intervalDays.toInt())
      }
    }

    builder.setValueCount(dateCol.size())
    return builder.build()
  }

  override fun toString(): String {
    return "$dateExpr - $intervalExpr"
  }
}

/** Expression for adding an interval (days) to a date. Returns a date. */
class DateAddIntervalExpression(
    val dateExpr: Expression,
    val intervalExpr: Expression
) : Expression {

  override fun evaluate(input: RecordBatch): ColumnVector {
    val dateCol = dateExpr.evaluate(input)
    val intervalCol = intervalExpr.evaluate(input)

    val fieldVector =
        FieldVectorFactory.create(ArrowTypes.DateDayType, dateCol.size())
    val builder = ArrowVectorBuilder(fieldVector)

    (0 until dateCol.size()).forEach { i ->
      val dateValue = dateCol.getValue(i)
      val intervalValue = intervalCol.getValue(i)

      if (dateValue == null || intervalValue == null) {
        builder.set(i, null)
      } else {
        val dateDays = (dateValue as Number).toInt()
        val intervalDays = (intervalValue as Number).toLong()
        builder.set(i, dateDays + intervalDays.toInt())
      }
    }

    builder.setValueCount(dateCol.size())
    return builder.build()
  }

  override fun toString(): String {
    return "$dateExpr + $intervalExpr"
  }
}
