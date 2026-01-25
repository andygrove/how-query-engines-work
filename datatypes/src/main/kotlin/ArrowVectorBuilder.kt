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

package io.andygrove.kquery.datatypes

import java.math.BigDecimal
import org.apache.arrow.vector.*

class ArrowVectorBuilder(val fieldVector: FieldVector) {

  fun set(i: Int, value: Any?) {
    when (fieldVector) {
      is VarCharVector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is ByteArray) {
          fieldVector.set(i, value)
        } else {
          fieldVector.set(i, value.toString().toByteArray())
        }
      }
      is TinyIntVector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is Number) {
          fieldVector.set(i, value.toByte())
        } else if (value is String) {
          fieldVector.set(i, value.toByte())
        } else {
          throw IllegalStateException()
        }
      }
      is SmallIntVector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is Number) {
          fieldVector.set(i, value.toShort())
        } else if (value is String) {
          fieldVector.set(i, value.toShort())
        } else {
          throw IllegalStateException()
        }
      }
      is IntVector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is Number) {
          fieldVector.set(i, value.toInt())
        } else if (value is String) {
          fieldVector.set(i, value.toInt())
        } else {
          throw IllegalStateException()
        }
      }
      is BigIntVector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is Number) {
          fieldVector.set(i, value.toLong())
        } else if (value is String) {
          fieldVector.set(i, value.toLong())
        } else {
          throw IllegalStateException()
        }
      }
      is Float4Vector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is Number) {
          fieldVector.set(i, value.toFloat())
        } else if (value is String) {
          fieldVector.set(i, value.toFloat())
        } else {
          throw IllegalStateException()
        }
      }
      is Float8Vector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is Number) {
          fieldVector.set(i, value.toDouble())
        } else if (value is String) {
          fieldVector.set(i, value.toDouble())
        } else {
          throw IllegalStateException(
              "Cannot set Float8Vector with ${value.javaClass.name}")
        }
      }
      is DecimalVector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else {
          val decimal =
              when (value) {
                is BigDecimal -> value
                is Number -> BigDecimal.valueOf(value.toDouble())
                is String -> BigDecimal(value)
                else ->
                    throw IllegalStateException(
                        "Cannot set DecimalVector with ${value.javaClass.name}")
              }
          fieldVector.set(
              i,
              decimal.setScale(
                  fieldVector.scale, java.math.RoundingMode.HALF_UP))
        }
      }
      is Decimal256Vector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else {
          val decimal =
              when (value) {
                is BigDecimal -> value
                is Number -> BigDecimal.valueOf(value.toDouble())
                is String -> BigDecimal(value)
                else ->
                    throw IllegalStateException(
                        "Cannot set Decimal256Vector with ${value.javaClass.name}")
              }
          fieldVector.set(
              i,
              decimal.setScale(
                  fieldVector.scale, java.math.RoundingMode.HALF_UP))
        }
      }
      is DateDayVector -> {
        if (value == null) {
          fieldVector.setNull(i)
        } else if (value is Number) {
          fieldVector.set(i, value.toInt())
        } else {
          throw IllegalStateException(
              "Cannot set DateDayVector with ${value.javaClass.name}")
        }
      }
      else ->
          throw IllegalStateException(
              "Unsupported vector type: ${fieldVector.javaClass.name}")
    }
  }

  fun setValueCount(n: Int) {
    fieldVector.valueCount = n
  }

  fun build(): ColumnVector {
    return ArrowFieldVector(fieldVector)
  }
}
