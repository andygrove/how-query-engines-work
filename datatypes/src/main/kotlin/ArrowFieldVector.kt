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

import java.lang.IllegalStateException
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType

/** Shared Arrow memory allocator for the entire application. */
object ArrowAllocator {
  val rootAllocator: BufferAllocator = RootAllocator(Long.MAX_VALUE)
}

object FieldVectorFactory {

  fun create(arrowType: ArrowType, initialCapacity: Int): FieldVector {
    val fieldVector: FieldVector =
        when (arrowType) {
          ArrowTypes.BooleanType -> BitVector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.Int8Type ->
              TinyIntVector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.Int16Type ->
              SmallIntVector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.Int32Type -> IntVector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.Int64Type ->
              BigIntVector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.FloatType ->
              Float4Vector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.DoubleType ->
              Float8Vector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.StringType ->
              VarCharVector("v", ArrowAllocator.rootAllocator)
          ArrowTypes.DateDayType ->
              DateDayVector("v", ArrowAllocator.rootAllocator)
          else -> throw IllegalStateException()
        }
    if (initialCapacity != 0) {
      fieldVector.setInitialCapacity(initialCapacity)
    }
    fieldVector.allocateNew()
    return fieldVector
  }
}
/** Wrapper around Arrow FieldVector */
class ArrowFieldVector(val field: FieldVector) : ColumnVector {

  override fun getType(): ArrowType {
    return when (field) {
      is BitVector -> ArrowTypes.BooleanType
      is TinyIntVector -> ArrowTypes.Int8Type
      is SmallIntVector -> ArrowTypes.Int16Type
      // snippet-omit-start
      is IntVector -> ArrowTypes.Int32Type
      is BigIntVector -> ArrowTypes.Int64Type
      is Float4Vector -> ArrowTypes.FloatType
      is Float8Vector -> ArrowTypes.DoubleType
      is VarCharVector -> ArrowTypes.StringType
      is VarBinaryVector -> ArrowTypes.BinaryType
      is DateDayVector -> ArrowTypes.DateDayType
      is DecimalVector -> field.field.type
      is Decimal256Vector -> field.field.type
      // snippet-omit-end
      else ->
          throw IllegalStateException(
              "Unsupported vector type: ${field.javaClass.name}")
    }
  }

  override fun getValue(i: Int): Any? {

    if (field.isNull(i)) {
      return null
    }

    return when (field) {
      is BitVector -> if (field.get(i) == 1) true else false
      is TinyIntVector -> field.get(i)
      is SmallIntVector -> field.get(i)
      // snippet-omit-start
      is IntVector -> field.get(i)
      is BigIntVector -> field.get(i)
      is Float4Vector -> field.get(i)
      is Float8Vector -> field.get(i)
      is VarCharVector -> {
        val bytes = field.get(i)
        if (bytes == null) {
          null
        } else {
          String(bytes)
        }
      }
      is VarBinaryVector -> {
        val bytes = field.get(i)
        if (bytes == null) {
          null
        } else {
          String(bytes)
        }
      }
      is DateDayVector -> field.get(i)
      is DecimalVector -> field.getObject(i)
      is Decimal256Vector -> field.getObject(i)
      // snippet-omit-end
      else ->
          throw IllegalStateException(
              "Unsupported vector type: ${field.javaClass.name}")
    }
  }

  override fun size(): Int {
    return field.valueCount
  }

  override fun close() {
    field.close()
  }
}
