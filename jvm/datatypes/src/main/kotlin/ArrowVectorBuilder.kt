package org.ballistacompute.datatypes

import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector

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
            is IntVector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toInt())
                } else if (value is String) {
                    fieldVector.set(i, value.toInt())
                } else {
                    TODO()
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
                    TODO()
                }
            }
            else -> TODO()
        }
    }

    fun build(): ColumnVector {
        return ArrowFieldVector(fieldVector)
    }

}