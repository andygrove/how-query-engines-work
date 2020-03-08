package org.ballistacompute.datasource

import org.apache.arrow.vector.*
import java.lang.IndexOutOfBoundsException

interface ColumnVector {
    fun getValue(i: Int) : Any?
    fun size(): Int
}

/** Wrapper around Arrow FieldVector */
class ArrowFieldVector(val field: FieldVector) : ColumnVector {

    override fun getValue(i: Int) : Any? {

        if (field.isNull(i)) {
            return null
        }

        return when (field) {
            is VarCharVector -> field.get(i)
            is IntVector -> field.get(i)
            is Float8Vector -> field.get(i)
            else -> TODO(field.javaClass.name)
        }
    }

    override fun size(): Int {
        return field.valueCount
    }
}

/** Represents a literal value */
class LiteralValueVector(val value: Any?, val size: Int) : ColumnVector {

    override fun getValue(i: Int): Any? {
        if (i<0 || i>=size) {
            throw IndexOutOfBoundsException()
        }
        return value
    }

    override fun size(): Int {
        return size
    }
}

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

}