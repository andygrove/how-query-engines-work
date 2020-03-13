package org.ballistacompute.datatypes

import java.lang.IllegalStateException
import java.lang.IndexOutOfBoundsException

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

    override fun eq(rhs: ColumnVector): ColumnVector {
        return when (rhs) {
            is LiteralValueVector -> LiteralValueVector(value == rhs.value, size)
            else -> throw IllegalArgumentException()
        }
    }

    override fun neq(rhs: ColumnVector): ColumnVector {
        return when (rhs) {
            is LiteralValueVector -> LiteralValueVector(value != rhs.value, size)
            else -> throw IllegalArgumentException()
        }
    }

    override fun lt(rhs: ColumnVector): ColumnVector {
        TODO()
    }

    override fun lteq(rhs: ColumnVector): ColumnVector {
        TODO()
    }

    override fun gt(rhs: ColumnVector): ColumnVector {
        TODO()
    }

    override fun gteq(rhs: ColumnVector): ColumnVector {
        TODO()
    }

    override fun add(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun subtract(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun multiply(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun divide(rhs: ColumnVector): ColumnVector {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}