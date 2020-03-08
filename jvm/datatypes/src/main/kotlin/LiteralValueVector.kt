package org.ballistacompute.datatypes

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
}