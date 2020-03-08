package org.ballistacompute.datatypes

import org.apache.arrow.vector.*
import java.lang.IndexOutOfBoundsException

interface ColumnVector {
    fun getValue(i: Int) : Any?
    fun size(): Int
}



