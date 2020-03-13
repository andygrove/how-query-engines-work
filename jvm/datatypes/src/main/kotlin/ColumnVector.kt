package org.ballistacompute.datatypes

/**
 * Abstraction over different implementations of a column vector.
 */
interface ColumnVector {

    fun getValue(i: Int) : Any?

    fun size(): Int

    fun add(rhs: ColumnVector): ColumnVector
    fun subtract(rhs: ColumnVector): ColumnVector
    fun multiply(rhs: ColumnVector): ColumnVector
    fun divide(rhs: ColumnVector): ColumnVector

    fun eq(rhs: ColumnVector): ColumnVector
    fun neq(rhs: ColumnVector): ColumnVector
    fun lt(rhs: ColumnVector): ColumnVector
    fun lteq(rhs: ColumnVector): ColumnVector
    fun gt(rhs: ColumnVector): ColumnVector
    fun gteq(rhs: ColumnVector): ColumnVector
}



