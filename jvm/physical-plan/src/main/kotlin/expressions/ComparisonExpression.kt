package org.ballistacompute.physical.expressions

import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

abstract class ComparisonExpression(val l: Expression, val r: Expression) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return compare(ll, rr)
    }

    abstract fun compare(l: ColumnVector, r: ColumnVector) : ColumnVector
}