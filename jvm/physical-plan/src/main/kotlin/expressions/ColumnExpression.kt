package org.ballistacompute.physical.expressions

import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

/** Reference column in a batch by index */
class ColumnExpression(val i: Int) : Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        return input.field(i)
    }

    override fun toString(): String {
        return "#$i"
    }
}