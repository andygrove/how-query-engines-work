package org.ballistacompute.physical.expressions

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.LiteralValueVector

/**
 * Physical representation of an expression.
 */
interface Expression {

    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    fun evaluate(input: RecordBatch): ColumnVector
}

class LiteralLongExpression(val value: Long) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value, input.rowCount())
    }
}

class LiteralDoubleExpression(val value: Double) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value, input.rowCount())
    }
}

class LiteralStringExpression(val value: String) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(value.toByteArray(), input.rowCount())
    }
}

interface Accumulator {
    fun accumulate(value: Any?)
    fun finalValue(): Any?
}
