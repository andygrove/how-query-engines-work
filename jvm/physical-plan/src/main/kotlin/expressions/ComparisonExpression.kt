package org.ballistacompute.physical.expressions

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.RecordBatch

abstract class ComparisonExpression(val l: Expression, val r: Expression) : Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        return compare(ll, rr)
    }

    fun compare(l: ColumnVector, r: ColumnVector): ColumnVector {
        assert(l.size() == r.size())
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()

        (0 until l.size()).forEach {
            if (evaluate(l.getValue(it), r.getValue(it))) {
                v.set(it, 1)
            } else {
                v.set(it, 0)
            }
        }
        v.valueCount = l.size()
        return ArrowFieldVector(v)
    }

    abstract fun evaluate(l: Any?, r: Any?) : Boolean
}