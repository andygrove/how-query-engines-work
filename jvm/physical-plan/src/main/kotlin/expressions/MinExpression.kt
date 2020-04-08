package org.ballistacompute.physical.expressions

import java.lang.UnsupportedOperationException

class MinExpression(private val expr: Expression) : AggregateExpression {

    override fun inputExpression(): Expression {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return MinAccumulator()
    }

    override fun toString(): String {
        return "MIN($expr)"
    }
}

class MinAccumulator : Accumulator {

    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                val isMin = when (value) {
                    is Byte -> value < this.value as Byte
                    is Short -> value < this.value as Short
                    is Int -> value < this.value as Int
                    is Long -> value < this.value as Long
                    is Float -> value < this.value as Float
                    is Double -> value < this.value as Double
                    is ByteArray -> throw UnsupportedOperationException("MIN is not implemented for String yet: ${String(value)}")
                    else -> throw UnsupportedOperationException("MIN is not implemented for String yet: ${value.javaClass.name}")
                }
                if (isMin) {
                    this.value = value
                }
            }
        }
    }

    override fun finalValue(): Any? {
        return value
    }
}