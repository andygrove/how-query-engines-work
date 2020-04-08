package org.ballistacompute.physical.expressions

interface AggregateExpression {
    fun inputExpression(): Expression
    fun createAccumulator(): Accumulator
}