package io.andygrove.kquery.logical

import org.apache.arrow.vector.types.pojo.Schema

/**
 * Logical plan representing a selection (a.k.a. filter) against an input
 */
class Selection(val input: LogicalPlan, val expr: LogicalExpr): LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Selection: $expr"
    }
}