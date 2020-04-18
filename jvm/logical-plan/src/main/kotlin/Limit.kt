package org.ballistacompute.logical

import org.ballistacompute.datatypes.Schema

/**
 * Logical plan representing a limit
 */
class Limit(val input: LogicalPlan, val limit: Int): LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Limit: $limit"
    }
}