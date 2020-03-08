package org.ballistacompute.logical

import org.apache.arrow.vector.types.pojo.Field

/**
 * Logical Expression for use in logical query plans. The logical expression provides information needed
 * during the planning phase such as the name and data type of the expression.
 */
interface LogicalExpr {

    /**
     * Return meta-data about the value that will be produced by this expression when evaluated against
     * a particular input.
     */
    fun toField(input: LogicalPlan): Field
}