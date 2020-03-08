package org.ballistacompute.physical

import org.ballistacompute.datasource.RecordBatch
import org.apache.arrow.vector.types.pojo.Schema

/**
 * Execute a projection.
 */
class ProjectionExec(val input: PhysicalPlan,
                     val schema: Schema,
                     val expr: List<PhysicalExpr>) : PhysicalPlan {

    override fun execute(): Sequence<RecordBatch> {
        return input.execute().map { batch ->
            val columns = expr.map { it.evaluate(batch) }
            RecordBatch(schema, columns)
        }
    }
}