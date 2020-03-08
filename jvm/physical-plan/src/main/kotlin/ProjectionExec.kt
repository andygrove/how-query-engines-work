package org.ballistacompute.physical

import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.LiteralValueVector
import org.ballistacompute.datatypes.ArrowVectorBuilder

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