package org.ballistacompute.physical

import org.ballistacompute.datasource.ArrowFieldVector
import org.ballistacompute.datasource.ArrowVectorBuilder
import org.ballistacompute.datasource.ColumnVector
import org.ballistacompute.datasource.RecordBatch
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot

/**
 * Execute a selection.
 */
class SelectionExec(val input: PhysicalPlan, val expr: PhysicalExpr) : PhysicalPlan {
    override fun execute(): Sequence<RecordBatch> {
        val input = input.execute()
        return input.map { batch ->

            //println("selection input:\n${batch.toCSV()}")

            val result = (expr.evaluate(batch) as ArrowFieldVector).field as BitVector

            val schema = batch.schema
            val columnCount = batch.schema.fields.size
            val filteredFields = (0 until columnCount).map { filter(batch.field(it), result) }
            val filteredBatch = RecordBatch(schema, filteredFields.map { ArrowFieldVector(it) })

            //println("selection output:\n${filteredBatch.toCSV()}")

            filteredBatch
        }
    }

    private fun filter(v: ColumnVector, selection: BitVector) : FieldVector {
        //println("filter() selection BitVector length = ${selection.valueCount}")
        val filteredVector = VarCharVector("v", RootAllocator(Long.MAX_VALUE))
        filteredVector.allocateNew()

        val builder = ArrowVectorBuilder(filteredVector)

        var count = 0
        (0 until selection.valueCount)
                .forEach {
                    if (selection.get(it) == 1) {
                        //println("match")
                        builder.set(count, v.getValue(it))
                        count++
                    } else {
                        //println("no match")
                    }
                }
        filteredVector.valueCount = count

        return filteredVector
    }
}
