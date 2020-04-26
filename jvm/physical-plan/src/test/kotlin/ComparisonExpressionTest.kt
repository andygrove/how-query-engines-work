package org.ballistacompute.physical

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.VectorSchemaRoot
import org.ballistacompute.datatypes.*
import org.ballistacompute.physical.expressions.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ComparisonExpressionTest {

    @Test
    fun `gteq doubles`() {

        val schema = Schema(listOf(
                Field("a", ArrowTypes.DoubleType),
                Field("b", ArrowTypes.DoubleType)
        ))

        val a: List<Double> = listOf(0.0, 1.0, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN)
        val b = a.reversed()

        val batch = createRecordBatch(schema, listOf(a,b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach {
            assertEquals(if (a[it] >= b[it]) 1 else 0, result.getValue(it))
        }
    }


    private fun createRecordBatch(schema: Schema, columns: List<List<Any?>>): RecordBatch {

        val rowCount = columns[0].size

        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.allocateNew()

        (0 until rowCount).forEach { row ->
            (0 until columns.size).forEach { col ->
                val v = root.getVector(col)
                val value = columns[col][row]
                when (v) {
                    is Float4Vector -> v.set(row, value as Float)
                    is Float8Vector -> v.set(row, value as Double)
                    else -> TODO()
                }
            }
        }
        root.rowCount = rowCount

        return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
    }

}