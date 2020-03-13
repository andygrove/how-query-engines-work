package org.ballistacompute.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ColumnVectorTest {

    @Test
    fun `add int vectors`() {
        val l = createArrowIntVector(10)
        val r = createArrowIntVector(10)
        var result = l.add(r)
        assertEquals(10, result.size())
        (0 until result.size()).forEach {
            assertEquals(it + it, result.getValue(it))
        }
    }

    private fun createArrowIntVector(size: Int): ColumnVector {
        val fieldVector = IntVector("foo", RootAllocator(Long.MAX_VALUE))
        fieldVector.allocateNew(size)
        fieldVector.valueCount = size
        val b = ArrowVectorBuilder(fieldVector)
        (0 until size).forEach {
            b.set(it, it)
        }
        return b.build()
    }
}