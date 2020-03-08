package org.ballistacompute.execution

//import kotlinx.coroutines.*
import org.ballistacompute.physical.*
import org.ballistacompute.datatypes.RecordBatch
import org.ballistacompute.datatypes.ColumnVector
import org.ballistacompute.datatypes.ArrowFieldVector
import org.ballistacompute.datatypes.LiteralValueVector
import org.ballistacompute.datatypes.ArrowVectorBuilder
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ParallelQueryTest {

    @Test
    @Ignore
    fun `parallel query example`() {

        val list = mutableListOf<List<RecordBatch>>()

        0.rangeTo(10).forEach {
//            list.add(GlobalScope.launch {
//                query("partition-$it.csv")
//            })
        }



    }

    private fun query(filename: String) : List<RecordBatch> {
        TODO()
    }

}