package org.ballistacompute.fuzzer

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FuzzerTest {

    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `fuzzer example`() {
        val csv = CsvDataSource(employeeCsv, null, 10)
        val input = DataFrameImpl(Scan("employee.csv", csv, listOf()))
        val fuzzer = Fuzzer()


        (0 until 50).forEach {
            println(fuzzer.createPlan(input, 0, 6, 1).logicalPlan().pretty())

//            val expr = fuzzer.createExpression(input, 0, 5)
//            println(expr)
        }
    }

}