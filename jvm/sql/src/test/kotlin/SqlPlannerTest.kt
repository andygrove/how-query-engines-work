package org.ballistacompute.sql

import org.ballistacompute.datasource.CsvDataSource
import org.ballistacompute.logical.LogicalPlan
import org.ballistacompute.logical.format
import org.ballistacompute.logical.DataFrameImpl
import org.ballistacompute.logical.Scan
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlPlannerTest {

    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `plan simple SELECT`() {
        val plan = plan("SELECT state FROM employee")
        assertEquals("Projection: #state\n" +
                "\tScan: ; projection=None\n", format(plan))
    }

    @Test
    @Ignore
    fun `parse aggregate query`() {
        val plan = plan("SELECT state, MAX(salary) FROM employee")
        assertEquals("Aggregate: groupBy=#state, aggregate=MAX(#salary)\n" +
                "\tScan: ; projection=None", format(plan))
    }

    private fun plan(sql: String) : LogicalPlan {
        println("parse() $sql")

        val tokens = SqlTokenizer(sql).tokenize()
        println(tokens)

        val parsedQuery = SqlParser(tokens).parse()
        println(parsedQuery)

       val tables = mapOf("employee" to DataFrameImpl(Scan("", CsvDataSource(employeeCsv, 1024), listOf())))

        val df = SqlPlanner().createDataFrame(parsedQuery as SqlSelect, tables)

        val plan = df.logicalPlan()
        println(format(plan))

        return plan
    }
}

