// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.andygrove.kquery.sql

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.logical.DataFrameImpl
import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.logical.format
import java.io.File
import java.sql.SQLException
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlPlannerTest {

  val dir = "../testdata"

  val employeeCsv = File(dir, "employee.csv").absolutePath

  @Test
  fun `simple select`() {
    val plan = plan("SELECT state FROM employee")
    assertEquals(
        "Projection: #state\n" + "\tScan: ; projection=None\n", format(plan))
  }

  @Test
  fun `select with filter`() {
    val plan = plan("SELECT state FROM employee WHERE state = 'CA'")
    assertEquals(
        "Selection: #state = 'CA'\n" +
            "\tProjection: #state\n" +
            "\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `select with filter not in projection`() {
    val plan = plan("SELECT last_name FROM employee WHERE state = 'CA'")
    assertEquals(
        "Projection: #last_name\n" +
            "\tSelection: #state = 'CA'\n" +
            "\t\tProjection: #last_name, #state\n" +
            "\t\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `select filter on projection`() {
    val plan =
        plan("SELECT last_name AS foo FROM employee WHERE foo = 'Einstein'")
    assertEquals(
        "Selection: #foo = 'Einstein'\n" +
            "\tProjection: #last_name as foo\n" +
            "\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `select filter on projection and not`() {
    val plan =
        plan(
            "SELECT last_name AS foo " +
                "FROM employee " +
                "WHERE foo = 'Einstein' AND state = 'CA'")
    assertEquals(
        "Projection: #foo\n" +
            "\tSelection: #foo = 'Einstein' AND #state = 'CA'\n" +
            "\t\tProjection: #last_name as foo, #state\n" +
            "\t\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `plan aggregate query`() {
    val plan = plan("SELECT state, MAX(salary) FROM employee GROUP BY state")
    assertEquals(
        "Projection: #0, #1\n" +
            "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
            "\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `plan aggregate query with having`() {
    val plan =
        plan(
            "SELECT state, MAX(salary) FROM employee GROUP BY state HAVING MAX(salary) > 10")
    assertEquals(
        "Selection: MAX(#salary) > 10\n" +
            "\tProjection: #0, #1\n" +
            "\t\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
            "\t\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `plan aggregate query aggr first`() {
    val plan = plan("SELECT MAX(salary), state FROM employee GROUP BY state")
    assertEquals(
        "Projection: #1, #0\n" +
            "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
            "\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `plan aggregate query with filter`() {
    val plan =
        plan(
            "SELECT state, MAX(salary) FROM employee WHERE salary > 50000 GROUP BY state")
    assertEquals(
        "Projection: #0, #1\n" +
            "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
            "\t\tSelection: #salary > 50000\n" +
            "\t\t\tProjection: #state, #salary\n" +
            "\t\t\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `plan aggregate query with cast`() {
    val plan =
        plan(
            "SELECT state, MAX(CAST(salary AS double)) FROM employee GROUP BY state")
    assertEquals(
        "Projection: #0, #1\n" +
            "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(CAST(#salary AS FloatingPoint(DOUBLE)))]\n" +
            "\t\tScan: ; projection=None\n",
        format(plan))
  }

  @Test
  fun `COUNT without argument should error`() {
    val exception =
        assertThrows<SQLException> { plan("SELECT COUNT() FROM employee") }
    assertEquals(
        "COUNT() requires an argument, use COUNT(*) to count all rows",
        exception.message)
  }

  @Test
  fun `MAX without argument should error`() {
    val exception =
        assertThrows<SQLException> { plan("SELECT MAX() FROM employee") }
    assertEquals("MAX() requires an argument", exception.message)
  }

  @Test
  fun `MIN without argument should error`() {
    val exception =
        assertThrows<SQLException> { plan("SELECT MIN() FROM employee") }
    assertEquals("MIN() requires an argument", exception.message)
  }

  @Test
  fun `SUM without argument should error`() {
    val exception =
        assertThrows<SQLException> { plan("SELECT SUM() FROM employee") }
    assertEquals("SUM() requires an argument", exception.message)
  }

  @Test
  fun `AVG without argument should error`() {
    val exception =
        assertThrows<SQLException> { plan("SELECT AVG() FROM employee") }
    assertEquals("AVG() requires an argument", exception.message)
  }

  private fun plan(sql: String): LogicalPlan {
    println("parse() $sql")

    val tokens = SqlTokenizer(sql).tokenize()
    println(tokens)

    val parsedQuery = SqlParser(tokens).parse()
    println(parsedQuery)

    val tables =
        mapOf(
            "employee" to
                DataFrameImpl(
                    Scan(
                        "",
                        CsvDataSource(employeeCsv, null, true, 1024),
                        listOf())))

    val df = SqlPlanner().createDataFrame(parsedQuery as SqlSelect, tables)

    val plan = df.logicalPlan()
    println(format(plan))

    return plan
  }
}
