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

package io.andygrove.kquery.benchmarks

import io.andygrove.kquery.datasource.ParquetDataSource
import io.andygrove.kquery.execution.ExecutionContext
import java.io.File
import kotlin.system.measureTimeMillis

/**
 * TPC-H query runner.
 *
 * Usage: TpchRunner <sql-file> <data-dir>
 *
 * Arguments: sql-file Path to SQL file containing the query data-dir Path to
 * directory containing TPC-H parquet files
 *
 * Expected parquet files in data-dir: customer.parquet, lineitem.parquet,
 * nation.parquet, orders.parquet, part.parquet, partsupp.parquet,
 * region.parquet, supplier.parquet
 */
fun main(args: Array<String>) {
  if (args.size != 2) {
    System.err.println("Usage: TpchRunner <sql-file> <data-dir>")
    System.err.println()
    System.err.println("Arguments:")
    System.err.println("  sql-file  Path to SQL file containing the query")
    System.err.println(
        "  data-dir  Path to directory containing TPC-H parquet files")
    System.exit(1)
  }

  val sqlFile = args[0]
  val dataDir = args[1]

  // Read SQL query from file
  val sql = File(sqlFile).readText()
  println("Executing query from $sqlFile:")
  println(sql)
  println()

  // Create execution context
  val ctx = ExecutionContext(mapOf())

  // Register TPC-H tables
  val tables =
      listOf(
          "customer",
          "lineitem",
          "nation",
          "orders",
          "part",
          "partsupp",
          "region",
          "supplier")
  for (table in tables) {
    val path = "$dataDir/$table.parquet"
    ctx.registerDataSource(table, ParquetDataSource(path))
  }

  // Execute query and measure time
  val time = measureTimeMillis {
    val df = ctx.sql(sql)
    val results = ctx.execute(df)
    results.forEach { batch ->
      println(batch.schema)
      println(batch.toCSV())
    }
  }

  println()
  println("Query executed in $time ms")
}
