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

package io.andygrove.kquery.distributed

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.logical.DataFrame
import io.andygrove.kquery.logical.DataFrameImpl
import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.optimizer.Optimizer
import io.andygrove.kquery.planner.QueryPlanner
import io.andygrove.kquery.sql.SqlParser
import io.andygrove.kquery.sql.SqlPlanner
import io.andygrove.kquery.sql.SqlSelect
import io.andygrove.kquery.sql.SqlTokenizer

/**
 * High-level context for executing distributed queries.
 *
 * This provides a similar API to ExecutionContext but executes queries across
 * multiple executors in a distributed fashion.
 */
class DistributedContext(
    private val config: DistributedConfig,
    private val executorClient: ExecutorClient
) {
  private val tables = mutableMapOf<String, DataFrame>()
  private val planner = DistributedPlanner(config)
  private val scheduler = Scheduler(config, planner, executorClient)

  /**
   * Register a CSV file as a table.
   *
   * @param tableName Name to use for the table in SQL queries
   * @param path Path to the CSV file
   * @param hasHeader Whether the file has a header row
   */
  fun registerCsv(tableName: String, path: String, hasHeader: Boolean = true) {
    val ds = CsvDataSource(path, null, hasHeader, 1024)
    tables[tableName] = DataFrameImpl(Scan(path, ds, listOf()))
  }

  /** Register a DataFrame as a table. */
  fun register(tableName: String, df: DataFrame) {
    tables[tableName] = df
  }

  /** Execute a SQL query and return the results. */
  fun sql(sql: String): Sequence<RecordBatch> {
    // Parse SQL to logical plan
    val tokens = SqlTokenizer(sql).tokenize()
    val ast = SqlParser(tokens).parse() as SqlSelect
    val df = SqlPlanner().createDataFrame(ast, tables)

    // Execute distributed
    return execute(df.logicalPlan())
  }

  /** Execute a logical plan and return the results. */
  fun execute(plan: LogicalPlan): Sequence<RecordBatch> {
    // Optimize
    val optimizedPlan = Optimizer().optimize(plan)

    // Convert to physical plan
    val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)

    // Execute distributed
    return scheduler.execute(physicalPlan)
  }
}
