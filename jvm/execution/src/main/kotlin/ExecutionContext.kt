// Copyright 2020 Andy Grove
//
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

package io.andygrove.kquery.execution

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.datasource.DataSource
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.logical.*
import io.andygrove.kquery.optimizer.Optimizer
import io.andygrove.kquery.planner.QueryPlanner
import io.andygrove.kquery.sql.SqlParser
import io.andygrove.kquery.sql.SqlPlanner
import io.andygrove.kquery.sql.SqlSelect
import io.andygrove.kquery.sql.SqlTokenizer

/** Execution context */
class ExecutionContext(val settings: Map<String, String>) {

  val batchSize: Int = settings.getOrDefault("ballista.csv.batchSize", "1024").toInt()

  /** Tables registered with this context */
  private val tables = mutableMapOf<String, DataFrame>()

  /** Create a DataFrame for the given SQL Select */
  fun sql(sql: String): DataFrame {
    val tokens = SqlTokenizer(sql).tokenize()
    val ast = SqlParser(tokens).parse() as SqlSelect
    val df = SqlPlanner().createDataFrame(ast, tables)
    return DataFrameImpl(df.logicalPlan())
  }

  /** Get a DataFrame representing the specified CSV file */
  fun csv(filename: String): DataFrame {
    return DataFrameImpl(Scan(filename, CsvDataSource(filename, null, true, batchSize), listOf()))
  }

  /** Register a DataFrame with the context */
  fun register(tablename: String, df: DataFrame) {
    tables[tablename] = df
  }

  /** Register a CSV data source with the context */
  fun registerDataSource(tablename: String, datasource: DataSource) {
    register(tablename, DataFrameImpl(Scan(tablename, datasource, listOf())))
  }

  /** Register a CSV data source with the context */
  fun registerCsv(tablename: String, filename: String) {
    register(tablename, csv(filename))
  }

  /** Execute the logical plan represented by a DataFrame */
  fun execute(df: DataFrame): Sequence<RecordBatch> {
    return execute(df.logicalPlan())
  }

  /** Execute the provided logical plan */
  fun execute(plan: LogicalPlan): Sequence<RecordBatch> {
    val optimizedPlan = Optimizer().optimize(plan)
    val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)
    return physicalPlan.execute()
  }
}
