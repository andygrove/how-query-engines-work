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

package io.andygrove.kquery.planner

import io.andygrove.kquery.datasource.InMemoryDataSource
import io.andygrove.kquery.datatypes.ArrowTypes
import io.andygrove.kquery.datatypes.Field
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.logical.*
import io.andygrove.kquery.logical.DataFrameImpl
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.optimizer.*
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueryPlannerTest {

  @Test
  fun `plan aggregate query`() {
    val schema =
        Schema(
            listOf(
                Field("passenger_count", ArrowTypes.UInt32Type),
                Field("max_fare", ArrowTypes.DoubleType)))

    val dataSource = InMemoryDataSource(schema, listOf())

    val df = DataFrameImpl(Scan("", dataSource, listOf()))

    val plan =
        df.aggregate(listOf(col("passenger_count")), listOf(max(col("max_fare")))).logicalPlan()
    assertEquals(
        "Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(#max_fare)]\n" +
            "\tScan: ; projection=None\n",
        plan.pretty())

    val optimizedPlan = Optimizer().optimize(plan)
    assertEquals(
        "Aggregate: groupExpr=[#passenger_count], aggregateExpr=[MAX(#max_fare)]\n" +
            "\tScan: ; projection=[max_fare, passenger_count]\n",
        optimizedPlan.pretty())

    val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)
    assertEquals(
        "HashAggregateExec: groupExpr=[#1], aggrExpr=[MAX(#0)]\n" +
            "\tScanExec: schema=Schema(fields=[Field(name=max_fare, dataType=FloatingPoint(DOUBLE)), Field(name=passenger_count, dataType=Int(32, false))]), projection=[max_fare, passenger_count]\n",
        physicalPlan.pretty())
  }
}
