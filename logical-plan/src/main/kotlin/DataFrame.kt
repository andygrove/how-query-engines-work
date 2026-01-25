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

package io.andygrove.kquery.logical

import io.andygrove.kquery.datatypes.Schema

interface DataFrame {

  /** Apply a projection */
  fun project(expr: List<LogicalExpr>): DataFrame

  /** Apply a filter */
  fun filter(expr: LogicalExpr): DataFrame

  /** Aggregate */
  fun aggregate(
      groupBy: List<LogicalExpr>,
      aggregateExpr: List<AggregateExpr>
  ): DataFrame
  // snippet-omit-start

  /** Limit the number of rows */
  fun limit(n: Int): DataFrame
  // snippet-omit-end

  /** Join with another DataFrame */
  fun join(
      right: DataFrame,
      joinType: JoinType,
      on: List<Pair<String, String>>
  ): DataFrame

  /** Returns the schema of the data that will be produced by this DataFrame. */
  fun schema(): Schema

  /** Get the logical plan */
  fun logicalPlan(): LogicalPlan
}

class DataFrameImpl(private val plan: LogicalPlan) : DataFrame {

  override fun project(expr: List<LogicalExpr>): DataFrame {
    return DataFrameImpl(Projection(plan, expr))
  }

  override fun filter(expr: LogicalExpr): DataFrame {
    return DataFrameImpl(Selection(plan, expr))
  }

  override fun aggregate(
      groupBy: List<LogicalExpr>,
      aggregateExpr: List<AggregateExpr>
  ): DataFrame {
    return DataFrameImpl(Aggregate(plan, groupBy, aggregateExpr))
  }
  // snippet-omit-start

  override fun limit(n: Int): DataFrame {
    return DataFrameImpl(Limit(plan, n))
  }
  // snippet-omit-end

  override fun join(
      right: DataFrame,
      joinType: JoinType,
      on: List<Pair<String, String>>
  ): DataFrame {
    return DataFrameImpl(Join(plan, right.logicalPlan(), joinType, on))
  }

  override fun schema(): Schema {
    return plan.schema()
  }

  override fun logicalPlan(): LogicalPlan {
    return plan
  }
}
