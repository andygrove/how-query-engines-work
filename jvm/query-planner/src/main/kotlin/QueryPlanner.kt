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

import java.sql.SQLException
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.logical.*
import io.andygrove.kquery.physical.*
import io.andygrove.kquery.physical.expressions.*

/** The query planner creates a physical query plan from a logical query plan. */
class QueryPlanner {

  /** Create a physical plan from a logical plan. */
  fun createPhysicalPlan(plan: LogicalPlan): PhysicalPlan {
    return when (plan) {
      is Scan -> {
        ScanExec(plan.dataSource, plan.projection)
      }
      is Selection -> {
        val input = createPhysicalPlan(plan.input)
        val filterExpr = createPhysicalExpr(plan.expr, plan.input)
        SelectionExec(input, filterExpr)
      }
      is Projection -> {
        val input = createPhysicalPlan(plan.input)
        val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
        val projectionSchema = Schema(plan.expr.map { it.toField(plan.input) })
        ProjectionExec(input, projectionSchema, projectionExpr)
      }
      is Aggregate -> {
        val input = createPhysicalPlan(plan.input)
        val groupExpr = plan.groupExpr.map { createPhysicalExpr(it, plan.input) }
        val aggregateExpr =
            plan.aggregateExpr.map {
              when (it) {
                is Max -> MaxExpression(createPhysicalExpr(it.expr, plan.input))
                is Min -> MinExpression(createPhysicalExpr(it.expr, plan.input))
                is Sum -> SumExpression(createPhysicalExpr(it.expr, plan.input))
                else -> throw java.lang.IllegalStateException("Unsupported aggregate function: $it")
              }
            }
        HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema())
      }
      else -> throw IllegalStateException(plan.javaClass.toString())
    }
  }

  /** Create a physical expression from a logical expression. */
  fun createPhysicalExpr(expr: LogicalExpr, input: LogicalPlan): Expression =
      when (expr) {
        is LiteralLong -> LiteralLongExpression(expr.n)
        is LiteralDouble -> LiteralDoubleExpression(expr.n)
        is LiteralString -> LiteralStringExpression(expr.str)
        is ColumnIndex -> ColumnExpression(expr.i)
        is Alias -> {
          // note that there is no physical expression for an alias since the alias
          // only affects the name using in the planning phase and not how the aliased
          // expression is executed
          createPhysicalExpr(expr.expr, input)
        }
        is Column -> {
          val i = input.schema().fields.indexOfFirst { it.name == expr.name }
          if (i == -1) {
            throw SQLException("No column named '${expr.name}'")
          }
          ColumnExpression(i)
        }
        is CastExpr -> CastExpression(createPhysicalExpr(expr.expr, input), expr.dataType)
        is BinaryExpr -> {
          val l = createPhysicalExpr(expr.l, input)
          val r = createPhysicalExpr(expr.r, input)
          when (expr) {
            // comparision
            is Eq -> EqExpression(l, r)
            is Neq -> NeqExpression(l, r)
            is Gt -> GtExpression(l, r)
            is GtEq -> GtEqExpression(l, r)
            is Lt -> LtExpression(l, r)
            is LtEq -> LtEqExpression(l, r)

            // boolean
            is And -> AndExpression(l, r)
            is Or -> OrExpression(l, r)

            // math
            is Add -> AddExpression(l, r)
            is Subtract -> SubtractExpression(l, r)
            is Multiply -> MultiplyExpression(l, r)
            is Divide -> DivideExpression(l, r)
            else -> throw IllegalStateException("Unsupported binary expression: $expr")
          }
        }
        else -> throw IllegalStateException("Unsupported logical expression: $expr")
      }
}
