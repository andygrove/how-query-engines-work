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

import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.logical.*
import io.andygrove.kquery.physical.*
import io.andygrove.kquery.physical.expressions.*
import java.sql.SQLException

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
                is Avg -> AvgExpression(createPhysicalExpr(it.expr, plan.input))
                is Count -> CountExpression(createPhysicalExpr(it.expr, plan.input))
                else -> throw java.lang.IllegalStateException("Unsupported aggregate function: $it")
              }
            }
        HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema())
      }
      is Limit -> {
        val input = createPhysicalPlan(plan.input)
        LimitExec(input, plan.limit)
      }
      is Join -> {
        val leftPlan = createPhysicalPlan(plan.left)
        val rightPlan = createPhysicalPlan(plan.right)
        val leftSchema = plan.left.schema()
        val rightSchema = plan.right.schema()

        // Find column indices for join keys
        val leftKeys =
            plan.on.map { (leftCol, _) ->
              leftSchema.fields
                  .indexOfFirst { it.name == leftCol }
                  .also {
                    if (it == -1) throw SQLException("No column named '$leftCol' in left input")
                  }
            }
        val rightKeys =
            plan.on.map { (_, rightCol) ->
              rightSchema.fields
                  .indexOfFirst { it.name == rightCol }
                  .also {
                    if (it == -1) throw SQLException("No column named '$rightCol' in right input")
                  }
            }

        // Find right columns to exclude (duplicate key columns with same name)
        val duplicateKeyNames = plan.on.filter { it.first == it.second }.map { it.second }.toSet()
        val rightColumnsToExclude =
            rightSchema.fields
                .mapIndexedNotNull { index, field ->
                  if (duplicateKeyNames.contains(field.name)) index else null
                }
                .toSet()

        HashJoinExec(
            leftPlan,
            rightPlan,
            plan.join_type,
            leftKeys,
            rightKeys,
            plan.schema(),
            rightColumnsToExclude)
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
