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

import io.andygrove.kquery.logical.*
import java.sql.SQLException
import java.util.logging.Logger
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType

/** SqlPlanner creates a logical plan from a parsed SQL statement. */
class SqlPlanner {

  private val logger = Logger.getLogger(SqlPlanner::class.simpleName)

  /** Create logical plan from parsed SQL statement. */
  fun createDataFrame(
      select: SqlSelect,
      tables: Map<String, DataFrame>
  ): DataFrame {

    // get a reference to the data source
    val table =
        tables[select.tableName]
            ?: throw SQLException("No table named '${select.tableName}'")

    // translate projection sql expressions into logical expressions
    val projectionExpr = select.projection.map { createLogicalExpr(it, table) }

    // build a list of columns referenced in the projection
    val columnNamesInProjection = getReferencedColumns(projectionExpr)

    val aggregateExprCount = projectionExpr.count { isAggregateExpr(it) }
    if (aggregateExprCount == 0 && select.groupBy.isNotEmpty()) {
      throw SQLException(
          "GROUP BY without aggregate expressions is not supported")
    }

    // does the filter expression reference anything not in the final projection?
    val columnNamesInSelection = getColumnsReferencedBySelection(select, table)

    var plan = table

    if (aggregateExprCount == 0) {
      return planNonAggregateQuery(
          select,
          plan,
          projectionExpr,
          columnNamesInSelection,
          columnNamesInProjection)
    } else {
      val projection = mutableListOf<LogicalExpr>()
      val aggrExpr = mutableListOf<AggregateExpr>()
      val numGroupCols = select.groupBy.size
      var groupCount = 0

      projectionExpr.forEach { expr ->
        when (expr) {
          is AggregateExpr -> {
            projection.add(ColumnIndex(numGroupCols + aggrExpr.size))
            aggrExpr.add(expr)
          }
          is Alias -> {
            val innerExpr = expr.expr
            if (innerExpr !is AggregateExpr) {
              throw SQLException(
                  "Alias in aggregate query must wrap an aggregate expression, found: $innerExpr")
            }
            projection.add(
                Alias(ColumnIndex(numGroupCols + aggrExpr.size), expr.alias))
            aggrExpr.add(innerExpr)
          }
          else -> {
            projection.add(ColumnIndex(groupCount))
            groupCount += 1
          }
        }
      }
      plan =
          planAggregateQuery(
              projectionExpr, select, columnNamesInSelection, plan, aggrExpr)
      plan = plan.project(projection)
      if (select.having != null) {
        plan = plan.filter(createLogicalExpr(select.having, plan))
      }
      if (select.limit != null) {
        plan = plan.limit(select.limit)
      }
      return plan
    }
  }

  private fun isAggregateExpr(expr: LogicalExpr): Boolean {
    return when (expr) {
      is AggregateExpr -> true
      is Alias -> expr.expr is AggregateExpr
      else -> false
    }
  }

  private fun planNonAggregateQuery(
      select: SqlSelect,
      df: DataFrame,
      projectionExpr: List<LogicalExpr>,
      columnNamesInSelection: Set<String>,
      columnNamesInProjection: Set<String>
  ): DataFrame {

    var plan = df
    if (select.selection == null) {
      plan = plan.project(projectionExpr)
      if (select.limit != null) {
        plan = plan.limit(select.limit)
      }
      return plan
    }

    val missing = (columnNamesInSelection - columnNamesInProjection)
    logger.info("** missing: $missing")

    // if the selection only references outputs from the projection we can simply apply the filter
    // expression
    // to the DataFrame representing the projection
    if (missing.isEmpty()) {
      plan = plan.project(projectionExpr)
      plan = plan.filter(createLogicalExpr(select.selection, plan))
    } else {

      // because the selection references some columns that are not in the projection output we need
      // to create an
      // interim projection that has the additional columns and then we need to remove them after
      // the selection
      // has been applied
      val n = projectionExpr.size

      plan = plan.project(projectionExpr + missing.map { Column(it) })
      plan = plan.filter(createLogicalExpr(select.selection, plan))

      // drop the columns that were added for the selection
      val expr = (0 until n).map { i -> Column(plan.schema().fields[i].name) }
      plan = plan.project(expr)
    }

    if (select.limit != null) {
      plan = plan.limit(select.limit)
    }

    return plan
  }

  private fun planAggregateQuery(
      projectionExpr: List<LogicalExpr>,
      select: SqlSelect,
      columnNamesInSelection: Set<String>,
      df: DataFrame,
      aggregateExpr: List<AggregateExpr>
  ): DataFrame {
    var plan = df
    val projectionWithoutAggregates =
        projectionExpr.filterNot { isAggregateExpr(it) }

    // Get columns referenced by aggregate expressions - these must be available in the
    // aggregate's input
    val columnNamesInAggregates = getReferencedColumns(aggregateExpr)

    if (select.selection != null) {

      val columnNamesInProjectionWithoutAggregates =
          getReferencedColumns(projectionWithoutAggregates)

      // Include columns needed by selection AND by aggregate expressions
      val allRequiredColumns =
          columnNamesInProjectionWithoutAggregates +
              columnNamesInSelection +
              columnNamesInAggregates
      val missing =
          (allRequiredColumns - columnNamesInProjectionWithoutAggregates)
      logger.info("** missing: $missing")

      // if the selection only references outputs from the projection we can simply apply the filter
      // expression
      // to the DataFrame representing the projection
      if (missing.isEmpty()) {
        plan = plan.project(projectionWithoutAggregates)
        plan = plan.filter(createLogicalExpr(select.selection, plan))
      } else {
        // because the selection references some columns that are not in the projection output we
        // need to create an
        // interim projection that has the additional columns and then we need to remove them after
        // the selection
        // has been applied
        plan =
            plan.project(
                projectionWithoutAggregates + missing.map { Column(it) })
        plan = plan.filter(createLogicalExpr(select.selection, plan))
      }
    }

    val groupByExpr = select.groupBy.map { createLogicalExpr(it, plan) }
    return plan.aggregate(groupByExpr, aggregateExpr)
  }

  private fun getColumnsReferencedBySelection(
      select: SqlSelect,
      table: DataFrame
  ): Set<String> {
    val accumulator = mutableSetOf<String>()
    if (select.selection != null) {
      var filterExpr = createLogicalExpr(select.selection, table)
      visit(filterExpr, accumulator)
      val validColumnNames = table.schema().fields.map { it.name }
      accumulator.removeIf { name -> !validColumnNames.contains(name) }
    }
    return accumulator
  }

  private fun getReferencedColumns(exprs: List<LogicalExpr>): Set<String> {
    val accumulator = mutableSetOf<String>()
    exprs.forEach { visit(it, accumulator) }
    return accumulator
  }

  private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
    //        logger.info("BEFORE visit() $expr, accumulator=$accumulator")
    when (expr) {
      is Column -> accumulator.add(expr.name)
      is Alias -> visit(expr.expr, accumulator)
      is BinaryExpr -> {
        visit(expr.l, accumulator)
        visit(expr.r, accumulator)
      }
      is AggregateExpr -> visit(expr.expr, accumulator)
    }
    //        logger.info("AFTER visit() $expr, accumulator=$accumulator")
  }

  private fun createLogicalExpr(expr: SqlExpr, input: DataFrame): LogicalExpr {
    return when (expr) {
      is SqlIdentifier -> Column(expr.id)
      is SqlString -> LiteralString(expr.value)
      is SqlLong -> LiteralLong(expr.value)
      is SqlDouble -> LiteralDouble(expr.value)
      is SqlDate -> LiteralDate(java.time.LocalDate.parse(expr.value))
      is SqlInterval -> parseInterval(expr.value)
      is SqlBinaryExpr -> {
        val l = createLogicalExpr(expr.l, input)
        val r = createLogicalExpr(expr.r, input)
        when (expr.op) {
          // comparison operators
          "=" -> Eq(l, r)
          "!=",
          "<>" -> Neq(l, r)
          ">" -> Gt(l, r)
          ">=" -> GtEq(l, r)
          "<" -> Lt(l, r)
          "<=" -> LtEq(l, r)
          // boolean operators
          "AND" -> And(l, r)
          "OR" -> Or(l, r)
          // math operators
          "+" ->
              if (l is LiteralDate && r is LiteralIntervalDays) {
                DateAddInterval(l, r)
              } else {
                Add(l, r)
              }
          "-" ->
              if (l is LiteralDate && r is LiteralIntervalDays) {
                DateSubtractInterval(l, r)
              } else {
                Subtract(l, r)
              }
          "*" -> Multiply(l, r)
          "/" -> Divide(l, r)
          "%" -> Modulus(l, r)
          else -> throw SQLException("Invalid operator ${expr.op}")
        }
      }
      // is SqlUnaryExpr -> when (expr.op) {
      // "NOT" -> Not(createLogicalExpr(expr.l, input))
      // }
      is SqlAlias -> Alias(createLogicalExpr(expr.expr, input), expr.alias.id)
      is SqlCast ->
          CastExpr(
              createLogicalExpr(expr.expr, input),
              parseDataType(expr.dataType.id))
      is SqlFunction ->
          when (expr.id.uppercase()) {
            "MIN",
            "MAX",
            "SUM",
            "AVG" -> {
              if (expr.args.isEmpty()) {
                throw SQLException(
                    "${expr.id.uppercase()}() requires an argument")
              }
              val arg = createLogicalExpr(expr.args.first(), input)
              when (expr.id.uppercase()) {
                "MIN" -> Min(arg)
                "MAX" -> Max(arg)
                "SUM" -> Sum(arg)
                "AVG" -> Avg(arg)
                else -> throw SQLException("Unexpected aggregate function")
              }
            }
            "COUNT" -> {
              if (expr.args.isEmpty()) {
                throw SQLException(
                    "COUNT() requires an argument, use COUNT(*) to count all rows")
              }
              val arg = expr.args.first()
              if (arg is SqlIdentifier && arg.id == "*") {
                Count(LiteralLong(1))
              } else {
                Count(createLogicalExpr(arg, input))
              }
            }
            else -> throw SQLException("Invalid aggregate function: ${expr.id}")
          }
      else ->
          throw SQLException(
              "Cannot create logical expression from sql expression: $expr")
    }
  }

  private fun parseDataType(id: String): ArrowType {
    return when (id) {
      "double" -> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      else -> throw SQLException("Invalid data type $id")
    }
  }

  private fun parseInterval(value: String): LiteralIntervalDays {
    val regex = Regex("(\\d+)\\s+days?", RegexOption.IGNORE_CASE)
    val match =
        regex.matchEntire(value.trim())
            ?: throw SQLException(
                "Invalid interval format: '$value'. Expected format: 'N days'")
    val days = match.groupValues[1].toLong()
    return LiteralIntervalDays(days)
  }
}
