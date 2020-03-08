package org.ballistacompute.sql

import org.ballistacompute.logical.*

import java.sql.SQLException
import java.util.logging.Logger

/**
 * SqlPlanner creates a logical plan from a parsed SQL statement.
 */
class SqlPlanner {

    private val logger = Logger.getLogger(SqlPlanner::class.simpleName)

    /**
     * Create logical plan from parsed SQL statement.
     */
    fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {

        // get a reference to the data source
        var df = tables[select.tableName] ?: throw SQLException("No table named '${select.tableName}'")

        // create the logical expressions for the projection
        val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

        if (select.selection == null) {
            // if there is no selection then we can just return the projection
            return df.select(projectionExpr)
        }

        // create the logical expression to represent the selection
        val filterExpr = createLogicalExpr(select.selection, df)

        // get a list of columns references in the projection expression
        val columnsInProjection = projectionExpr
            .map { it.toField(df.logicalPlan()).name}
            .toSet()
        logger.info("projection references columns $columnsInProjection")

        // get a list of columns referenced in the selection expression
        val columnNames = mutableSetOf<String>()
        visit(filterExpr, columnNames)
        logger.info("selection references columns: $columnNames")

        // determine if the selection references any columns not in the projection
        val missing = columnNames - columnsInProjection
        logger.info("** missing: $missing")

        // if the selection only references outputs from the projection we can simply apply the filter expression
        // to the DataFrame representing the projection
        if (missing.size == 0) {
            return df.select(projectionExpr)
                .filter(filterExpr)
        }

        // because the selection references some columns that are not in the projection output we need to create an
        // interim projection that has the additional columns and then we need to remove them after the selection
        // has been applied
        return df.select(projectionExpr + missing.map { Column(it) })
            .filter(filterExpr)
            .select(projectionExpr.map { Column(it.toField(df.logicalPlan()).name) })
    }

    private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
        logger.info("visit() $expr, accumulator=$accumulator")
        when (expr) {
            is Column -> accumulator.add(expr.name)
            is Alias -> visit(expr.expr, accumulator)
            is BinaryExpr -> {
                visit(expr.l, accumulator)
                visit(expr.r, accumulator)
            }
        }
    }

    private fun createLogicalExpr(expr: SqlExpr, input: DataFrame) : LogicalExpr {
        return when (expr) {
            is SqlIdentifier -> Column(expr.id)
            is SqlString -> LiteralString(expr.value)
            is SqlLong -> LiteralLong(expr.value)
            is SqlDouble -> LiteralDouble(expr.value)
            is SqlBinaryExpr -> when(expr.op) {
                // comparison operators
                "=" -> Eq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "!=" -> Neq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                ">" -> Gt(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                ">=" -> GtEq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "<" -> Lt(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "<=" -> LtEq(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                // boolean operators
                "AND" -> And(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "OR" -> Or(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                // math operators
                "+" -> Add(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "-" -> Subtract(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "*" -> Multiply(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "/" -> Divide(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                "%" -> Modulus(createLogicalExpr(expr.l, input), createLogicalExpr(expr.r, input))
                //TODO add other math operations
                else -> TODO("Binary operator ${expr.op}")
            }
            //is SqlUnaryExpr -> when (expr.op) {
            //"NOT" -> Not(createLogicalExpr(expr.l, input))
            //}
            is SqlAlias -> Alias(createLogicalExpr(expr.expr, input), expr.alias.id)
            is SqlFunction -> when(expr.id) {
                "MAX" -> Max(createLogicalExpr(expr.args.first(), input))
                else -> TODO()
            }
            else -> TODO(expr.javaClass.toString())
        }
    }

}