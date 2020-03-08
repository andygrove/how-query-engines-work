package org.ballistacompute.optimizer

import org.ballistacompute.logical.*
import java.lang.IllegalStateException

class ProjectionPushDownRule : OptimizerRule {

    override fun optimize(plan: LogicalPlan): LogicalPlan {
        return pushDown(plan, mutableSetOf())
    }

    private fun pushDown(plan: LogicalPlan,
                         columnNames: MutableSet<String>): LogicalPlan {
        return when (plan) {
            is Projection -> {
                extractColumns(plan.expr, columnNames)
                val input = pushDown(plan.input, columnNames)
                Projection(input, plan.expr)
            }
            is Selection -> {
                extractColumns(plan.expr, columnNames)
                val input = pushDown(plan.input, columnNames)
                Selection(input, plan.expr)
            }
            is Aggregate -> {
                extractColumns(plan.groupExpr, columnNames)
                extractColumns(plan.aggregateExpr.map { it.expr }, columnNames)
                val input = pushDown(plan.input, columnNames)
                Aggregate(input, plan.groupExpr, plan.aggregateExpr)
            }
            is Scan -> Scan(plan.name, plan.dataSource, columnNames.toList().sorted())
            else -> TODO(plan.javaClass.name)
        }
    }

}