package org.ballistacompute.optimizer

import org.ballistacompute.logical.*

class Optimizer() {

    fun optimize(plan: LogicalPlan) : LogicalPlan {
        // note there is only one rule implemented so far but later there will be a list
        val rule = ProjectionPushDownRule()
        return rule.optimize(plan)
    }

}

interface OptimizerRule {
    fun optimize(plan: LogicalPlan) : LogicalPlan
}

fun extractColumns(expr: List<LogicalExpr>, accum: MutableSet<String>) {
    expr.forEach { extractColumns(it, accum) }
}

fun extractColumns(expr: LogicalExpr, accum: MutableSet<String>) {
    when (expr) {
        is Column -> accum.add(expr.name)
        is BinaryExpr -> {
            extractColumns(expr.l, accum)
            extractColumns(expr.r, accum)
        }
        is Alias -> extractColumns(expr.expr, accum)
        is CastExpr -> extractColumns(expr.expr, accum)
        is LiteralString -> {}
        is LiteralLong -> {}
        is LiteralDouble -> {}
        else -> TODO(expr.javaClass.name)
    }
}

