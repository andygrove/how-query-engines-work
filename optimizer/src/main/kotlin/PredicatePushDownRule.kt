package io.andygrove.kquery.optimizer

import io.andygrove.kquery.logical.Aggregate
import io.andygrove.kquery.logical.Column
import io.andygrove.kquery.logical.Join
import io.andygrove.kquery.logical.JoinType
import io.andygrove.kquery.logical.Limit
import io.andygrove.kquery.logical.LogicalExpr
import io.andygrove.kquery.logical.LogicalPlan
import io.andygrove.kquery.logical.Projection
import io.andygrove.kquery.logical.Scan
import io.andygrove.kquery.logical.Selection

class PredicatePushDownRule : OptimizerRule {

    /** Return the plan with every Selection pushed as low as it can legally go. */
    override fun optimize(plan: LogicalPlan): LogicalPlan {
        return when (plan) {
            is Scan -> plan  // we can't go lower than Scan, it's always first
            is Selection -> {
                // first we try to optimize whole subtree. We can try lowering selection only into the subtree that is already fully optimized
                val optimizedInput = optimize(plan.input)
                pushDown(plan.expr, optimizedInput)
            }
            // we stop pushdown on limit - but optimization can just step around it. so we call optimize on subtree and re-create limit on result
            is Limit -> Limit(optimize(plan.input), plan.limit)
            // in join, projection and aggregate we just try to optimize children independently, nothing else to do and no blockers
            is Join ->
                Join(optimize(plan.left), optimize(plan.right), plan.joinType, plan.on)
            is Projection -> Projection(optimize(plan.input), plan.expr)
            is Aggregate ->
                Aggregate(optimize(plan.input), plan.groupExpr, plan.aggregateExpr)
            else -> throw IllegalStateException(
                "PredicatePushDownRule does not support plan: $plan")
        }
    }

    /**
     * Place [expr] as low as possible in [plan]. [plan] must already be optimized.
     */
    private fun pushDown(expr: LogicalExpr, plan: LogicalPlan): LogicalPlan {
        return when (plan) {
            // we can't go below any of these. So we return Selection, and current plan (scan, limit or another selection) is it's child
            is Scan, is Limit, is Selection -> Selection(plan, expr)
            is Projection ->
                if (passesThrough(expr, plan, plan.expr)) {  // if all Selection columns are still available below - keep pushing.
                    Projection(pushDown(expr, plan.input), plan.expr)
                } else {  // otherwise stop and return Selection with Projection as child - we can't go on.
                    Selection(plan, expr)
                }
            is Aggregate ->
                if (passesThrough(expr, plan, plan.groupExpr)) { // same as above. now we can only check groupExpr. as all Selection columns required to be in groupExpr, it automatically st[...] (rest of comment cut off in photo)
                    Aggregate(pushDown(expr, plan.input), plan.groupExpr, plan.aggregateExpr)
                } else {
                    Selection(plan, expr)
                }
            is Join -> {
                val filterColumns = extractColumns(expr, plan)
                val onLeft = columnNames(plan.left).containsAll(filterColumns)
                val onRight = columnNames(plan.right).containsAll(filterColumns)
                val (leftAllowed, rightAllowed) = when (plan.joinType) {
                    JoinType.Inner -> Pair(true, true)
                    JoinType.Left -> Pair(true, false) // in left join we can push left, but we can never push right
                    JoinType.Right -> Pair(false, true)  // same in right join - we can never push left
                }
                when {
                    // if just columns on the left part are used for filter - we can push filter lower, and filter before join
                    leftAllowed && onLeft -> // so if it's a left join OR inner join, and all selection columns on the left - we can push further
                        Join(pushDown(expr, plan.left), plan.right, plan.joinType, plan.on)
                    rightAllowed && onRight ->
                        Join(plan.left, pushDown(expr, plan.right), plan.joinType, plan.on)
                    // if columns from both left and right are used, or columns from null-supplying side (right in case of left join) we can't push lower, so stop
                    else -> Selection(plan, expr)
                }
            }
            else -> throw IllegalStateException(
                "PredicatePushDownRule does not support plan: $plan")
        }
    }

    // same idea as isAvailableBelow but being extra-cautious about sameness of columns. e.g. salary * 12 as salary will pass name check, but is not the same
    // so it will fail this filter, as it only allows simple Columns to the final comparison list
    private fun passesThrough(
        expr: LogicalExpr,
        plan: LogicalPlan,
        outputs: List<LogicalExpr>
    ): Boolean {
        val filterColumns = extractColumns(expr, plan)
        val plainColumns = outputs.filterIsInstance<Column>().map { it.name }.toSet()
        return plainColumns.containsAll(filterColumns)
    }

    private fun columnNames(plan: LogicalPlan): Set<String> {
        return plan.schema().fields.map { it.name }.toSet()
    }
}
