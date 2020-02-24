package io.andygrove.kquery.logical

interface DataFrame {

    /** Apply a projection */
    fun select(expr: List<LogicalExpr>): DataFrame

    /** Apply a filter */
    fun filter(expr: LogicalExpr): DataFrame

    /** Aggregate */
    fun aggregate(groupBy: List<LogicalExpr>, aggregateExpr: List<AggregateExpr>): DataFrame

    /** Get the logical plan */
    fun logicalPlan() : LogicalPlan

}

class DataFrameImpl(private val plan: LogicalPlan) : DataFrame {

    override fun select(expr: List<LogicalExpr>): DataFrame {
        return DataFrameImpl(Projection(plan, expr))
    }

    override fun filter(expr: LogicalExpr): DataFrame {
        return DataFrameImpl(Selection(plan, expr))
    }

    override fun aggregate(groupBy: List<LogicalExpr>, aggregateExpr: List<AggregateExpr>): DataFrame {
        return DataFrameImpl(Aggregate(plan, groupBy, aggregateExpr))
    }

    override fun logicalPlan(): LogicalPlan {
        return plan
    }


}

