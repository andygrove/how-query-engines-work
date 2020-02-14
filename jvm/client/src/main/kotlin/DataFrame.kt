package io.andygrove.ballista.client

interface DataFrame {

    /** Apply a projection */
    fun select(expr: List<Expr>): DataFrame

    /** Apply a filter */
    fun filter(expr: Expr): DataFrame

    /** Read a parquet data source at the given path */
    fun parquet(filename: String)

    /** Execute the query and collect the results */
    fun collect(): Iterator<RecordBatch>

}

interface RecordBatch {
    //TODO
}


sealed class Expr {
    class Column(val name: String)
    class LiteralLong(val n: Long)
    class Add(val expr: Expr) : Expr()
    class Subtract(val expr: Expr) : Expr()
    class Multiply(val expr: Expr) : Expr()
    class Divide(val expr: Expr) : Expr()
}

sealed class AggregateExpr {
    class Min(val expr: Expr): AggregateExpr();
    class Max(val expr: Expr): AggregateExpr();
    class Sum(val expr: Expr): AggregateExpr();
    class Avg(val expr: Expr): AggregateExpr();
    class Count(val expr: Expr): AggregateExpr();
}

sealed class LogicalPlan {
    class Projection(val expr: List<Expr>): LogicalPlan()
    class Selection(val expr: Expr): LogicalPlan()
    class Aggregate(val groupBy: List<Expr>, aggregates: List<AggregateExpr>) : LogicalPlan()
    class Offset(val n: Int): LogicalPlan()
    class Limit(val n: Int): LogicalPlan()
}