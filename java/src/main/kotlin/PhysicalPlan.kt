package io.andygrove.ballista

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

sealed class PhysicalPlan {
    class Projection(val expr: List<Expr>): PhysicalPlan()
    class Selection(val expr: Expr): PhysicalPlan()
    class HashAggregate(val groupBy: List<Expr>, aggregates: List<AggregateExpr>) : PhysicalPlan()
    class Offset(val n: Int): PhysicalPlan()
    class Limit(val n: Int): PhysicalPlan()
}