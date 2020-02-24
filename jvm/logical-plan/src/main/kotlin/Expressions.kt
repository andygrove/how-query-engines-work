package io.andygrove.kquery.logical

import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import java.sql.SQLException

/**
 * Logical expression representing a reference to a column by name.
 */
class Column(val name: String): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields.find { it.name == name } ?: throw SQLException("No column named '$name'")
    }

    override fun toString(): String {
        return "#$name"
    }

}

/** Convenience method to create a Column reference */
fun col(name: String) = Column(name)

/**
 * Logical expression representing a reference to a column by index.
 */
class ColumnIndex(val i: Int): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields[i]
    }

    override fun toString(): String {
        return "#$i"
    }

}

/**
 * Logical expression representing a literal string value.
 */
class LiteralString(val str: String): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(str, ArrowType.Utf8())
    }

    override fun toString(): String {
        return "'$str'"
    }

}

/** Convenience method to create a LiteralString */
fun lit(value: String) = LiteralString(value)

/**
 * Logical expression representing a literal long value.
 */
class LiteralLong(val n: Long): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(n.toString(), ArrowType.Int(32, false))
    }

    override fun toString(): String {
        return n.toString()
    }

}

/** Convenience method to create a LiteralLong */
fun lit(value: Long) = LiteralLong(value)

/**
 * Logical expression representing a literal double value.
 */
class LiteralDouble(val n: Double): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(n.toString(), ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
    }

    override fun toString(): String {
        return n.toString()
    }

}

/** Convenience method to create a LiteralDouble */
fun lit(value: Double) = LiteralDouble(value)

class CastExpr(val expr: LogicalExpr, val dataType: ArrowType.PrimitiveType) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field.nullablePrimitive(expr.toField(input).name, dataType)
    }

    override fun toString(): String {
        return "CAST($expr AS $dataType)"
    }
}

fun cast(expr: LogicalExpr, dataType: ArrowType.PrimitiveType) = CastExpr(expr, dataType)

abstract class BinaryExpr(val name: String,
                          val op: String,
                          val l: LogicalExpr,
                          val r: LogicalExpr) : LogicalExpr {

    override fun toString(): String {
        return "$l $op $r"
    }
}

/** Boolean expressions are binary expressions that return a boolean type */
abstract class BooleanExpr(name: String,
                           op: String,
                           l: LogicalExpr,
                           r: LogicalExpr) : BinaryExpr(name, op, l, r) {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullablePrimitive(name, ArrowType.Bool())
    }

}

/** Logical expression representing a logical AND */
class And(l: LogicalExpr, r: LogicalExpr): BooleanExpr("and", "AND", l, r)

/** Logical expression representing a logical AND */
class Or(l: LogicalExpr, r: LogicalExpr): BooleanExpr("or", "OR", l, r)

/** Logical expression representing an equality (`=`) comparison */
class Eq(l: LogicalExpr, r: LogicalExpr): BooleanExpr("eq", "=", l, r)

/** Logical expression representing an inequality (`!=`) comparison */
class Neq(l: LogicalExpr, r: LogicalExpr): BooleanExpr("neq", "!=", l, r)

/** Logical expression representing a greater than (`>`) comparison */
class Gt(l: LogicalExpr, r: LogicalExpr): BooleanExpr("gt", ">", l, r)

/** Logical expression representing a greater than or equals (`>=`) comparison */
class GtEq(l: LogicalExpr, r: LogicalExpr): BooleanExpr("gteq", ">=", l, r)

/** Logical expression representing a less than (`<`) comparison */
class Lt(l: LogicalExpr, r: LogicalExpr): BooleanExpr("lt", "<", l, r)

/** Logical expression representing a less than or equals (`<=`) comparison */
class LtEq(l: LogicalExpr, r: LogicalExpr): BooleanExpr("lteq", "<=", l, r)

/** Convenience method to create an equality expression using an infix operator */
infix fun LogicalExpr.eq(rhs: LogicalExpr): LogicalExpr { return Eq(this, rhs) }

/** Convenience method to create an inequality expression using an infix operator */
infix fun LogicalExpr.neq(rhs: LogicalExpr): LogicalExpr { return Neq(this, rhs) }

/** Convenience method to create a greater than expression using an infix operator */
infix fun LogicalExpr.gt(rhs: LogicalExpr): LogicalExpr { return Gt(this, rhs) }

/** Convenience method to create a greater than or equals expression using an infix operator */
infix fun LogicalExpr.gteq(rhs: LogicalExpr): LogicalExpr { return GtEq(this, rhs) }

/** Convenience method to create a less than expression using an infix operator */
infix fun LogicalExpr.lt(rhs: LogicalExpr): LogicalExpr { return Lt(this, rhs) }

/** Convenience method to create a less than or equals expression using an infix operator */
infix fun LogicalExpr.lteq(rhs: LogicalExpr): LogicalExpr { return LtEq(this, rhs) }

class Mult(val l: LogicalExpr, val r: LogicalExpr) : LogicalExpr {

    //TODO type checking

    override fun toField(input: LogicalPlan): Field {
        return Field.nullablePrimitive("mult", l.toField(input).type as ArrowType.PrimitiveType)
    }

    override fun toString(): String {
        return "$l * $r"
    }
}

/** Convenience method to create a multiplication expression using an infix operator */
infix fun LogicalExpr.mult(rhs: LogicalExpr): Mult {
    return Mult(this, rhs)
}


/** Aliased expression e.g. `expr AS alias`. */
class Alias(val expr: LogicalExpr, val alias: String) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field.nullablePrimitive(alias, expr.toField(input).type as ArrowType.PrimitiveType)
    }

    override fun toString(): String {
        return "$expr as $alias"
    }
}

/** Convenience method to wrap the current expression in an alias using an infix operator */
infix fun LogicalExpr.alias(alias: String) : Alias {
    return Alias(this, alias)
}

/**
 * Base interface for all aggregate expressions.
 */
interface AggregateExpr {

    /**
     * Return meta-data about the value that will be produced by this expression when evaluated against
     * a particular input.
     */
    fun toField(input: LogicalPlan): Field
}

/** Base class for aggregate functions that are of the same type as the underlying expression */
abstract class BaseAggregateExpr(val name: String, val e: LogicalExpr) : AggregateExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable(name, e.toField(input).type)
    }

    override fun toString(): String {
        return "$name($e)"
    }
}

/**
 * Logical expression representing the SUM aggregate expression.
 */
class Sum(e: LogicalExpr) : BaseAggregateExpr("SUM", e)

/**
 * Logical expression representing the MIN aggregate expression.
 */
class Min(e: LogicalExpr) : BaseAggregateExpr("MIN", e)

/**
 * Logical expression representing the MAX aggregate expression.
 */
class Max(e: LogicalExpr) : BaseAggregateExpr("MAX", e)

/**
 * Logical expression representing the AVG aggregate expression.
 */
class Avg(e: LogicalExpr) : BaseAggregateExpr("AVG", e)

/** Logical expression representing the COUNT aggregate expression. */
class Count(val e: LogicalExpr) : AggregateExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable("COUNT", ArrowType.Int(32, false))
    }

    override fun toString(): String {
        return "COUNT($e)"
    }
}

/** Logical expression representing the COUNT DISTINCT aggregate expression. */
class CountDistinct(val e: LogicalExpr) : AggregateExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field.nullable("COUNT_DISTINCT", ArrowType.Int(32, false))
    }

    override fun toString(): String {
        return "COUNT(DISTINCT $e)"
    }
}