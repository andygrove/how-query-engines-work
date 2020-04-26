package org.ballistacompute.physical.expressions

import java.util.*

//TODO null handling - either or both sides could be null

class EqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {

    override fun evaluate(l: Any?, r: Any?) : Boolean {
        //TODO
        return if (l is ByteArray) {
            if (r is ByteArray) {
                Arrays.equals(l, r)
            } else if (r is String) {
                Arrays.equals(l, r.toByteArray())
            } else {
                TODO()
            }
        } else {
            l == r
        }
    }
}

class NeqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?): Boolean {
        return when (l) {
            is Float -> l != r as Float
            is Double -> l != r as Double
            else -> TODO()
        }
    }
}

class LtExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?): Boolean {
        return when (l) {
            is Float -> l < r as Float
            is Double -> l < r as Double
            else -> TODO()
        }
    }
}

class LtEqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?): Boolean {
        return when (l) {
            is Float -> l <= r as Float
            is Double -> l <= r as Double
            else -> TODO()
        }
    }
}

class GtExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?): Boolean {
        return when (l) {
            is Float -> l > r as Float
            is Double -> l > r as Double
            else -> TODO()
        }
    }
}

class GtEqExpression(l: Expression, r: Expression): ComparisonExpression(l,r) {
    override fun evaluate(l: Any?, r: Any?): Boolean {
        return when (l) {
            is Float -> l >= r as Float
            is Double -> l >= r as Double
            else -> TODO()
        }
    }
}
