package org.ballistacompute.fuzzer

import org.ballistacompute.logical.*
import java.lang.IllegalStateException
import kotlin.random.Random

class Fuzzer {

    val rand = Random(0)

    fun createPlan(input: DataFrame, depth: Int, maxDepth: Int, maxExprDepth: Int): DataFrame {
        return if (depth == maxDepth) {
            input
        } else {
            val child = createPlan(input, depth+1, maxDepth, maxExprDepth)
            when (rand.nextInt(2)) {
                0 -> {
                    val exprCount = 1.rangeTo(rand.nextInt(1, 5))
                    child.project(exprCount.map { createExpression(child, 0, maxExprDepth)})
                }
                1 -> child.filter(createExpression(input, 0, maxExprDepth))
                else -> throw IllegalStateException()
            }
        }
    }

    fun createExpression(input: DataFrame, depth: Int, maxDepth: Int): LogicalExpr {
        return if (depth == maxDepth) {
            // return a leaf node
            when (rand.nextInt(4)) {
                0 -> ColumnIndex(rand.nextInt(input.schema().fields.size))
                1 -> LiteralDouble(rand.nextDouble())
                2 -> LiteralLong(rand.nextLong())
                3 -> LiteralString(randomString(rand.nextInt(64)))
                else -> throw IllegalStateException()
            }
        } else {
            // binary expressions
            val l = createExpression(input, depth+1, maxDepth)
            val r = createExpression(input, depth+1, maxDepth)
            return when (rand.nextInt(8)) {
                0 -> Eq(l, r)
                1 -> Neq(l, r)
                2 -> Lt(l, r)
                3 -> LtEq(l, r)
                4 -> Gt(l, r)
                5 -> GtEq(l, r)
                6 -> And(l, r)
                7 -> Or(l, r)
                else -> throw IllegalStateException()
            }
        }
    }

    private val charPool : List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
    private fun randomString(len: Int): String {
        return (0 until len)
            .map { i -> rand.nextInt(charPool.size) }
            .map(charPool::get)
            .joinToString("")
    }

}