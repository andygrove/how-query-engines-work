package io.andygrove.kquery.logical

import io.andygrove.kquery.datatypes.Field
import io.andygrove.kquery.datatypes.Schema

enum class JoinType {
  Inner,
  Left,
  Right
}

class Join(
    val left: LogicalPlan,
    val right: LogicalPlan,
    val joinType: JoinType,
    val on: List<Pair<String, String>>
) : LogicalPlan {

  override fun schema(): Schema {
    // snippet-omit-start
    val duplicateKeys =
        on.filter { it -> it.first == it.second }.map { it.first }.toSet()
    val fields: List<Field> =
        when (joinType) {
          JoinType.Inner,
          JoinType.Left -> {
            val leftFields = left.schema().fields
            val rightFields =
                right.schema().fields.filter { it ->
                  !duplicateKeys.contains(it.name)
                }
            leftFields + rightFields
          }
          JoinType.Right -> {
            val leftFields =
                left.schema().fields.filter { it ->
                  !duplicateKeys.contains(it.name)
                }
            val rightFields = right.schema().fields
            leftFields + rightFields
          }
        }
    return Schema(fields)
    // snippet-omit-end
  }

  override fun children(): List<LogicalPlan> {
    return listOf(left, right)
  }

  override fun toString(): String {
    return "Join: type=$joinType, on=$on"
  }
}
