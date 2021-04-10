// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.andygrove.kquery.protobuf

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.datatypes.ArrowTypes
import io.andygrove.kquery.logical.*
import io.andygrove.kquery.physical.QueryAction

class ProtobufDeserializer {
@Throws(RuntimeException::class)
  fun fromProto(node: LogicalPlanNode): LogicalPlan {
    return if (node.hasCsvScan() ) {
      val schema = fromProto(node.csvScan.schema)
      val ds = CsvDataSource(node.csvScan.path, schema, node.csvScan.hasHeader, 1024)
      Scan(node.csvScan.path, ds, node.csvScan.projection.columnsList.asByteStringList().map { it.toString() })
    } else if (node.hasSelection()) {
      Selection(fromProto(node.input), fromProto(node.selection.expr))
    } else if (node.hasProjection()) {
      Projection(fromProto(node.input), node.projection.exprList.map { fromProto(it) })
    } else if (node.hasLimit()) {
      Limit(fromProto(node.input), node.limit.limit)
    } else if (node.hasAggregate()) {
      val input = fromProto(node.input)
      val groupExpr = node.aggregate.groupExprList.map { fromProto(it) }
      val aggrExpr = node.aggregate.aggrExprList.map { fromProto(it) as AggregateExpr }
      Aggregate(input, groupExpr, aggrExpr)
    } else {
      throw RuntimeException("Failed to parse logical operator: $node")
    }
  }

  @Throws(RuntimeException::class)
  fun fromProto(node: LogicalExprNode): LogicalExpr {

      return when (node.exprTypeCase){
          LogicalExprNode.ExprTypeCase.BINARY_EXPR -> {
              val binaryNode = node.binaryExpr
              val ll = fromProto(binaryNode.l)
              val rr = fromProto(binaryNode.r)
              when (binaryNode.op) {
                  "eq" -> Eq(ll, rr)
                  "neq" -> Neq(ll, rr)
                  "lt" -> Lt(ll, rr)
                  "lteq" -> LtEq(ll, rr)
                  "gt" -> Gt(ll, rr)
                  "gteq" -> GtEq(ll, rr)
                  "and" -> And(ll, rr)
                  "or" -> Or(ll, rr)
                  "add" -> Add(ll, rr)
                  "subtract" -> Subtract(ll, rr)
                  "multiply" -> Multiply(ll, rr)
                  "divide" -> Divide(ll, rr)
                  else -> throw RuntimeException("Failed to parse logical binary expression: $node")
              }
          }
          LogicalExprNode.ExprTypeCase.ALIAS -> Alias(fromProto(node.alias.expr), node.alias.alias)
          LogicalExprNode.ExprTypeCase.COLUMN_NAME -> col(node.columnName)
          LogicalExprNode.ExprTypeCase.LITERAL_STRING -> lit(node.literalString)
          LogicalExprNode.ExprTypeCase.LITERAL_INT8 -> lit(node.literalInt8.toLong())
          LogicalExprNode.ExprTypeCase.LITERAL_INT16 -> lit(node.literalInt16.toLong())
          LogicalExprNode.ExprTypeCase.LITERAL_INT32 -> lit(node.literalInt32.toLong())
          LogicalExprNode.ExprTypeCase.LITERAL_INT64 -> lit(node.literalInt64)
          LogicalExprNode.ExprTypeCase.LITERAL_UINT8 -> lit(node.literalUint8.toLong())
          LogicalExprNode.ExprTypeCase.LITERAL_UINT16 -> lit(node.literalUint16.toLong())
          LogicalExprNode.ExprTypeCase.LITERAL_UINT32 -> lit(node.literalUint32.toLong())
          LogicalExprNode.ExprTypeCase.LITERAL_UINT64 -> lit(node.literalUint64)
          LogicalExprNode.ExprTypeCase.LITERAL_F32 -> lit(node.literalF32)
          LogicalExprNode.ExprTypeCase.LITERAL_F64 -> lit(node.literalF64)
          LogicalExprNode.ExprTypeCase.AGGREGATE_EXPR -> {
              val aggr = node.aggregateExpr
              val expr = fromProto(aggr.expr)
              return when (aggr.aggrFunction) {
                  AggregateFunction.MIN -> Min(expr)
                  AggregateFunction.MAX -> Max(expr)
                  AggregateFunction.SUM -> Sum(expr)
                  AggregateFunction.AVG -> Avg(expr)
                  AggregateFunction.COUNT -> Count(expr)
                  AggregateFunction.COUNT_DISTINCT -> CountDistinct(expr)
                  else ->
                      throw RuntimeException(
                              "Failed to parse logical aggregate expression: ${aggr.aggrFunction}")
              }
          }
          LogicalExprNode.ExprTypeCase.IS_NULL_EXPR -> TODO("Requires that IsNullExpr is implemented in kotlin first")
          LogicalExprNode.ExprTypeCase.IS_NOT_NULL_EXPR -> TODO("Requires that IsNotNullExpr is implemented in kotlin first")
          LogicalExprNode.ExprTypeCase.NOT_EXPR -> TODO("Requires that NotExpr is implemented in kotlin first")
          LogicalExprNode.ExprTypeCase.EXPRTYPE_NOT_SET,null -> throw RuntimeException("Found null expr enum when deserializing protobuf logical expression")
      }
  }
    @Throws(IllegalStateException::class)
  fun fromProto(schema: Schema): io.andygrove.kquery.datatypes.Schema {

    val arrowFields =
        schema.columnsList.map {

          // TODO add all types
          val dt =
              when (it.arrowTypeValue) {
                ArrowType.UTF8_VALUE -> ArrowTypes.StringType
                ArrowType.INT8_VALUE -> ArrowTypes.Int8Type
                ArrowType.INT16_VALUE -> ArrowTypes.Int16Type
                ArrowType.INT32_VALUE -> ArrowTypes.Int32Type
                ArrowType.INT64_VALUE -> ArrowTypes.Int64Type
                ArrowType.UINT8_VALUE -> ArrowTypes.UInt8Type
                ArrowType.UINT16_VALUE -> ArrowTypes.UInt16Type
                ArrowType.UINT32_VALUE -> ArrowTypes.UInt32Type
                ArrowType.UINT64_VALUE -> ArrowTypes.UInt64Type
                ArrowType.FLOAT_VALUE -> ArrowTypes.FloatType
                ArrowType.DOUBLE_VALUE -> ArrowTypes.DoubleType
                else ->
                    throw IllegalStateException(
                        "Failed to parse Arrow data type enum from protobuf: ${it.arrowTypeValue}")
              }

          val fieldType = org.apache.arrow.vector.types.pojo.FieldType(true, dt, null)
          org.apache.arrow.vector.types.pojo.Field(it.name, fieldType, listOf())
        }

    return io.andygrove.kquery.datatypes.SchemaConverter
        .fromArrow(org.apache.arrow.vector.types.pojo.Schema(arrowFields))
  }

  fun fromProto(action: Action): io.andygrove.kquery.physical.Action {
    return when {
      action.hasQuery() -> {
        QueryAction(fromProto(action.query))
      }
      action.hasFetchShuffle() -> {
        throw NotImplementedError("Shuffle not found")
      }
      action.hasTask() -> {
        throw NotImplementedError("Kotlin executor doesn't yet support any physical plans")
      }
      else -> {
        throw NotImplementedError("Action is not implemented $action")
      }
    }
  }
}
