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
import io.andygrove.kquery.datasource.ParquetDataSource
import io.andygrove.kquery.datatypes.ArrowTypes
import io.andygrove.kquery.datatypes.Field
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.physical.*
import io.andygrove.kquery.physical.expressions.*
import org.apache.arrow.vector.types.pojo.ArrowType as ArrowTypeClass

/** Utility to convert from protobuf representation to physical plan. */
class PhysicalPlanDeserializer {

  /** Convert a protobuf physical plan to a PhysicalPlan */
  fun fromProto(node: PhysicalPlanNode): PhysicalPlan {
    return when {
      node.hasScan() -> {
        val scan = node.scan
        val schema = fromProto(scan.schema)
        val ds =
            when (scan.fileFormat) {
              "csv" -> CsvDataSource(scan.path, schema, true, 1024)
              "parquet" -> ParquetDataSource(scan.path)
              else ->
                  throw IllegalStateException(
                      "Unsupported file format: ${scan.fileFormat}")
            }
        ScanExec(ds, scan.projectionList.toList())
      }
      node.hasProjection() -> {
        val proj = node.projection
        val input = fromProto(proj.input)
        val schema = fromProto(proj.schema)
        val expr = proj.exprList.map { fromProto(it) }
        ProjectionExec(input, schema, expr)
      }
      node.hasSelection() -> {
        val sel = node.selection
        val input = fromProto(sel.input)
        val expr = fromProto(sel.expr)
        SelectionExec(input, expr)
      }
      node.hasHashAggregate() -> {
        val agg = node.hashAggregate
        val input = fromProto(agg.input)
        val groupExpr = agg.groupExprList.map { fromProto(it) }
        val aggregateExpr = agg.aggregateExprList.map { fromProtoAggr(it) }
        val schema = fromProto(agg.schema)
        val mode =
            when (agg.mode) {
              AggregateMode.COMPLETE ->
                  io.andygrove.kquery.physical.AggregateMode.COMPLETE
              AggregateMode.PARTIAL ->
                  io.andygrove.kquery.physical.AggregateMode.PARTIAL
              AggregateMode.FINAL ->
                  io.andygrove.kquery.physical.AggregateMode.FINAL
              else -> io.andygrove.kquery.physical.AggregateMode.COMPLETE
            }
        HashAggregateExec(input, groupExpr, aggregateExpr, schema, mode)
      }
      node.hasShuffleWriter() -> {
        val sw = node.shuffleWriter
        val input = fromProto(sw.input)
        val partitionExpr = sw.partitionExprList.map { fromProto(it) }
        ShuffleWriterExec(
            input, partitionExpr, sw.jobUuid, sw.stageId, sw.partitionCount)
      }
      node.hasShuffleReader() -> {
        val sr = node.shuffleReader
        val schema = fromProto(sr.schema)
        val locations = sr.shuffleLocationsList.map { fromProto(it) }
        ShuffleReaderExec(schema, locations)
      }
      else ->
          throw RuntimeException("Failed to parse physical plan node: $node")
    }
  }

  /** Convert a protobuf physical expression to Expression */
  fun fromProto(node: PhysicalExprNode): Expression {
    return when (node.exprTypeCase) {
      PhysicalExprNode.ExprTypeCase.COLUMN -> {
        ColumnExpression(node.column)
      }
      PhysicalExprNode.ExprTypeCase.LITERAL_STRING -> {
        LiteralStringExpression(node.literalString)
      }
      PhysicalExprNode.ExprTypeCase.LITERAL_LONG -> {
        LiteralLongExpression(node.literalLong)
      }
      PhysicalExprNode.ExprTypeCase.LITERAL_DOUBLE -> {
        LiteralDoubleExpression(node.literalDouble)
      }
      PhysicalExprNode.ExprTypeCase.LITERAL_DATE -> {
        LiteralDateExpression(node.literalDate)
      }
      PhysicalExprNode.ExprTypeCase.BINARY_EXPR -> {
        val binary = node.binaryExpr
        val l = fromProto(binary.l)
        val r = fromProto(binary.r)
        when (binary.op) {
          "eq" -> EqExpression(l, r)
          "neq" -> NeqExpression(l, r)
          "lt" -> LtExpression(l, r)
          "lteq" -> LtEqExpression(l, r)
          "gt" -> GtExpression(l, r)
          "gteq" -> GtEqExpression(l, r)
          "and" -> AndExpression(l, r)
          "or" -> OrExpression(l, r)
          "add" -> AddExpression(l, r)
          "subtract" -> SubtractExpression(l, r)
          "multiply" -> MultiplyExpression(l, r)
          "divide" -> DivideExpression(l, r)
          else ->
              throw RuntimeException(
                  "Unsupported binary operator: ${binary.op}")
        }
      }
      PhysicalExprNode.ExprTypeCase.CAST_EXPR -> {
        val cast = node.castExpr
        val expr = fromProto(cast.expr)
        val arrowType = fromProtoArrowType(cast.arrowType)
        CastExpression(expr, arrowType)
      }
      PhysicalExprNode.ExprTypeCase.EXPRTYPE_NOT_SET,
      null ->
          throw RuntimeException("Physical expression type not set in protobuf")
    }
  }

  /** Convert a protobuf aggregate expression to AggregateExpression */
  fun fromProtoAggr(node: PhysicalAggregateExprNode): AggregateExpression {
    val inputExpr = fromProto(node.inputExpr)
    return when (node.aggrFunction) {
      AggregateFunction.SUM -> SumExpression(inputExpr)
      AggregateFunction.MIN -> MinExpression(inputExpr)
      AggregateFunction.MAX -> MaxExpression(inputExpr)
      AggregateFunction.AVG -> AvgExpression(inputExpr)
      AggregateFunction.COUNT -> CountExpression(inputExpr)
      else ->
          throw RuntimeException(
              "Unsupported aggregate function: ${node.aggrFunction}")
    }
  }

  /** Convert a protobuf schema to Schema */
  fun fromProto(schema: io.andygrove.kquery.protobuf.Schema): Schema {
    return Schema(schema.columnsList.map { fromProto(it) })
  }

  /** Convert a protobuf field to Field */
  fun fromProto(field: io.andygrove.kquery.protobuf.Field): Field {
    return Field(field.name, fromProtoArrowType(field.arrowType))
  }

  /** Convert a protobuf Arrow type enum to Arrow type */
  fun fromProtoArrowType(arrowType: ArrowType): ArrowTypeClass {
    return when (arrowType) {
      ArrowType.BOOL -> ArrowTypes.BooleanType
      ArrowType.INT8 -> ArrowTypes.Int8Type
      ArrowType.INT16 -> ArrowTypes.Int16Type
      ArrowType.INT32 -> ArrowTypes.Int32Type
      ArrowType.INT64 -> ArrowTypes.Int64Type
      ArrowType.UINT8 -> ArrowTypes.UInt8Type
      ArrowType.UINT16 -> ArrowTypes.UInt16Type
      ArrowType.UINT32 -> ArrowTypes.UInt32Type
      ArrowType.UINT64 -> ArrowTypes.UInt64Type
      ArrowType.FLOAT -> ArrowTypes.FloatType
      ArrowType.DOUBLE -> ArrowTypes.DoubleType
      ArrowType.UTF8 -> ArrowTypes.StringType
      ArrowType.DATE32 -> ArrowTypes.DateDayType
      else ->
          throw IllegalStateException(
              "Cannot deserialize Arrow type from protobuf: $arrowType")
    }
  }

  /** Convert a protobuf ShuffleLocation to ShuffleLocation */
  fun fromProto(
      loc: ShuffleLocation
  ): io.andygrove.kquery.physical.ShuffleLocation {
    return io.andygrove.kquery.physical.ShuffleLocation(
        loc.jobUuid,
        loc.stageId,
        loc.partitionId,
        loc.executorId,
        loc.executorHost,
        loc.executorPort)
  }

  /** Convert a protobuf TaskInfo to Task */
  fun fromProto(task: TaskInfo): Task {
    return Task(
        task.jobUuid,
        task.stageId,
        task.taskId,
        task.partitionId,
        fromProto(task.plan))
  }
}
