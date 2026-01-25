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

import io.andygrove.kquery.datatypes.ArrowTypes
import io.andygrove.kquery.datatypes.Field
import io.andygrove.kquery.datatypes.Schema
import io.andygrove.kquery.physical.*
import io.andygrove.kquery.physical.expressions.*
import org.apache.arrow.vector.types.pojo.ArrowType as ArrowTypeClass

/** Utility to convert between physical plan and protobuf representation. */
class PhysicalPlanSerializer {

  /** Convert a physical plan to a protobuf representation */
  fun toProto(plan: PhysicalPlan): PhysicalPlanNode {
    return when (plan) {
      is ScanExec -> {
        val ds = plan.ds
        val path =
            when (ds) {
              is io.andygrove.kquery.datasource.CsvDataSource -> ds.filename
              is io.andygrove.kquery.datasource.ParquetDataSource -> ds.filename
              else ->
                  throw UnsupportedOperationException(
                      "Unsupported datasource type: ${ds.javaClass.name}")
            }
        val format =
            when (ds) {
              is io.andygrove.kquery.datasource.CsvDataSource -> "csv"
              is io.andygrove.kquery.datasource.ParquetDataSource -> "parquet"
              else -> "unknown"
            }
        PhysicalPlanNode.newBuilder()
            .setScan(
                ScanExecNode.newBuilder()
                    .setPath(path)
                    .setSchema(toProto(plan.schema()))
                    .addAllProjection(plan.projection)
                    .setFileFormat(format)
                    .build())
            .build()
      }
      is ProjectionExec -> {
        PhysicalPlanNode.newBuilder()
            .setProjection(
                ProjectionExecNode.newBuilder()
                    .setInput(toProto(plan.input))
                    .setSchema(toProto(plan.schema))
                    .addAllExpr(plan.expr.map { toProto(it) })
                    .build())
            .build()
      }
      is SelectionExec -> {
        PhysicalPlanNode.newBuilder()
            .setSelection(
                SelectionExecNode.newBuilder()
                    .setInput(toProto(plan.input))
                    .setExpr(toProto(plan.expr))
                    .build())
            .build()
      }
      is HashAggregateExec -> {
        val mode =
            when (plan.mode) {
              io.andygrove.kquery.physical.AggregateMode.COMPLETE ->
                  AggregateMode.COMPLETE
              io.andygrove.kquery.physical.AggregateMode.PARTIAL ->
                  AggregateMode.PARTIAL
              io.andygrove.kquery.physical.AggregateMode.FINAL ->
                  AggregateMode.FINAL
            }
        PhysicalPlanNode.newBuilder()
            .setHashAggregate(
                HashAggregateExecNode.newBuilder()
                    .setInput(toProto(plan.input))
                    .addAllGroupExpr(plan.groupExpr.map { toProto(it) })
                    .addAllAggregateExpr(
                        plan.aggregateExpr.map { toProtoAggr(it) })
                    .setSchema(toProto(plan.schema))
                    .setMode(mode)
                    .build())
            .build()
      }
      is ShuffleWriterExec -> {
        PhysicalPlanNode.newBuilder()
            .setShuffleWriter(
                ShuffleWriterExecNode.newBuilder()
                    .setInput(toProto(plan.input))
                    .addAllPartitionExpr(plan.partitionExpr.map { toProto(it) })
                    .setJobUuid(plan.jobUuid)
                    .setStageId(plan.stageId)
                    .setPartitionCount(plan.partitionCount)
                    .build())
            .build()
      }
      is ShuffleReaderExec -> {
        PhysicalPlanNode.newBuilder()
            .setShuffleReader(
                ShuffleReaderExecNode.newBuilder()
                    .setSchema(toProto(plan.schema()))
                    .addAllShuffleLocations(
                        plan.shuffleLocations.map { toProto(it) })
                    .build())
            .build()
      }
      else ->
          throw IllegalStateException(
              "Cannot serialize physical operator to protobuf: ${plan.javaClass.name}")
    }
  }

  /** Convert a physical expression to a protobuf representation */
  fun toProto(expr: Expression): PhysicalExprNode {
    return when (expr) {
      is ColumnExpression -> {
        PhysicalExprNode.newBuilder().setColumn(expr.i).build()
      }
      is LiteralStringExpression -> {
        PhysicalExprNode.newBuilder().setLiteralString(expr.value).build()
      }
      is LiteralLongExpression -> {
        PhysicalExprNode.newBuilder().setLiteralLong(expr.value).build()
      }
      is LiteralDoubleExpression -> {
        PhysicalExprNode.newBuilder().setLiteralDouble(expr.value).build()
      }
      is LiteralDateExpression -> {
        PhysicalExprNode.newBuilder()
            .setLiteralDate(expr.daysSinceEpoch)
            .build()
      }
      is BooleanExpression -> {
        val op =
            when (expr) {
              is EqExpression -> "eq"
              is NeqExpression -> "neq"
              is LtExpression -> "lt"
              is LtEqExpression -> "lteq"
              is GtExpression -> "gt"
              is GtEqExpression -> "gteq"
              is AndExpression -> "and"
              is OrExpression -> "or"
              else ->
                  throw IllegalStateException(
                      "Cannot serialize boolean expression to protobuf: ${expr.javaClass.name}")
            }
        PhysicalExprNode.newBuilder()
            .setBinaryExpr(
                PhysicalBinaryExprNode.newBuilder()
                    .setL(toProto(expr.l))
                    .setOp(op)
                    .setR(toProto(expr.r))
                    .build())
            .build()
      }
      is MathExpression -> {
        val op =
            when (expr) {
              is AddExpression -> "add"
              is SubtractExpression -> "subtract"
              is MultiplyExpression -> "multiply"
              is DivideExpression -> "divide"
              else ->
                  throw IllegalStateException(
                      "Cannot serialize math expression to protobuf: ${expr.javaClass.name}")
            }
        PhysicalExprNode.newBuilder()
            .setBinaryExpr(
                PhysicalBinaryExprNode.newBuilder()
                    .setL(toProto(expr.l))
                    .setOp(op)
                    .setR(toProto(expr.r))
                    .build())
            .build()
      }
      is CastExpression -> {
        PhysicalExprNode.newBuilder()
            .setCastExpr(
                PhysicalCastExprNode.newBuilder()
                    .setExpr(toProto(expr.expr))
                    .setArrowType(toProtoArrowType(expr.dataType))
                    .build())
            .build()
      }
      else ->
          throw IllegalStateException(
              "Cannot serialize physical expression to protobuf: ${expr.javaClass.name}")
    }
  }

  /** Convert an aggregate expression to protobuf */
  fun toProtoAggr(expr: AggregateExpression): PhysicalAggregateExprNode {
    val fn =
        when (expr) {
          is SumExpression -> AggregateFunction.SUM
          is MinExpression -> AggregateFunction.MIN
          is MaxExpression -> AggregateFunction.MAX
          is AvgExpression -> AggregateFunction.AVG
          is CountExpression -> AggregateFunction.COUNT
          else ->
              throw IllegalStateException(
                  "Cannot serialize aggregate expression to protobuf: ${expr.javaClass.name}")
        }
    return PhysicalAggregateExprNode.newBuilder()
        .setAggrFunction(fn)
        .setInputExpr(toProto(expr.inputExpression()))
        .build()
  }

  /** Convert a schema to protobuf */
  fun toProto(schema: Schema): io.andygrove.kquery.protobuf.Schema {
    return io.andygrove.kquery.protobuf.Schema.newBuilder()
        .addAllColumns(schema.fields.map { toProto(it) })
        .build()
  }

  /** Convert a field to protobuf */
  fun toProto(field: Field): io.andygrove.kquery.protobuf.Field {
    return io.andygrove.kquery.protobuf.Field.newBuilder()
        .setName(field.name)
        .setArrowType(toProtoArrowType(field.dataType))
        .build()
  }

  /** Convert an Arrow type to protobuf enum */
  fun toProtoArrowType(arrowType: ArrowTypeClass): ArrowType {
    return when (arrowType) {
      ArrowTypes.BooleanType -> ArrowType.BOOL
      ArrowTypes.Int8Type -> ArrowType.INT8
      ArrowTypes.Int16Type -> ArrowType.INT16
      ArrowTypes.Int32Type -> ArrowType.INT32
      ArrowTypes.Int64Type -> ArrowType.INT64
      ArrowTypes.UInt8Type -> ArrowType.UINT8
      ArrowTypes.UInt16Type -> ArrowType.UINT16
      ArrowTypes.UInt32Type -> ArrowType.UINT32
      ArrowTypes.UInt64Type -> ArrowType.UINT64
      ArrowTypes.FloatType -> ArrowType.FLOAT
      ArrowTypes.DoubleType -> ArrowType.DOUBLE
      ArrowTypes.StringType -> ArrowType.UTF8
      ArrowTypes.DateDayType -> ArrowType.DATE32
      else ->
          throw IllegalStateException(
              "Cannot serialize Arrow type to protobuf: $arrowType")
    }
  }

  /** Convert a shuffle location to protobuf */
  fun toProto(
      loc: io.andygrove.kquery.physical.ShuffleLocation
  ): ShuffleLocation {
    return ShuffleLocation.newBuilder()
        .setJobUuid(loc.jobUuid)
        .setStageId(loc.stageId)
        .setPartitionId(loc.partitionId)
        .setExecutorId(loc.executorId)
        .setExecutorHost(loc.executorHost)
        .setExecutorPort(loc.executorPort)
        .build()
  }

  /** Convert a task to protobuf */
  fun toProto(task: Task): TaskInfo {
    return TaskInfo.newBuilder()
        .setJobUuid(task.jobUuid)
        .setStageId(task.stageId)
        .setTaskId(task.taskId)
        .setPartitionId(task.partitionId)
        .setPlan(toProto(task.plan))
        .build()
  }
}
