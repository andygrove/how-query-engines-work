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

package io.andygrove.kquery.flightserver

import io.andygrove.kquery.datatypes.ArrowAllocator
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.execution.ExecutionContext
import io.andygrove.kquery.physical.HashAggregateExec
import io.andygrove.kquery.physical.ShuffleLocation
import io.andygrove.kquery.physical.ShuffleManager
import io.andygrove.kquery.physical.ShuffleReaderExec
import io.andygrove.kquery.physical.ShuffleWriterExec
import io.andygrove.kquery.protobuf.PhysicalPlanDeserializer
import io.andygrove.kquery.protobuf.TaskInfo
import io.andygrove.kquery.protobuf.TaskResult
import java.lang.IllegalStateException
import org.apache.arrow.flight.*
import org.apache.arrow.vector.*

/**
 * Flight producer that handles both query execution and distributed task
 * execution.
 *
 * Actions:
 * - "execute_task": Execute a distributed task and return shuffle locations
 *
 * Streams:
 * - Query execution via ticket containing LogicalPlanNode
 * - Shuffle data via ticket containing ShuffleId
 */
class KQueryFlightProducer(
    private val executorId: String = "executor-0",
    private val executorHost: String = "localhost",
    private val executorPort: Int = 50051,
    private val shuffleDir: String = "/tmp/kquery-shuffle"
) : FlightProducer {

  private val shuffleManager = ShuffleManager(shuffleDir)

  override fun getStream(
      context: FlightProducer.CallContext?,
      ticket: Ticket?,
      listener: FlightProducer.ServerStreamListener?
  ) {

    if (listener == null) {
      throw IllegalArgumentException()
    }

    try {

      val action =
          io.andygrove.kquery.protobuf.Action.parseFrom(
              ticket?.bytes ?: throw IllegalArgumentException())
      val logicalPlan =
          io.andygrove.kquery.protobuf
              .ProtobufDeserializer()
              .fromProto(action.query)
      println(logicalPlan.pretty())

      val schema = logicalPlan.schema()
      println(schema)

      // TODO get from protobuf request
      val settings = mapOf<String, String>()

      val ctx = ExecutionContext(settings)

      val results = ctx.execute(logicalPlan)

      val root =
          VectorSchemaRoot.create(
              schema.toArrow(), ArrowAllocator.rootAllocator)
      listener.start(root, null)

      // val loader = VectorLoader(root)
      var counter = 0
      results.iterator().forEach { batch ->
        root.clear()

        val rowCount = batch.rowCount()
        println("Received batch with $rowCount rows")

        var batchSize = rowCount
        root.fieldVectors.forEach { it.setInitialCapacity(batchSize) }
        root.allocateNew()

        (0 until schema.fields.size).forEach { columnIndex ->
          val sourceVector = batch.fields[columnIndex]
          val v = root.fieldVectors[columnIndex]

          // TODO this is brute force copying that can be optimized if the underlying data is
          // already in Arrow format
          when (v) {
            is TinyIntVector -> {
              (0 until rowCount).forEach { rowIndex ->
                val value = sourceVector.getValue(rowIndex)
                if (value == null) {
                  v.setNull(rowIndex)
                } else {
                  v.set(rowIndex, value as Byte)
                }
              }
            }
            is SmallIntVector -> {
              (0 until rowCount).forEach { rowIndex ->
                val value = sourceVector.getValue(rowIndex)
                if (value == null) {
                  v.setNull(rowIndex)
                } else {
                  v.set(rowIndex, value as Short)
                }
              }
            }
            is IntVector -> {
              (0 until rowCount).forEach { rowIndex ->
                val value = sourceVector.getValue(rowIndex)
                if (value == null) {
                  v.setNull(rowIndex)
                } else {
                  v.set(rowIndex, value as Int)
                }
              }
            }
            is BigIntVector -> {
              (0 until rowCount).forEach { rowIndex ->
                val value = sourceVector.getValue(rowIndex)
                if (value == null) {
                  v.setNull(rowIndex)
                } else {
                  v.set(rowIndex, value as Long)
                }
              }
            }
            is Float4Vector -> {
              (0 until rowCount).forEach { rowIndex ->
                val value = sourceVector.getValue(rowIndex)
                if (value == null) {
                  v.setNull(rowIndex)
                } else {
                  v.set(rowIndex, value as Float)
                }
              }
            }
            is Float8Vector -> {
              (0 until rowCount).forEach { rowIndex ->
                val value = sourceVector.getValue(rowIndex)
                if (value == null) {
                  v.setNull(rowIndex)
                } else {
                  v.set(rowIndex, value as Double)
                }
              }
            }
            is VarCharVector -> {
              (0 until rowCount).forEach { ri ->
                val value = sourceVector.getValue(ri)
                if (value == null) {
                  v.setNull(ri)
                } else {
                  v.set(ri, value as ByteArray)
                }
              }
            }
            else -> throw IllegalStateException(v.javaClass.name)
          }
        }

        root.rowCount = rowCount
        listener.putNext()

        counter++
      }

      root.close()
      listener.completed()
    } catch (ex: Exception) {
      ex.printStackTrace()
      listener.error(ex)
    }
  }

  override fun listFlights(
      context: FlightProducer.CallContext?,
      criteria: Criteria?,
      listener: FlightProducer.StreamListener<FlightInfo>?
  ) {
    TODO(
        "not implemented") // To change body of created functions use File | Settings | File
    // Templates.
  }

  override fun getFlightInfo(
      context: FlightProducer.CallContext?,
      descriptor: FlightDescriptor?
  ): FlightInfo {
    TODO(
        "not implemented") // To change body of created functions use File | Settings | File
    // Templates.
  }

  override fun listActions(
      context: FlightProducer.CallContext?,
      listener: FlightProducer.StreamListener<ActionType>?
  ) {
    TODO(
        "not implemented") // To change body of created functions use File | Settings | File
    // Templates.
  }

  override fun acceptPut(
      context: FlightProducer.CallContext?,
      flightStream: FlightStream?,
      ackStream: FlightProducer.StreamListener<PutResult>?
  ): Runnable {
    TODO(
        "not implemented") // To change body of created functions use File | Settings | File
    // Templates.
  }

  override fun doAction(
      context: FlightProducer.CallContext?,
      action: Action?,
      listener: FlightProducer.StreamListener<Result>?
  ) {
    if (listener == null || action == null) {
      throw IllegalArgumentException("listener and action are required")
    }

    try {
      when (action.type) {
        "execute_task" -> {
          val taskInfo = TaskInfo.parseFrom(action.body)
          val task =
              io.andygrove.kquery.protobuf
                  .PhysicalPlanDeserializer()
                  .fromProto(taskInfo)

          println("Executing task ${task.taskId} for stage ${task.stageId}")

          // Execute the task based on the plan type
          val shuffleLocations = executeTask(task)

          // Build TaskResult protobuf
          val resultBuilder =
              TaskResult.newBuilder()
                  .setJobUuid(task.jobUuid)
                  .setStageId(task.stageId)
                  .setTaskId(task.taskId)
                  .setPartitionId(task.partitionId)

          shuffleLocations.forEach { loc ->
            resultBuilder.addShuffleLocations(
                io.andygrove.kquery.protobuf.ShuffleLocation.newBuilder()
                    .setJobUuid(loc.jobUuid)
                    .setStageId(loc.stageId)
                    .setPartitionId(loc.partitionId)
                    .setExecutorId(loc.executorId)
                    .setExecutorHost(loc.executorHost)
                    .setExecutorPort(loc.executorPort)
                    .build())
          }

          val result = Result(resultBuilder.build().toByteArray())
          listener.onNext(result)
          listener.onCompleted()
        }
        else -> {
          throw IllegalArgumentException("Unknown action type: ${action.type}")
        }
      }
    } catch (ex: Exception) {
      ex.printStackTrace()
      listener.onError(ex)
    }
  }

  /** Execute a distributed task and return shuffle locations. */
  private fun executeTask(
      task: io.andygrove.kquery.physical.Task
  ): List<ShuffleLocation> {
    return when (val plan = task.plan) {
      is ShuffleWriterExec -> {
        plan.executeAndWriteShuffle(
            executorId, executorHost, executorPort, shuffleManager)
      }
      else -> {
        // For non-shuffle plans, just execute and return empty locations
        plan.execute().forEach { _ -> /* consume results */ }
        listOf()
      }
    }
  }

  /** Execute a plan with shuffle reader support. */
  private fun executePlanWithShuffleSupport(
      plan: io.andygrove.kquery.physical.PhysicalPlan
  ): Sequence<RecordBatch> {
    return when (plan) {
      is ShuffleReaderExec -> {
        plan.executeWithContext(executorId, shuffleManager, null)
      }
      is HashAggregateExec -> {
        // Replace the input if it's a shuffle reader
        val updatedInput = executePlanWithShuffleSupport(plan.input)
        // Create a new HashAggregateExec with the executed input
        sequence {
          val inputBatches = updatedInput.toList()
          // ... execution logic would go here
          // For now, just execute normally
          yieldAll(plan.execute())
        }
      }
      else -> plan.execute()
    }
  }
}
