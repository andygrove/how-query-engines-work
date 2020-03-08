package org.ballistacompute.client

import io.andygrove.ballista.*
import org.ballistacompute.logical.Column as KQColumn
import org.ballistacompute.logical.LogicalExpr
import org.ballistacompute.logical.LogicalPlan
import org.ballistacompute.logical.Projection as KQProjection
import org.apache.arrow.flight.CallOptions
import org.apache.arrow.flight.FlightClient
import org.apache.arrow.flight.Location
import org.apache.arrow.flight.Ticket
import org.apache.arrow.memory.RootAllocator

import java.util.concurrent.TimeUnit

/**
 * Connection to a Ballista executor.
 */
class Client(val host: String, val port: Int) {

    var client = FlightClient.builder()
            .allocator(RootAllocator(Long.MAX_VALUE))
            .location(Location.forGrpcInsecure(host, port))
            .build()

    var callOptions = CallOptions.timeout(600, TimeUnit.SECONDS)

    fun execute(plan: LogicalPlan) {

        val protoBuf = toProto(plan)

        var ticket = Ticket(protoBuf.toByteArray())

        var stream = client.getStream(ticket, callOptions)

    }

    /** Convert a Kotlin LogicalPlan to a protobuf LogicalPlan */
    fun toProto(plan: LogicalPlan): LogicalPlanNode {
        return when (plan) {
            is KQProjection -> {
                LogicalPlanNode.newBuilder()
                        .setProjection(Projection.newBuilder()
                            .addAllExpr(plan.expr.map { toProto(it) }).build())
                        .build()
            }
            else -> throw IllegalStateException()
        }
    }

    /** Convert a Kotlin Expr to a protobuf ExprNode */
    fun toProto(expr: LogicalExpr): ExprNode {
        return when (expr) {
            is KQColumn -> {
                ExprNode.newBuilder().setColumn(Column.newBuilder().setName(expr.name).build()).build()
            }
            else -> throw IllegalStateException()
        }
    }

}