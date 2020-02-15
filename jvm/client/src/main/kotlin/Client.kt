package io.andygrove.ballista.client

//import io.andygrove.ballista.

import io.andygrove.ballista.*
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
            is LogicalPlan.Projection -> {
                LogicalPlanNode.newBuilder()
                        .setProjection(Projection.newBuilder()
                            .addAllExpr(plan.expr.map { toProto(it) }).build())
                        .build()
            }
            else -> throw IllegalStateException()
        }
    }

    /** Convert a Kotlin Expr to a protobuf ExprNode */
    fun toProto(expr: Expr): ExprNode {
        return when (expr) {
            is Expr.ColumnIndex -> {
                ExprNode.newBuilder().setColumnIndex(ColumnIndex.newBuilder().setIndex(expr.i).build()).build()
            }
            else -> throw IllegalStateException()
        }
    }

}