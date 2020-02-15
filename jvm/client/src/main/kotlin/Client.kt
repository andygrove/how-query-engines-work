package io.andygrove.ballista.client

//import io.andygrove.ballista.

import io.andygrove.ballista.ColumnIndex
import io.andygrove.ballista.ExprNode
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


//        var ticket = Ticket(query.toByteArray())
//
//        var stream = client.getStream(ticket, callOptions)

    }

    /** Convert a Kotlin LogicalPlan to a protobuf LogicalPlan */
    fun toProto(plan: LogicalPlan) {
        when (plan) {
            is LogicalPlan.Projection -> plan.expr

        }
    }

    /** Convert a Kotlin Expr to a protobuf ExprNode */
    fun toProto(expr: Expr): ExprNode {
        return when (expr) {
            is Expr.ColumnIndex -> {
                ExprNode.newBuilder().setColumnIndex(ColumnIndex.newBuilder().setIndex(expr.i).build()).build()
            }
            else -> {
                throw IllegalStateException()
            }
        }
    }

}