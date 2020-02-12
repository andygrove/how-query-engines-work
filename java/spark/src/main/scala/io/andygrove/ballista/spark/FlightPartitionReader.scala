package io.andygrove.ballista.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

import io.andygrove.ballista.jdbc.FlightConnection

class FlightPartitionReader extends InputPartitionReader[InternalRow] {

  val connection = new FlightConnection("localhost", 50051)

  override def next(): Boolean = true

  override def get(): InternalRow = ???

  override def close(): Unit = {

  }
}
