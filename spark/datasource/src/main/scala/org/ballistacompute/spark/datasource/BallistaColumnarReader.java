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

package io.andygrove.spark.datasource;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class BallistaColumnarReader implements PartitionReader<ColumnarBatch> {

  private final TableMeta tableMeta;

  private final FlightStream stream;

  private VectorSchemaRoot root;

  private FlightClient client;

  public BallistaColumnarReader(TableMeta tableMeta) {
    this.tableMeta = tableMeta;

    client = FlightClient.builder()
        .allocator(new RootAllocator(Long.MAX_VALUE))
        .location(Location.forGrpcInsecure(tableMeta.host, tableMeta.port))
        .build();

    Ticket query = new Ticket("SELECT id FROM alltypes_plain".getBytes());
    stream = client.getStream(query);
  }

  @Override
  public boolean next() throws IOException {
    if (stream.next()) {
      System.out.println("next()");
//      Schema schema = stream.getSchema();
//      System.out.println(schema);

      root = stream.getRoot();
      System.out.println("Received " + root.getRowCount() + " rows");

      return true;
    } else {
      root = null;
      return false;
    }
  }

  @Override
  public ColumnarBatch get() {
    ArrowColumnVector columns[] = new ArrowColumnVector[root.getFieldVectors().size()];
    for (int i=0; i<columns.length; i++) {
      columns[i] = new ArrowColumnVector(root.getFieldVectors().get(i));
    }
    return new ColumnarBatch(columns, root.getRowCount());
  }

  @Override
  public void close() throws IOException {
  }
}
