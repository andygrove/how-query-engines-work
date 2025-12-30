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
import org.apache.arrow.flight.FlightServer
import org.apache.arrow.flight.Location

class KQueryFlightServer {
  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      val name = KQueryFlightServer::class.java.`package`.implementationTitle
      val version =
          KQueryFlightServer::class.java.`package`.implementationVersion
      println("Starting $name $version")

      // https://issues.apache.org/jira/browse/ARROW-5412
      System.setProperty("io.netty.tryReflectionSetAccessible", "true")

      val bindHost = "0.0.0.0"
      val port = 50051

      val server =
          FlightServer.builder(
                  ArrowAllocator.rootAllocator,
                  Location.forGrpcInsecure(bindHost, port),
                  KQueryFlightProducer())
              .build()
      server.start()

      println("Listening on $bindHost:$port")

      while (true) {
        Thread.sleep(1000)
      }
    }
  }
}
