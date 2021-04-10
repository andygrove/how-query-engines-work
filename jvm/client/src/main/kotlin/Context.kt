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

package io.andygrove.kquery.client

class Context(endpoint: Endpoint) {
  val client = Client(endpoint.host, endpoint.port)
}

class ContextBuilder {

  var master: String? = null

  fun master(master: String): ContextBuilder {
    this.master = master
    return this
  }

  fun build(): Context {
    return Context(parseMaster(this.master))
  }

  private fun parseMaster(master: String?): Endpoint {
    val parts = this.master?.split(':')
    if (parts?.size == 2) {
      return Endpoint(parts[0], parts[1].toInt())
    } else {
      throw IllegalStateException()
    }
  }
}

data class Endpoint(val host: String, val port: Int)
