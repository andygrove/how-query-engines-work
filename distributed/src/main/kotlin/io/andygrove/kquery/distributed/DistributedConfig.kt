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

package io.andygrove.kquery.distributed

/** Configuration for a single executor in the distributed cluster. */
data class ExecutorConfig(
    /** Unique identifier for this executor. */
    val id: String,

    /** Hostname or IP address. */
    val host: String,

    /** Port for Arrow Flight service. */
    val port: Int
)

/** Configuration for the distributed query execution environment. */
data class DistributedConfig(
    /** List of executors in the cluster. */
    val executors: List<ExecutorConfig>,

    /** Base directory for shuffle data on each executor. */
    val shuffleDir: String = "/tmp/kquery-shuffle",

    /** Default number of partitions for shuffle operations. */
    val defaultPartitions: Int = 0 // 0 means use number of executors
) {
  /** Get the effective number of partitions for shuffle operations. */
  fun getPartitionCount(): Int {
    return if (defaultPartitions > 0) defaultPartitions else executors.size
  }
}
