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

package io.andygrove.kquery.physical

/**
 * Represents the location of shuffle data that can be fetched from an executor.
 *
 * In distributed query execution, intermediate results are shuffled between
 * stages. This class describes where shuffle data for a specific partition can
 * be found.
 */
data class ShuffleLocation(
    /** Unique identifier for the job. */
    val jobUuid: String,

    /** Stage that produced this shuffle data. */
    val stageId: Int,

    /** Partition number within the stage. */
    val partitionId: Int,

    /** Identifier of the executor holding this data. */
    val executorId: String,

    /** Host address of the executor. */
    val executorHost: String,

    /** Port number of the executor's Arrow Flight service. */
    val executorPort: Int
)
