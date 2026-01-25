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

import io.andygrove.kquery.physical.PhysicalPlan
import io.andygrove.kquery.physical.ShuffleLocation

/**
 * Represents a stage in a distributed query execution plan.
 *
 * A query is divided into stages at shuffle boundaries. Each stage can be
 * executed independently on different executors, with data shuffled between
 * stages.
 */
data class QueryStage(
    /** Unique identifier for this stage within the job. */
    val stageId: Int,

    /** The physical plan to execute for this stage. */
    val plan: PhysicalPlan,

    /** IDs of stages that must complete before this stage can start. */
    val dependencies: List<Int> = listOf(),

    /** Number of partitions to create if this stage produces shuffle output. */
    val partitionCount: Int = 1,

    /** Whether this is a final stage that produces the query result. */
    val isFinalStage: Boolean = false
)

/** Represents the result of executing a stage. */
data class StageResult(
    /** The stage that was executed. */
    val stageId: Int,

    /**
     * Shuffle locations produced by this stage. Empty for final stages that
     * produce direct results.
     */
    val shuffleLocations: List<ShuffleLocation> = listOf(),

    /** Whether the stage completed successfully. */
    val success: Boolean = true,

    /** Error message if the stage failed. */
    val error: String? = null
)
