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
 * Represents the mode of aggregation for distributed execution.
 *
 * In distributed query execution, aggregation is typically performed in two
 * stages:
 * 1. PARTIAL: Each executor computes partial aggregates on its local data
 * 2. FINAL: A coordinator merges the partial aggregates into final results
 *
 * COMPLETE mode is used for single-node execution where all data is aggregated
 * at once.
 */
enum class AggregateMode {
  /** Single-node aggregation - all data is aggregated in one step. */
  COMPLETE,

  /** First stage of distributed aggregation - outputs intermediate state. */
  PARTIAL,

  /** Second stage of distributed aggregation - merges intermediate states. */
  FINAL
}
