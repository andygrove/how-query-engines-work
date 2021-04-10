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

package io.andygrove.kquery.logical

import io.andygrove.kquery.datatypes.Schema

/** Logical plan representing a limit */
class Limit(val input: LogicalPlan, val limit: Int) : LogicalPlan {
  override fun schema(): Schema {
    return input.schema()
  }

  override fun children(): List<LogicalPlan> {
    return listOf(input)
  }

  override fun toString(): String {
    return "Limit: $limit"
  }
}
