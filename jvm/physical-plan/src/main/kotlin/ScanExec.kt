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

package io.andygrove.kquery.physical

import io.andygrove.kquery.datasource.DataSource
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema

/** Scan a data source with optional push-down projection. */
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {

  override fun schema(): Schema {
    return ds.schema().select(projection)
  }

  override fun children(): List<PhysicalPlan> {
    return listOf()
  }

  override fun execute(): Sequence<RecordBatch> {
    return ds.scan(projection)
  }

  override fun toString(): String {
    return "ScanExec: schema=${schema()}, projection=$projection"
  }
}
