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

package io.andygrove.kquery.datasource

import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema

interface DataSource {

  /** Return the schema for the underlying data source */
  fun schema(): Schema

  /** Scan the data source, selecting the specified columns */
  fun scan(projection: List<String>): Sequence<RecordBatch>
}
