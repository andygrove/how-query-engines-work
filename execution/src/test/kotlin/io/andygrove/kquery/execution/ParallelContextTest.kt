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

package io.andygrove.kquery.execution

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class ParallelContextTest {

  private val testDataDir = "../testdata"

  @Test
  fun `parallel aggregate produces same results as sequential`() {
    val employeeCsv = File(testDataDir, "employee.csv").absolutePath
    val sql =
        "SELECT state, SUM(CAST(salary AS double)) FROM employee GROUP BY state"

    // Execute with sequential context
    val seqCtx = ExecutionContext(mapOf())
    seqCtx.registerCsv("employee", employeeCsv)
    val seqResults = seqCtx.execute(seqCtx.sql(sql)).toList()

    // Execute with parallel context (4 workers)
    val parCtx = ParallelContext(parallelism = 4)
    parCtx.registerCsv("employee", employeeCsv)
    val parResults = parCtx.execute(parCtx.sql(sql)).toList()

    // Both should produce the same number of result batches
    assertEquals(seqResults.size, parResults.size)

    // Extract results as maps for easier comparison
    val seqMap = extractResults(seqResults)
    val parMap = extractResults(parResults)

    // Same keys
    assertEquals(seqMap.keys, parMap.keys)

    // Same values for each key
    seqMap.keys.forEach { key -> assertEquals(seqMap[key], parMap[key]) }
  }

  @Test
  fun `parallel context with parallelism 1 behaves like sequential`() {
    val employeeCsv = File(testDataDir, "employee.csv").absolutePath
    val sql =
        "SELECT state, SUM(CAST(salary AS double)) FROM employee GROUP BY state"

    val ctx = ParallelContext(parallelism = 1)
    ctx.registerCsv("employee", employeeCsv)
    val results = ctx.execute(ctx.sql(sql)).toList()

    // Should produce results
    assertEquals(1, results.size)
    assert(results[0].rowCount() > 0)
  }

  private fun extractResults(
      batches: List<io.andygrove.kquery.datatypes.RecordBatch>
  ): Map<String, Any?> {
    val map = mutableMapOf<String, Any?>()
    batches.forEach { batch ->
      (0 until batch.rowCount()).forEach { row ->
        val key = batch.field(0).getValue(row)
        val value = batch.field(1).getValue(row)
        val keyStr =
            when (key) {
              is ByteArray -> String(key)
              else -> key.toString()
            }
        map[keyStr] = value
      }
    }
    return map
  }
}
