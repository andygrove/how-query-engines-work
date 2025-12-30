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

package io.andygrove.kquery.protobuf.test

import io.andygrove.kquery.datasource.CsvDataSource
import io.andygrove.kquery.logical.*
import io.andygrove.kquery.protobuf.ProtobufDeserializer
import io.andygrove.kquery.protobuf.ProtobufSerializer
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SerdeTest {

  @Test
  fun `Convert plan to protobuf`() {

    val df =
        csv()
            .filter(col("state") eq lit("CO"))
            .project(listOf(col("id"), col("first_name"), col("last_name")))

    val logicalPlan = roundtrip(df)

    val expected =
        "Projection: #id, #first_name, #last_name\n" +
            "\tSelection: #state = 'CO'\n" +
            "\t\tScan: src/test/resources/employee.csv; projection=None\n"

    assertEquals(expected, format(logicalPlan))
  }

  private fun roundtrip(df: DataFrame): LogicalPlan {
    val protobuf = ProtobufSerializer().toProto(df.logicalPlan())
    return ProtobufDeserializer().fromProto(protobuf)
  }

  private fun csv(): DataFrame {
    val employeeCsv = "src/test/resources/employee.csv"
    val csv = CsvDataSource(employeeCsv, null, true, 1024)
    println(csv.schema())
    return DataFrameImpl(Scan(employeeCsv, csv, listOf()))
  }
}
