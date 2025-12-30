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

import io.andygrove.kquery.datatypes.ArrowAllocator
import io.andygrove.kquery.datatypes.ArrowFieldVector
import io.andygrove.kquery.datatypes.RecordBatch
import io.andygrove.kquery.datatypes.Schema
import java.math.BigDecimal
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.DecimalVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.PrimitiveType

class ParquetDataSource(private val filename: String) : DataSource {

  override fun schema(): Schema {
    return ParquetScan(filename, listOf()).use {
      val arrowSchema = SchemaConverter().fromParquet(it.schema).arrowSchema
      io.andygrove.kquery.datatypes.SchemaConverter.fromArrow(arrowSchema)
    }
  }

  override fun scan(projection: List<String>): Sequence<RecordBatch> {
    return ParquetScan(filename, projection)
  }
}
/**
 * Based on blog post at https://www.arm64.ca/post/reading-parquet-files-java/
 */
class ParquetScan(filename: String, private val columns: List<String>) :
    AutoCloseable, Sequence<RecordBatch> {

  private val reader =
      ParquetFileReader.open(
          HadoopInputFile.fromPath(Path(filename), Configuration()))
  val schema = reader.footer.fileMetaData.schema

  override fun iterator(): Iterator<RecordBatch> {
    return ParquetIterator(reader, columns)
  }

  override fun close() {
    reader.close()
  }
}

class ParquetIterator(
    private val reader: ParquetFileReader,
    private val projectedColumns: List<String>
) : Iterator<RecordBatch> {

  val parquetSchema = reader.footer.fileMetaData.schema

  val arrowSchema = SchemaConverter().fromParquet(parquetSchema).arrowSchema

  val projectedArrowSchema =
      org.apache.arrow.vector.types.pojo.Schema(
          projectedColumns.map { name ->
            arrowSchema.fields.find { it.name == name }
          })

  var batch: RecordBatch? = null

  override fun hasNext(): Boolean {
    batch = nextBatch()
    return batch != null
  }

  override fun next(): RecordBatch {
    val next = batch
    batch = null
    return next!!
  }

  private fun nextBatch(): RecordBatch? {
    val pages: PageReadStore? = reader.readNextRowGroup()
    if (pages == null) {
      return null
    }

    if (pages.rowCount > Integer.MAX_VALUE) {
      throw IllegalStateException()
    }

    val rows = pages.rowCount.toInt()
    println("Reading $rows rows")

    val root =
        VectorSchemaRoot.create(
            projectedArrowSchema, ArrowAllocator.rootAllocator)
    root.allocateNew()
    root.rowCount = rows

    // Read data using GroupRecordConverter
    val columnIO = ColumnIOFactory().getColumnIO(parquetSchema)
    val recordReader =
        columnIO.getRecordReader(pages, GroupRecordConverter(parquetSchema))

    for (rowIndex in 0 until rows) {
      val group: Group = recordReader.read()
      for (projectionIndex in 0 until projectedColumns.size) {
        val fieldName = projectedColumns[projectionIndex]
        val fieldType = parquetSchema.getType(fieldName)
        val vector = root.fieldVectors[projectionIndex]

        if (group.getFieldRepetitionCount(fieldName) == 1) {
          when (fieldType.asPrimitiveType().primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.BOOLEAN -> {
              (vector as BitVector).set(
                  rowIndex, if (group.getBoolean(fieldName, 0)) 1 else 0)
            }
            PrimitiveType.PrimitiveTypeName.INT32 -> {
              (vector as IntVector).set(
                  rowIndex, group.getInteger(fieldName, 0))
            }
            PrimitiveType.PrimitiveTypeName.INT64 -> {
              val longValue = group.getLong(fieldName, 0)
              when (vector) {
                is BigIntVector -> vector.set(rowIndex, longValue)
                is DecimalVector -> {
                  // Parquet stores decimals as unscaled integers, so we need to
                  // apply the scale from the Arrow vector
                  val unscaledValue =
                      BigDecimal.valueOf(longValue, vector.scale)
                  vector.set(rowIndex, unscaledValue)
                }
                else ->
                    throw IllegalStateException(
                        "Unsupported vector type for INT64: ${vector.javaClass}")
              }
            }
            PrimitiveType.PrimitiveTypeName.FLOAT -> {
              (vector as Float4Vector).set(
                  rowIndex, group.getFloat(fieldName, 0))
            }
            PrimitiveType.PrimitiveTypeName.DOUBLE -> {
              (vector as Float8Vector).set(
                  rowIndex, group.getDouble(fieldName, 0))
            }
            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> {
              val bytes = group.getBinary(fieldName, 0).bytes
              when (vector) {
                is VarCharVector -> vector.setSafe(rowIndex, bytes)
                is VarBinaryVector -> vector.setSafe(rowIndex, bytes)
                else ->
                    throw IllegalStateException(
                        "Unsupported vector type for binary: ${vector.javaClass}")
              }
            }
            PrimitiveType.PrimitiveTypeName.INT96 -> {
              // INT96 is used for timestamps in legacy Parquet files
              val bytes = group.getInt96(fieldName, 0).bytes
              (vector as VarBinaryVector).setSafe(rowIndex, bytes)
            }
          }
        }
      }
    }

    val schema =
        io.andygrove.kquery.datatypes.SchemaConverter.fromArrow(
            projectedArrowSchema)
    return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
  }
}
