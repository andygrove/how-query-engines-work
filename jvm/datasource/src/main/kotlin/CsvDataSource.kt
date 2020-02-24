package io.andygrove.kquery.datasource

import com.github.doyaaaaaken.kotlincsv.client.CsvReader
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import java.io.File
import java.util.logging.Logger

/**
 * Simple CSV data source that assumes that the first line contains field names and that all values are strings.
 *
 * Note that this implementation loads the entire CSV file into memory so is not scalable. I plan on implementing
 * a streaming version later on.
 */
class CsvDataSource(filename: String, private val batchSize: Int) : DataSource {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    private val rows: List<List<String>> = CsvReader().readAll(File(filename))

    private val schema = Schema(rows[0].map { Field.nullable(it, ArrowType.Utf8()) })

    override fun schema(): Schema {
        return schema
    }

    override fun scan(columns: List<Int>): Iterable<RecordBatch> {
        logger.info("scan()")

        //TODO don't ignore projection

        val withoutHeader = rows.asSequence()
                .drop(1)

        return withoutHeader
                .chunked(batchSize)
                .map { createBatch(it) }
                .asIterable()
    }

    private fun createBatch(rows: List<List<String>>) : RecordBatch {
        logger.info("createBatch() rows=$rows")

        val root = VectorSchemaRoot.create(schema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = rows.size

        root.fieldVectors.withIndex().forEach { field ->
            when (field.value) {
                is VarCharVector -> rows.withIndex().forEach { row ->
                    val value = row.value[field.index]
                    (field.value as VarCharVector).set(row.index, value.toByteArray())
                }
                else -> TODO()
            }
            field.value.valueCount = rows.size
        }

        val batch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })

        logger.info("Created batch:\n${batch.toCSV()}")

        return batch
    }
}

