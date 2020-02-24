package io.andygrove.kquery.datasource

import org.apache.arrow.vector.types.pojo.Schema


interface DataSource {

    /** Return the schema for the underlying data source */
    fun schema(): Schema

    /** Scan the data source, selecting the specified columns */
    fun scan(columns: List<Int>): Iterable<RecordBatch>
}

