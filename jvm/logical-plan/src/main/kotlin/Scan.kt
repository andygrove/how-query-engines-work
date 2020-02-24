package io.andygrove.kquery.logical

import io.andygrove.kquery.datasource.DataSource
import org.apache.arrow.vector.types.pojo.Schema

/** Represents a scan of a data source */
class Scan(val name: String, val dataSource: DataSource, val projection: List<Int>): LogicalPlan {

    override fun schema(): Schema {
        return dataSource.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        return if (projection.isEmpty()) {
            "Scan: $name; projection=None"
        } else {
            "Scan: $name; projection=${ projection.map { "#$it" }.joinToString { "," } }"
        }
    }
}