package org.ballistacompute.logical

import org.ballistacompute.datasource.DataSource
import org.apache.arrow.vector.types.pojo.Schema

/** Represents a scan of a data source */
class Scan(val name: String, val dataSource: DataSource, val projection: List<String>): LogicalPlan {


    override fun schema(): Schema {
        val schema = dataSource.schema()
        if (projection.isEmpty()) {
            return schema
        } else {
            return Schema(projection.map { name -> schema.fields.findLast { it.name == name } })
        }
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        return if (projection.isEmpty()) {
            "Scan: $name; projection=None"
        } else {
            "Scan: $name; projection=$projection"
        }
    }
}