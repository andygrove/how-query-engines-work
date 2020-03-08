package org.ballistacompute.physical

import org.ballistacompute.datasource.DataSource
import org.ballistacompute.datasource.RecordBatch

/**
 * Scan a data source with optional push-down projection.
 */
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {
    override fun execute(): Sequence<RecordBatch> {
        return ds.scan(projection);
    }
}
