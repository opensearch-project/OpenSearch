/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.util.List;
import java.util.Set;

/**
 * SPI extension point for back-end query engines (DataFusion, Lucene, etc.).
 * @opensearch.internal
 */
public interface AnalyticsSearchBackendPlugin {

    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /**
     * Creates a searcher bound to the given reader snapshot.
     * @param ctx the execution context
     */
    SearchExecEngine searcher(ExecutionContext ctx);

    /** Returns the set of RelNode operator classes this backend supports. */
    default Set<Class<? extends RelNode>> supportedOperators() {
        return Set.of(LogicalTableScan.class, LogicalFilter.class, LogicalAggregate.class, LogicalProject.class);
    }

    /** Returns the data formats supported by this backend. */
    List<DataFormat> getSupportedFormats();
}
