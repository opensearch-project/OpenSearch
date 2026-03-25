/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.util.List;

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
    SearchExecEngine<ExecutionContext, EngineResultStream> searcher(ExecutionContext ctx);

    /** Returns the data formats supported by this backend. */
    List<DataFormat> getSupportedFormats();
}
