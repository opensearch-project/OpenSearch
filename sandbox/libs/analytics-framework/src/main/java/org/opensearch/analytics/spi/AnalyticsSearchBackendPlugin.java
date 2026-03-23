/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.SearchExecEngine;

import java.io.IOException;
import java.util.List;

/**
 * SPI extension point for back-end query engines (DataFusion, Lucene, etc.).
 * @opensearch.internal
 */
public interface AnalyticsSearchBackendPlugin {

    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /**
     * Creates a per-query bridge bound to the given reader.
     *
     * @param reader the format-specific reader from the composite reader
     * @return a bridge for this query, caller must close when done
     */
    EngineBridge<?, ?, ?> bridge(DataFormat format, Object reader, SearchExecEngine<?, ?, ?> engine) throws IOException;

    /** Supported functions as a Calcite operator table, or null if the back-end adds no functions. */
    SqlOperatorTable operatorTable();

    // TODO : remove this ?
    List<DataFormat> getSupportedFormats();
}
