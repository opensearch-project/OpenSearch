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
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;


/**
 * SPI extension point for back-end query engines (DataFusion, Lucene, etc.)
 * @opensearch.internal
 */
public interface AnalyticsBackEndPlugin {
    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /** JNI boundary for executing serialized plans, or null for engines without native execution. */
    EngineBridge<?, ?, ?> bridge(CatalogSnapshot snapshot);

    /** Supported functions as a Calcite operator table, or null if the back-end adds no functions. */
    SqlOperatorTable operatorTable();

    /** Whether this backend supports the SearchExecEngine path via CompositeEngine. */
    default boolean supportsSearchExecEngine() {
        return false;
    }
}
