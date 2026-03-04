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
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;

import java.io.IOException;
import java.util.List;

/**
 * SPI extension point for back-end query engines (DataFusion, Lucene, etc.).
 * @opensearch.internal
 */
public interface AnalyticsBackEndPlugin extends SearchAnalyticsBackEndPlugin {
    /** Unique engine name (e.g., "lucene", "datafusion"). */
    String name();

    /** JNI boundary for executing serialized plans, or null for engines without native execution. */
    EngineBridge<?, ?, ?> bridge(); // TODO this doesn't have context / index shard init

    /** Supported functions as a Calcite operator table, or null if the back-end adds no functions. */
    SqlOperatorTable operatorTable();

}
