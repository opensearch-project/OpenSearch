/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

import java.util.List;

/**
 * Debug exporter for development purpose
 */
public final class DebugExporter implements AbstractExporter {
    /**
     * Logger of the debug exporter
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor of DebugExporter
     */
    public DebugExporter() {}

    /**
     * Write the list of SearchQueryRecord to debug log
     *
     * @param records list of {@link SearchQueryRecord}
     * @return true
     */
    @Override
    public boolean export(final List<SearchQueryRecord> records) {
        logger.debug("QUERY_INSIGHTS_RECORDS: " + records.toString());
        return true;
    }

    /**
     * Close the debugger exporter sink
     */
    @Override
    public void close() {}
}
