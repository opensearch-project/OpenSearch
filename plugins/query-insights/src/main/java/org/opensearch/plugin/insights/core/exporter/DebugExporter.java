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
public final class DebugExporter implements QueryInsightsExporter {
    /**
     * Logger of the debug exporter
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Constructor of DebugExporter
     */
    private DebugExporter() {}

    private static class InstanceHolder {
        private static final DebugExporter INSTANCE = new DebugExporter();
    }

    /**
     Get the singleton instance of DebugExporter
     *
     @return DebugExporter instance
     */
    public static DebugExporter getInstance() {
        return InstanceHolder.INSTANCE;
    }

    /**
     * Write the list of SearchQueryRecord to debug log
     *
     * @param records list of {@link SearchQueryRecord}
     */
    @Override
    public void export(final List<SearchQueryRecord> records) {
        logger.debug("QUERY_INSIGHTS_RECORDS: " + records.toString());
    }

    /**
     * Close the debugger exporter sink
     */
    @Override
    public void close() {
        logger.debug("Closing the DebugExporter..");
    }
}
