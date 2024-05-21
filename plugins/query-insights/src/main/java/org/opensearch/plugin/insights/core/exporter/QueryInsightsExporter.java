/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Base interface for Query Insights exporters
 */
public interface QueryInsightsExporter extends Closeable {
    /**
     * Export a list of SearchQueryRecord to the exporter sink
     *
     * @param records list of {@link SearchQueryRecord}
     * @return True if export succeed, false otherwise
     */
    boolean export(final List<SearchQueryRecord> records);

    /**
     * Close the exporter sink
     *
     * @throws IOException IOException
     */
    void close() throws IOException;
}
