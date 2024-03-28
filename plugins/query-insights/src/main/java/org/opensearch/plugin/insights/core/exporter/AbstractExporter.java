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

import java.io.IOException;
import java.util.List;

/**
 * Base Abstract class for Query Insights exporters
 */
public abstract class AbstractExporter {
    /**
     * Logger of exporter
     */
    protected final Logger logger = LogManager.getLogger(this.getClass());

    /**
     * Constructor of AbstractExporter
     */
    protected AbstractExporter() {}

    /**
     * Export a list of SearchQueryRecord to the exporter sink
     *
     * @param records list of {@link SearchQueryRecord}
     * @return True if export succeed, false otherwise
     */
    public abstract boolean export(final List<SearchQueryRecord> records);

    /**
     * Close the exporter sink
     *
     * @throws IOException IOException
     */
    public void close() throws IOException {}
}
