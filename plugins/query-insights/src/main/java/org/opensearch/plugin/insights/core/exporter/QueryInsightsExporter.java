/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

import java.util.List;

/**
 * Simple abstract class to export data collected by search query analyzers
 * <p>
 * Mainly for use within the Query Insight framework
 *
 * @opensearch.internal
 */
public abstract class QueryInsightsExporter<T extends SearchQueryRecord<?>> {
    private QueryInsightsExporterType type;
    private String identifier;

    QueryInsightsExporter(QueryInsightsExporterType type, String identifier) {
        this.type = type;
        this.identifier = identifier;
    }

    /**
     * Export the data with the exporter.
     *
     * @param records the data to export
     */
    public abstract void export(List<T> records) throws Exception;

    public void setType(QueryInsightsExporterType type) {
        this.type = type;
    }

    public QueryInsightsExporterType getType() {
        return type;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }
}
