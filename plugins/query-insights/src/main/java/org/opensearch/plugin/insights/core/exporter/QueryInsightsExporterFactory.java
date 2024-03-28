/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.joda.time.format.DateTimeFormat;

import java.util.Locale;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_N_LATENCY_QUERIES_INDEX_PATTERN;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.DEFAULT_TOP_QUERIES_EXPORTER_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.EXPORTER_TYPE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.EXPORT_INDEX;

/**
 * Factory class for validating and creating exporters based on provided settings
 */
public class QueryInsightsExporterFactory {
    final private Client client;

    /**
     * Constructor of QueryInsightsExporterFactory
     *
     * @param client OS client
     */
    public QueryInsightsExporterFactory(final Client client) {
        this.client = client;
    }

    /**
     * Validate exporter sink config
     *
     * @param settings exporter sink config {@link Settings}
     * @throws IllegalArgumentException if provided exporter sink config settings are invalid
     */
    public void validateExporterConfig(final Settings settings) throws IllegalArgumentException {
        // Disable exporter if the EXPORTER_TYPE setting is null
        if (settings.get(EXPORTER_TYPE) == null) {
            return;
        }
        SinkType type;
        try {
            type = SinkType.parse(settings.get(EXPORTER_TYPE, DEFAULT_TOP_QUERIES_EXPORTER_TYPE));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid exporter type [%s]", settings.get(EXPORTER_TYPE)));
        }
        switch (type) {
            case LOCAL_INDEX:
                final String indexPattern = settings.get(EXPORT_INDEX, DEFAULT_TOP_N_LATENCY_QUERIES_INDEX_PATTERN);
                if (indexPattern.length() == 0) {
                    throw new IllegalArgumentException("Empty index pattern configured for the exporter");
                }
                try {
                    DateTimeFormat.forPattern(indexPattern);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Invalid index pattern [%s] configured for the exporter", indexPattern)
                    );
                }
        }
    }

    /**
     * Create an exporter based on provided parameters
     *
     * @param type The type of exporter to create
     * @param indexPattern the index pattern if creating a index exporter
     * @return AbstractExporter the created exporter sink
     */
    public AbstractExporter createExporter(SinkType type, String indexPattern) {
        switch (type) {
            case LOCAL_INDEX:
                return new LocalIndexExporter(client, DateTimeFormat.forPattern(indexPattern));
            default:
                return new DebugExporter();
        }
    }

}
