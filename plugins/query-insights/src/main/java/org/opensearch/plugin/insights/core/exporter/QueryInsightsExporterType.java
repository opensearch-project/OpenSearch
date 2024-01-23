/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.util.Locale;

/**
 * Types for the Query Insights Exporters
 *
 * @opensearch.internal
 */
public enum QueryInsightsExporterType {
    /** local index exporter */
    LOCAL_INDEX;

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }

    /**
     * Parse QueryInsightsExporterType from String
     * @param type the String representation of the QueryInsightsExporterType
     * @return QueryInsightsExporterType
     */
    public static QueryInsightsExporterType parse(String type) {
        return valueOf(type.toUpperCase(Locale.ROOT));
    }
}
