/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Type of supported sinks
 */
public enum SinkType {
    /** debug exporter */
    DEBUG("debug"),
    /** local index exporter */
    LOCAL_INDEX("local_index");

    private final String type;

    SinkType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }

    /**
     * Parse SinkType from String
     * @param type the String representation of the SinkType
     * @return SinkType
     */
    public static SinkType parse(final String type) {
        return valueOf(type.toUpperCase(Locale.ROOT));
    }

    /**
     * Get all valid SinkTypes
     *
     * @return A set contains all valid SinkTypes
     */
    public static Set<SinkType> allSinkTypes() {
        return Arrays.stream(values()).collect(Collectors.toSet());
    }

    /**
     * Get Sink type from exporter
     *
     * @param exporter the {@link QueryInsightsExporter}
     * @return SinkType associated with this exporter
     */
    public static SinkType getSinkTypeFromExporter(QueryInsightsExporter exporter) {
        if (exporter.getClass().equals(LocalIndexExporter.class)) {
            return SinkType.LOCAL_INDEX;
        }
        return SinkType.DEBUG;
    }
}
