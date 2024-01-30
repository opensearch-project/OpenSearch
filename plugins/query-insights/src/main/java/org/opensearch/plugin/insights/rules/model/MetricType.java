/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Valid metric types for a search query record
 */
public enum MetricType {
    /**
     * Latency metric type
     */
    LATENCY,
    /**
     * CPU usage metric type
     */
    CPU,
    /**
     * JVM heap usage metric type
     */
    JVM;

    /**
     * Read a MetricType from a StreamInput
     * @param in the StreamInput to read from
     * @return MetricType
     * @throws IOException IOException
     */
    public static MetricType readFromStream(StreamInput in) throws IOException {
        return fromString(in.readString());
    }

    /**
     * Create MetricType from String
     * @param metricType the String representation of MetricType
     * @return MetricType
     */
    public static MetricType fromString(String metricType) {
        return MetricType.valueOf(metricType.toUpperCase(Locale.ROOT));
    }

    /**
     * Write MetricType to a StreamOutput
     * @param out the StreamOutput to write
     * @param metricType the MetricType to write
     * @throws IOException IOException
     */
    public static void writeTo(StreamOutput out, MetricType metricType) throws IOException {
        out.writeString(metricType.toString());
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }

    /**
     * Get all valid metrics
     * @return A set of String that contains all valid metrics
     */
    public static Set<MetricType> allMetricTypes() {
        return Arrays.stream(values()).collect(Collectors.toSet());
    }
}
