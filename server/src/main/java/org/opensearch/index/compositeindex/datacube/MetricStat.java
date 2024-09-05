/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Arrays;
import java.util.List;

/**
 * Supported metric types for composite index
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum MetricStat {
    VALUE_COUNT("value_count", 0),
    SUM("sum", 1),
    MIN("min", 2),
    MAX("max", 3),
    AVG("avg", 4, VALUE_COUNT, SUM),
    DOC_COUNT("doc_count", true, 5);

    private final String typeName;
    private final MetricStat[] baseMetrics;
    private final int metricOrdinal;

    // System field stats cannot be used as input for user metric types
    private final boolean isSystemFieldStat;

    MetricStat(String typeName, int metricOrdinal) {
        this(typeName, false, metricOrdinal);
    }

    MetricStat(String typeName, int metricOrdinal, MetricStat... baseMetrics) {
        this(typeName, false, metricOrdinal, baseMetrics);
    }

    MetricStat(String typeName, boolean isSystemFieldStat, int metricOrdinal, MetricStat... baseMetrics) {
        this.typeName = typeName;
        this.isSystemFieldStat = isSystemFieldStat;
        this.baseMetrics = baseMetrics;
        this.metricOrdinal = metricOrdinal;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getMetricOrdinal() {
        return metricOrdinal;
    }

    /**
     * Return the list of metrics that this metric is derived from
     * For example, AVG is derived from COUNT and SUM
     */
    public List<MetricStat> getBaseMetrics() {
        return Arrays.asList(baseMetrics);
    }

    /**
     * Return true if this metric is derived from other metrics
     * For example, AVG is derived from COUNT and SUM
     */
    public boolean isDerivedMetric() {
        return baseMetrics != null && baseMetrics.length > 0;
    }

    public static MetricStat fromTypeName(String typeName) {
        for (MetricStat metric : MetricStat.values()) {
            // prevent system fields to be entered as user input
            if (metric.getTypeName().equalsIgnoreCase(typeName) && metric.isSystemFieldStat == false) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Invalid metric stat: " + typeName);
    }

    public static MetricStat fromMetricOrdinal(int metricOrdinal) {
        for (MetricStat metric : MetricStat.values()) {
            if (metric.getMetricOrdinal() == metricOrdinal) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Invalid metric stat: " + metricOrdinal);
    }
}
