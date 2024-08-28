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
    VALUE_COUNT("value_count"),
    SUM("sum"),
    MIN("min"),
    MAX("max"),
    AVG("avg", VALUE_COUNT, SUM),
    DOC_COUNT("doc_count", true);

    private final String typeName;
    private final MetricStat[] baseMetrics;

    // System field stats cannot be used as input for user metric types
    private final boolean isSystemFieldStat;

    MetricStat(String typeName) {
        this(typeName, false);
    }

    MetricStat(String typeName, MetricStat... baseMetrics) {
        this(typeName, false, baseMetrics);
    }

    MetricStat(String typeName, boolean isSystemFieldStat, MetricStat... baseMetrics) {
        this.typeName = typeName;
        this.isSystemFieldStat = isSystemFieldStat;
        this.baseMetrics = baseMetrics;
    }

    public String getTypeName() {
        return typeName;
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
}
