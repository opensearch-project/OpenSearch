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
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Supported metric types for composite index
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum MetricStat {
    COUNT("count", null),
    SUM("sum", null),
    MIN("min", null),
    MAX("max", null),
    AVG("avg", new MetricStat[] { COUNT, SUM });

    private final String typeName;
    private final MetricStat[] derivedFrom;

    MetricStat(String typeName, MetricStat[] derivedFrom) {
        this.typeName = typeName;
        this.derivedFrom = derivedFrom;
    }

    public String getTypeName() {
        return typeName;
    }

    /**
     * Return the list of metrics that this metric is derived from
     * For example, AVG is derived from COUNT and SUM
     */
    public List<MetricStat> getDerivedFromMetrics() {
        return Arrays.asList(derivedFrom);
    }

    /**
     * Return true if this metric is derived from other metrics
     * For example, AVG is derived from COUNT and SUM
     */
    public boolean isDerivedMetric() {
        return derivedFrom != null;
    }

    /**
     * Return required metrics for every metric field in star tree field
     */
    public static Set<MetricStat> getRequiredMetrics() {
        return Collections.singleton(MetricStat.COUNT);
    }

    public static MetricStat fromTypeName(String typeName) {
        for (MetricStat metric : MetricStat.values()) {
            if (metric.getTypeName().equalsIgnoreCase(typeName)) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Invalid metric stat: " + typeName);
    }
}
