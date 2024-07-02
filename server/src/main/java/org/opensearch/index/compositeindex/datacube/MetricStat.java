/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Supported metric types for composite index
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum MetricStat {
    COUNT("count"),
    AVG("avg"),
    SUM("sum"),
    MIN("min"),
    MAX("max");

    private final String typeName;

    MetricStat(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
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
