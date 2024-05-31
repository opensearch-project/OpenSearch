/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Supported metric types for composite index
 */
@ExperimentalApi
public enum MetricType {
    COUNT("count"),
    AVG("avg"),
    SUM("sum"),
    MIN("min"),
    MAX("max");

    private final String typeName;

    MetricType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public static MetricType fromTypeName(String typeName) {
        for (MetricType metric : MetricType.values()) {
            if (metric.getTypeName().equalsIgnoreCase(typeName)) {
                return metric;
            }
        }
        throw new IllegalArgumentException("Invalid metric type: " + typeName);
    }
}
