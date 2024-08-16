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
    MAX("max"),
    DOC_COUNT("doc_count", true);

    private final String typeName;

    // System field stats cannot be used as input for user metric types
    private final boolean isSystemFieldStat;

    MetricStat(String typeName) {
        this(typeName, false);
    }

    MetricStat(String typeName, boolean isSystemFieldStat) {
        this.typeName = typeName;
        this.isSystemFieldStat = isSystemFieldStat;
    }

    public String getTypeName() {
        return typeName;
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
