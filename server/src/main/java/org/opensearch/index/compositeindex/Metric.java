/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;

/**
 * Holds details of metrics field as part of composite field
 */
@ExperimentalApi
public class Metric {
    private final String field;
    private final List<MetricType> metrics;

    public Metric(String field, List<MetricType> metrics) {
        this.field = field;
        this.metrics = metrics;
    }

    public String getField() {
        return field;
    }

    public List<MetricType> getMetrics() {
        return metrics;
    }

    public void setDefaults(CompositeIndexConfig compositeIndexConfig) {
        if (metrics.isEmpty()) {
            metrics.addAll(compositeIndexConfig.getDefaultMetrics());
        }
    }

}
