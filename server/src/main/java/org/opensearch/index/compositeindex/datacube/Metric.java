/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Holds details of metrics field as part of composite field
 */
@ExperimentalApi
public class Metric implements ToXContent {
    private final String field;
    private final List<MetricStat> metrics;
    private final List<MetricStat> baseMetrics;

    public Metric(String field, List<MetricStat> metrics) {
        this.field = field;
        this.metrics = metrics;
        this.baseMetrics = new ArrayList<>();
        for (MetricStat metricStat : metrics) {
            if (metricStat.isDerivedMetric()) {
                continue;
            }
            baseMetrics.add(metricStat);
        }
    }

    public String getField() {
        return field;
    }

    public List<MetricStat> getMetrics() {
        return metrics;
    }

    /**
     * Returns only the base metrics
     */
    public List<MetricStat> getBaseMetrics() {
        return baseMetrics;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", field);
        builder.startArray("stats");
        for (MetricStat metricType : metrics) {
            builder.value(metricType.getTypeName());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metric metric = (Metric) o;
        return Objects.equals(field, metric.field) && Objects.equals(metrics, metric.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, metrics);
    }
}
