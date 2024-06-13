/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Holds details of metrics field as part of composite field
 */
@ExperimentalApi
public class Metric implements ToXContent {
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(field);
        builder.startArray("metrics");
        for (MetricType metricType : metrics) {
            builder.value(metricType.getTypeName());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
