/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.Dimension;
import org.opensearch.index.compositeindex.Metric;

import java.io.IOException;
import java.util.List;

/**
 * Star tree field which contains dimensions, metrics and specs
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeField implements ToXContent {
    private final String name;
    private final List<Dimension> dimensionsOrder;
    private final List<Metric> metrics;
    private final StarTreeFieldSpec starTreeFieldSpec;

    public StarTreeField(String name, List<Dimension> dimensions, List<Metric> metrics, StarTreeFieldSpec starTreeFieldSpec) {
        this.name = name;
        this.dimensionsOrder = dimensions;
        this.metrics = metrics;
        this.starTreeFieldSpec = starTreeFieldSpec;
    }

    public String getName() {
        return name;
    }

    public List<Dimension> getDimensionsOrder() {
        return dimensionsOrder;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public StarTreeFieldSpec getSpec() {
        return starTreeFieldSpec;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        if (dimensionsOrder != null && !dimensionsOrder.isEmpty()) {
            builder.startObject("ordered_dimensions");
            for (Dimension dimension : dimensionsOrder) {
                dimension.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (metrics != null && !metrics.isEmpty()) {
            builder.startObject("metrics");
            for (Metric metric : metrics) {
                metric.toXContent(builder, params);
            }
            builder.endObject();
        }
        starTreeFieldSpec.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
