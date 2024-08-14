/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    private final StarTreeFieldConfiguration starTreeConfig;
    private final List<String> dimensionNames;
    private final List<String> metricNames;

    public StarTreeField(String name, List<Dimension> dimensions, List<Metric> metrics, StarTreeFieldConfiguration starTreeConfig) {
        this.name = name;
        this.dimensionsOrder = dimensions;
        this.metrics = metrics;
        this.starTreeConfig = starTreeConfig;
        dimensionNames = new ArrayList<>();
        for (Dimension dimension : dimensions) {
            dimensionNames.addAll(dimension.getDimensionFieldsNames());
        }
        metricNames = new ArrayList<>();
        for (Metric metric : metrics) {
            for (MetricStat metricStat : metric.getMetrics()) {
                // TODO : revisit this post file formats
                metricNames.add(metric.getField() + "_" + metricStat.name());
            }
        }
    }

    public String getName() {
        return name;
    }

    public List<Dimension> getDimensionsOrder() {
        return dimensionsOrder;
    }

    public List<String> getDimensionNames() {
        return dimensionNames;
    }

    public List<String> getMetricNames() {
        return metricNames;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public StarTreeFieldConfiguration getStarTreeConfig() {
        return starTreeConfig;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        DateDimension dateDim = null;
        if (dimensionsOrder != null && !dimensionsOrder.isEmpty()) {
            builder.startArray("ordered_dimensions");
            for (Dimension dimension : dimensionsOrder) {
                // Handle dateDimension for later
                if (dimension instanceof DateDimension) {
                    dateDim = (DateDimension) dimension;
                    continue;
                }
                dimension.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (dateDim != null) {
            dateDim.toXContent(builder, params);
        }
        if (metrics != null && !metrics.isEmpty()) {
            builder.startArray("metrics");
            for (Metric metric : metrics) {
                metric.toXContent(builder, params);
            }
            builder.endArray();
        }
        starTreeConfig.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarTreeField that = (StarTreeField) o;
        return Objects.equals(name, that.name)
            && Objects.equals(dimensionsOrder, that.dimensionsOrder)
            && Objects.equals(metrics, that.metrics)
            && Objects.equals(starTreeConfig, that.starTreeConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dimensionsOrder, metrics, starTreeConfig);
    }
}
