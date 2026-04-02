/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Results of the top_metrics aggregation.
 *
 * @opensearch.internal
 */
public class InternalTopMetrics extends InternalAggregation {

    private static final String TOP_FIELD = "top";
    private static final String SORT_FIELD = "sort";
    private static final String METRICS_FIELD = "metrics";

    private final InternalTopHits topHits;
    private final List<String> metricFields;

    public InternalTopMetrics(String name, InternalTopHits topHits, List<String> metricFields, Map<String, Object> metadata) {
        super(name, metadata);
        this.topHits = topHits;
        this.metricFields = List.copyOf(metricFields);
    }

    public InternalTopMetrics(StreamInput in) throws IOException {
        super(in);
        this.topHits = (InternalTopHits) in.readNamedWriteable(InternalAggregation.class);
        this.metricFields = in.readStringList();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(topHits);
        out.writeStringCollection(metricFields);
    }

    @Override
    public String getWriteableName() {
        return TopMetricsAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        if (aggregations.isEmpty()) {
            throw new IllegalStateException("[top_metrics] reduce requires at least one aggregation");
        }
        List<InternalAggregation> topHitsAggs = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            if ((aggregation instanceof InternalTopMetrics) == false) {
                throw new IllegalStateException("Expected InternalTopMetrics but got [" + aggregation.getClass().getName() + "]");
            }
            topHitsAggs.add(((InternalTopMetrics) aggregation).topHits);
        }
        InternalTopHits reducedTopHits = (InternalTopHits) topHitsAggs.get(0).reduce(topHitsAggs, reduceContext);
        return new InternalTopMetrics(name, reducedTopHits, metricFields, getMetadata());
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TOP_FIELD);
        for (SearchHit hit : topHits.getHits().getHits()) {
            builder.startObject();
            Object[] sortValues = hit.getSortValues();
            builder.startArray(SORT_FIELD);
            for (Object sortValue : sortValues) {
                builder.value(sortValue);
            }
            builder.endArray();

            builder.startObject(METRICS_FIELD);
            for (String metricField : metricFields) {
                DocumentField field = hit.field(metricField);
                if (field == null || field.getValues().isEmpty()) {
                    builder.nullField(metricField);
                } else if (field.getValues().size() == 1) {
                    builder.field(metricField);
                    Object metricValue = field.getValues().get(0);
                    builder.value(metricValue);
                } else {
                    builder.field(metricField, field.getValues());
                }
            }
            builder.endObject();
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topHits, metricFields);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalTopMetrics that = (InternalTopMetrics) obj;
        return Objects.equals(topHits, that.topHits) && Objects.equals(metricFields, that.metricFields);
    }

    InternalTopHits getTopHits() {
        return topHits;
    }

    List<String> getMetricFields() {
        return metricFields;
    }
}
