/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed
 * under the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Final scalar cardinality produced by the distributed analytics engine. */
public final class InternalAnalyticsCardinality extends InternalNumericMetricsAggregation.SingleValue implements Cardinality {

    public static final String NAME = "dsl_analytics_cardinality";

    private final long value;

    public InternalAnalyticsCardinality(String name, long value, Map<String, Object> metadata) {
        super(name, metadata);
        this.value = value;
    }

    /** Reads an analytics cardinality from the transport stream. */
    public InternalAnalyticsCardinality(StreamInput in) throws IOException {
        super(in);
        value = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(value);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        if (aggregations.size() == 1) {
            return aggregations.get(0);
        }
        throw new UnsupportedOperationException("Analytics cardinality is already globally reduced and cannot be reduced again");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE.getPreferredName(), value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalAnalyticsCardinality that = (InternalAnalyticsCardinality) obj;
        return value == that.value;
    }
}
