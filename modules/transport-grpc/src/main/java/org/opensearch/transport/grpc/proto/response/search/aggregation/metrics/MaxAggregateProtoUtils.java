/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.metrics;

import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.SingleMetricAggregateBase;
import org.opensearch.protobufs.SingleMetricAggregateBaseAllOfValue;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

/**
 * Utility class for converting {@link InternalMax} aggregation results to
 * {@link SingleMetricAggregateBase} protocol buffer format.
 */
public class MaxAggregateProtoUtils {

    private MaxAggregateProtoUtils() {
        // Utility class
    }

    /**
     * Converts an InternalMax aggregation result to SingleMetricAggregateBase proto.
     *
     * <p>Mirrors {@link InternalMax#doXContentBody} structure exactly.
     *
     * @param internalMax The InternalMax aggregation result from OpenSearch
     * @return SingleMetricAggregateBase proto with value, value_as_string, and metadata
     * @see InternalMax#doXContentBody
     */
    public static SingleMetricAggregateBase toProto(InternalMax internalMax) {
        SingleMetricAggregateBase.Builder builder = SingleMetricAggregateBase.newBuilder();

        AggregateProtoUtils.addMetadata(builder::setMeta, internalMax);

        double max = internalMax.getValue();
        boolean hasValue = Double.isFinite(max);

        SingleMetricAggregateBaseAllOfValue.Builder valueBuilder = SingleMetricAggregateBaseAllOfValue.newBuilder();
        if (hasValue) {
            valueBuilder.setDouble(max);
        } else {
            valueBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        }
        builder.setValue(valueBuilder.build());

        if (hasValue && internalMax.getFormat() != DocValueFormat.RAW) {
            builder.setValueAsString(internalMax.getValueAsString());
        }

        return builder.build();
    }
}
