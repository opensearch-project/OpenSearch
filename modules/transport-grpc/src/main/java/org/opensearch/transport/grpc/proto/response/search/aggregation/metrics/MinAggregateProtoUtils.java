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
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

/**
 * Utility class for converting {@link InternalMin} aggregation results to
 * {@link SingleMetricAggregateBase} protocol buffer format.
 */
public class MinAggregateProtoUtils {

    private MinAggregateProtoUtils() {
        // Utility class
    }

    /**
     * Converts an InternalMin aggregation result to SingleMetricAggregateBase proto.
     *
     * <p>Mirrors {@link InternalMin#doXContentBody} structure exactly.
     *
     * @param internalMin The InternalMin aggregation result from OpenSearch
     * @return SingleMetricAggregateBase proto with value, value_as_string, and metadata
     * @see InternalMin#doXContentBody
     */
    public static SingleMetricAggregateBase toProto(InternalMin internalMin) {
        SingleMetricAggregateBase.Builder builder = SingleMetricAggregateBase.newBuilder();

        AggregateProtoUtils.addMetadata(builder::setMeta, internalMin);

        double min = internalMin.getValue();
        boolean hasValue = !Double.isInfinite(min);

        SingleMetricAggregateBaseAllOfValue.Builder valueBuilder = SingleMetricAggregateBaseAllOfValue.newBuilder();
        if (hasValue) {
            valueBuilder.setDouble(min);
        } else {
            valueBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        }
        builder.setValue(valueBuilder.build());

        if (hasValue && internalMin.getFormat() != DocValueFormat.RAW) {
            builder.setValueAsString(internalMin.getValueAsString());
        }

        return builder.build();
    }
}
