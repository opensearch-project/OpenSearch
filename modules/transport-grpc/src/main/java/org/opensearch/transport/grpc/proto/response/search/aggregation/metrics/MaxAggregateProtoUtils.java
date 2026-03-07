/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.metrics;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.SingleMetricAggregateBaseValue;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.InternalMax;

/**
 * Utility class for converting {@link InternalMax} aggregation results to Aggregate protocol buffer format.
 */
public class MaxAggregateProtoUtils {

    private MaxAggregateProtoUtils() {
        // Utility class
    }

    /**
     * Converts an InternalMax aggregation result to Aggregate proto.
     *
     * <p>Mirrors {@link InternalMax#doXContentBody} structure exactly.
     *
     * @param internalMax The InternalMax aggregation result from OpenSearch
     * @return Aggregate proto (metadata not included)
     * @see InternalMax#doXContentBody
     */
    public static Aggregate toProto(InternalMax internalMax) {
        Aggregate.Builder builder = Aggregate.newBuilder();

        double max = internalMax.getValue();
        boolean hasValue = !Double.isInfinite(max);

        SingleMetricAggregateBaseValue.Builder valueBuilder = SingleMetricAggregateBaseValue.newBuilder();
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
