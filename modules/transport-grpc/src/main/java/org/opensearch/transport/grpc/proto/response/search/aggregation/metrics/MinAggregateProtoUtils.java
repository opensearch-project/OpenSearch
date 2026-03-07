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
import org.opensearch.search.aggregations.metrics.InternalMin;

/**
 * Utility class for converting {@link InternalMin} aggregation results to Aggregate protocol buffer format.
 */
public class MinAggregateProtoUtils {

    private MinAggregateProtoUtils() {
        // Utility class
    }

    /**
     * Converts an InternalMin aggregation result to Aggregate proto.
     *
     * <p>Mirrors {@link InternalMin#doXContentBody} structure exactly.
     *
     * @param internalMin The InternalMin aggregation result from OpenSearch
     * @return Aggregate proto (metadata not included)
     * @see InternalMin#doXContentBody
     */
    public static Aggregate toProto(InternalMin internalMin) {
        Aggregate.Builder builder = Aggregate.newBuilder();

        double min = internalMin.getValue();
        boolean hasValue = !Double.isInfinite(min);

        SingleMetricAggregateBaseValue.Builder valueBuilder = SingleMetricAggregateBaseValue.newBuilder();
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
