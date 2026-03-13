/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.metrics;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverter;

/**
 * Converter for Min aggregation. Delegates to {@link MinAggregationProtoUtils}.
 * Mirrors REST parsing via {@link org.opensearch.search.aggregations.metrics.MinAggregationBuilder#PARSER}.
 */
public class MinAggregationBuilderProtoConverter implements AggregationBuilderProtoConverter {

    @Override
    public AggregationContainer.AggregationContainerCase getHandledAggregationCase() {
        return AggregationContainer.AggregationContainerCase.MIN;
    }

    /**
     * Converts protobuf MinAggregation to {@link org.opensearch.search.aggregations.metrics.MinAggregationBuilder}.
     * Equivalent to REST parsing via {@link org.opensearch.search.aggregations.metrics.MinAggregationBuilder#PARSER}.
     */
    @Override
    public AggregationBuilder fromProto(String name, AggregationContainer container) {
        if (!container.hasMin()) {
            throw new IllegalArgumentException("Container does not contain Min aggregation");
        }
        return MinAggregationProtoUtils.fromProto(name, container.getMin());
    }
}
