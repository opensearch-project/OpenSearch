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
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverter;

/**
 * Converter for Max aggregation.
 */
public class MaxAggregationBuilderProtoConverter implements AggregationBuilderProtoConverter {

    /**
     * Constructs a new MaxAggregationBuilderProtoConverter.
     */
    public MaxAggregationBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public AggregationContainer.AggregationContainerCase getHandledAggregationCase() {
        return AggregationContainer.AggregationContainerCase.MAX;
    }

    /**
     * Converts protobuf MaxAggregation to {@link MaxAggregationBuilder}.
     * Equivalent to REST parsing via {@link MaxAggregationBuilder#PARSER}.
     *
     * @param name the aggregation name
     * @param container the protobuf aggregation container
     * @return the constructed {@link MaxAggregationBuilder}
     */
    @Override
    public AggregationBuilder fromProto(String name, AggregationContainer container) {
        if (!container.hasMax()) {
            throw new IllegalArgumentException("Container does not contain Max aggregation");
        }
        return MaxAggregationProtoUtils.fromProto(name, container.getMax());
    }
}
