/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.spi;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.search.aggregations.AggregationBuilder;

/**
 * SPI interface for the aggregation converter registry.
 * Handles converter lookup, metadata, and recursive subaggregation parsing.
 *
 * @see org.opensearch.search.aggregations.AggregatorFactories#parseAggregators
 */
public interface AggregationBuilderProtoConverterRegistry {

    /**
     * Converts a protobuf aggregation container to an OpenSearch AggregationBuilder.
     * Similar to {@link org.opensearch.search.aggregations.AggregatorFactories#parseAggregators}.
     *
     * @param name The aggregation name
     * @param container The protobuf container
     * @return The AggregationBuilder with metadata and subaggregations
     */
    AggregationBuilder fromProto(String name, AggregationContainer container);
}
