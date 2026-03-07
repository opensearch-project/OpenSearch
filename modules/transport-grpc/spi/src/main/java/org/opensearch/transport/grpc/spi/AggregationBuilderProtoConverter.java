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
 * SPI interface for converting protobuf aggregation containers to OpenSearch AggregationBuilders.
 * Follows the same pattern as {@link QueryBuilderProtoConverter}.
 *
 * <p>The registry handles metadata and subaggregations. Converters should delegate to existing
 * {@code *ProtoUtils} classes.
 *
 * @see org.opensearch.search.aggregations.AggregatorFactories#parseAggregators
 */
public interface AggregationBuilderProtoConverter {

    /**
     * Returns the aggregation case this converter handles.
     */
    AggregationContainer.AggregationContainerCase getHandledAggregationCase();

    /**
     * Converts a protobuf aggregation container to an AggregationBuilder.
     * Similar to {@link org.opensearch.search.aggregations.AggregatorFactories.Builder#addAggregator}.
     *
     * @param name The aggregation name
     * @param container The protobuf container
     * @return The OpenSearch AggregationBuilder
     */
    AggregationBuilder fromProto(String name, AggregationContainer container);

    /**
     * Sets the registry for nested aggregations. Default no-op for metric aggregations.
     */
    default void setRegistry(AggregationBuilderProtoConverterRegistry registry) {}
}
