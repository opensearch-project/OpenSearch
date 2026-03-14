/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MaxAggregateProtoConverter;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MinAggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;

/**
 * Public facade for the aggregate converter registry that registers built-in converters.
 * Provides the main entry point for external code to convert aggregations to protobuf.
 */
public class AggregateProtoConverterRegistryImpl implements AggregateProtoConverterRegistry {

    private final AggregateProtoConverterSpiRegistry spiRegistry;

    public AggregateProtoConverterRegistryImpl() {
        this.spiRegistry = new AggregateProtoConverterSpiRegistry();
        registerBuiltInConverters();
    }

    /**
     * Registers all built-in aggregate converters.
     * External plugins can extend this by calling registerConverter() on the SPI registry.
     */
    private void registerBuiltInConverters() {
        spiRegistry.registerConverter(new MinAggregateProtoConverter());
        spiRegistry.registerConverter(new MaxAggregateProtoConverter());
    }

    @Override
    public Aggregate toProto(InternalAggregation aggregation) throws IOException {
        return spiRegistry.toProto(aggregation);
    }

    /**
     * Returns the underlying SPI registry for advanced use cases.
     *
     * @return The SPI registry
     */
    public AggregateProtoConverterSpiRegistry getSpiRegistry() {
        return spiRegistry;
    }
}
