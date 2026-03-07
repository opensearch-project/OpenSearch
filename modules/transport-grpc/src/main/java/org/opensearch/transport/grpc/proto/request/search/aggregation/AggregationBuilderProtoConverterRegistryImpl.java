/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.transport.grpc.proto.request.search.aggregation.metrics.MaxAggregationBuilderProtoConverter;
import org.opensearch.transport.grpc.proto.request.search.aggregation.metrics.MinAggregationBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverterRegistry;

/**
 * Registry for AggregationBuilderProtoConverter implementations.
 */
@Singleton
public class AggregationBuilderProtoConverterRegistryImpl implements AggregationBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(AggregationBuilderProtoConverterRegistryImpl.class);
    private final AggregationBuilderProtoConverterSpiRegistry delegate;

    /**
     * Creates a new AggregationBuilderProtoConverterRegistryImpl and registers built-in converters.
     */
    @Inject
    public AggregationBuilderProtoConverterRegistryImpl() {
        this.delegate = new AggregationBuilderProtoConverterSpiRegistry();

        registerBuiltInConverters();
    }

    /**
     * Registers all built-in aggregation converters.
     */
    protected void registerBuiltInConverters() {
        // Register metric aggregation converters
        delegate.registerConverter(new MinAggregationBuilderProtoConverter());
        delegate.registerConverter(new MaxAggregationBuilderProtoConverter());

        // Future: Register bucket aggregation converters here
        // Example: delegate.registerConverter(new TermsAggregationBuilderProtoConverter());

        // Set the registry on all converters so they can access each other
        delegate.setRegistryOnAllConverters(this);

        logger.info("Registered {} built-in aggregation converter(s)", delegate.size());
    }

    /**
     * Converts protobuf to AggregationBuilder.
     * Mirrors {@link org.opensearch.search.aggregations.AggregatorFactories#parseAggregators}.
     *
     * @param name The aggregation name
     * @param container The protobuf container
     * @return The OpenSearch AggregationBuilder
     */
    @Override
    public AggregationBuilder fromProto(String name, AggregationContainer container) {
        return delegate.fromProto(name, container);
    }

    /**
     * Registers an external aggregation converter.
     *
     * @param converter The converter to register
     */
    public void registerConverter(AggregationBuilderProtoConverter converter) {
        delegate.registerConverter(converter);
    }

    /**
     * Updates the registry reference on all registered converters.
     */
    public void updateRegistryOnAllConverters() {
        delegate.setRegistryOnAllConverters(this);
    }
}
