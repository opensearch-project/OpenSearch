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
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.transport.grpc.proto.request.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverterRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * SPI registry for AggregationBuilderProtoConverter implementations.
 * Handles converter lookup, metadata, and recursive subaggregation parsing.
 */
@Singleton
public class AggregationBuilderProtoConverterSpiRegistry implements AggregationBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(AggregationBuilderProtoConverterSpiRegistry.class);
    private final Map<AggregationContainer.AggregationContainerCase, AggregationBuilderProtoConverter> converterMap = new HashMap<>();

    /**
     * Creates a new AggregationBuilderProtoConverterSpiRegistry.
     * External converters are loaded via OpenSearch's ExtensiblePlugin mechanism
     * and registered manually via registerConverter() calls.
     */
    @Inject
    public AggregationBuilderProtoConverterSpiRegistry() {
        // External converters are loaded via OpenSearch's ExtensiblePlugin mechanism
        // and registered manually via registerConverter() calls
    }

    /**
     * Converts protobuf to AggregationBuilder with metadata and subaggregations.
     * Mirrors {@link AggregatorFactories#parseAggregators}.
     *
     * @param name The aggregation name
     * @param container The protobuf container
     * @return The OpenSearch AggregationBuilder
     */
    @Override
    public AggregationBuilder fromProto(String name, AggregationContainer container) {
        if (container == null) {
            throw new IllegalArgumentException("Aggregation container cannot be null");
        }

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Aggregation name cannot be null or empty");
        }

        Matcher validAggMatcher = AggregatorFactories.VALID_AGG_NAME.matcher(name);
        if (!validAggMatcher.matches()) {
            throw new IllegalArgumentException(
                "Invalid aggregation name [" + name + "]. Aggregation names can contain any character except '[', ']', and '>'"
            );
        }

        AggregationContainer.AggregationContainerCase aggregationCase = container.getAggregationContainerCase();
        AggregationBuilderProtoConverter converter = converterMap.get(aggregationCase);

        if (converter == null) {
            throw new IllegalArgumentException(
                "Unsupported aggregation type in container: " + container + " (case: " + aggregationCase + ")"
            );
        }

        logger.debug("Using converter for {}: {}", aggregationCase, converter.getClass().getName());

        AggregationBuilder builder = converter.fromProto(name, container);

        if (container.hasMeta()) {
            Map<String, Object> metadata = ObjectMapProtoUtils.fromProto(container.getMeta());
            builder.setMetadata(metadata);
            logger.debug("Applied metadata to aggregation '{}': {}", name, metadata);
        }

        return builder;
    }

    /**
     * Returns the number of registered converters.
     *
     * @return The converter count
     */
    public int size() {
        return converterMap.size();
    }

    /**
     * Sets the registry on all registered converters.
     *
     * @param registry The registry to set
     */
    public void setRegistryOnAllConverters(AggregationBuilderProtoConverterRegistry registry) {
        for (AggregationBuilderProtoConverter converter : converterMap.values()) {
            converter.setRegistry(registry);
        }
        logger.info("Set registry on {} aggregation converter(s)", converterMap.size());
    }

    /**
     * Registers a converter for a specific aggregation type.
     *
     * @param converter The converter to register
     */
    public void registerConverter(AggregationBuilderProtoConverter converter) {
        if (converter == null) {
            throw new IllegalArgumentException("Converter cannot be null");
        }

        AggregationContainer.AggregationContainerCase aggregationCase = converter.getHandledAggregationCase();

        if (aggregationCase == null) {
            throw new IllegalArgumentException("Handled aggregation case cannot be null for converter: " + converter.getClass().getName());
        }

        if (aggregationCase == AggregationContainer.AggregationContainerCase.AGGREGATIONCONTAINER_NOT_SET) {
            throw new IllegalArgumentException(
                "Cannot register converter for AGGREGATIONCONTAINER_NOT_SET case: " + converter.getClass().getName()
            );
        }

        AggregationBuilderProtoConverter existingConverter = converterMap.put(aggregationCase, converter);
        if (existingConverter != null) {
            logger.warn(
                "Replacing existing converter for aggregation type {}: {} -> {}",
                aggregationCase,
                existingConverter.getClass().getName(),
                converter.getClass().getName()
            );
        }

        logger.info("Registered aggregation converter for {}: {}", aggregationCase, converter.getClass().getName());
    }
}
