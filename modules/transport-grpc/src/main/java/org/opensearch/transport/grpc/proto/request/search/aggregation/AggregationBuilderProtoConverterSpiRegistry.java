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

/**
 * SPI registry for AggregationBuilderProtoConverter implementations.
 * Handles converter lookup, metadata, and recursive subaggregation parsing.
 */
@Singleton
public class AggregationBuilderProtoConverterSpiRegistry implements AggregationBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(AggregationBuilderProtoConverterSpiRegistry.class);
    private final Map<AggregationContainer.AggregationContainerCase, AggregationBuilderProtoConverter> converterMap = new HashMap<>();

    @Inject
    public AggregationBuilderProtoConverterSpiRegistry() {
        // External converters are loaded via OpenSearch's ExtensiblePlugin mechanism
        // and registered manually via registerConverter() calls
    }

    /**
     * Converts protobuf to AggregationBuilder with metadata and subaggregations.
     * Mirrors {@link org.opensearch.search.aggregations.AggregatorFactories#parseAggregators}.
     */
    @Override
    public AggregationBuilder fromProto(String name, AggregationContainer container) {
        if (container == null) {
            throw new IllegalArgumentException("Aggregation container cannot be null");
        }

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Aggregation name cannot be null or empty");
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

        // TODO: Nested aggregations not yet supported in proto definition
        // if (container.getAggregationsCount() > 0) {
        //     AggregatorFactories.Builder subFactories = new AggregatorFactories.Builder();
        //
        //     for (Map.Entry<String, AggregationContainer> entry : container.getAggregationsMap().entrySet()) {
        //         String subAggName = entry.getKey();
        //         AggregationContainer subAggContainer = entry.getValue();
        //
        //         logger.debug("Parsing subaggregation '{}' for parent '{}'", subAggName, name);
        //         AggregationBuilder subAgg = fromProto(subAggName, subAggContainer);
        //         subFactories.addAggregator(subAgg);
        //     }
        //
        //     builder.subAggregations(subFactories);
        //     logger.debug("Added {} subaggregation(s) to aggregation '{}'", container.getAggregationsCount(), name);
        // }

        return builder;
    }

    public int size() {
        return converterMap.size();
    }

    public void setRegistryOnAllConverters(AggregationBuilderProtoConverterRegistry registry) {
        for (AggregationBuilderProtoConverter converter : converterMap.values()) {
            converter.setRegistry(registry);
        }
        logger.info("Set registry on {} aggregation converter(s)", converterMap.size());
    }

    public void registerConverter(AggregationBuilderProtoConverter converter) {
        if (converter == null) {
            throw new IllegalArgumentException("Converter cannot be null");
        }

        AggregationContainer.AggregationContainerCase aggregationCase = converter.getHandledAggregationCase();

        if (aggregationCase == null) {
            throw new IllegalArgumentException(
                "Handled aggregation case cannot be null for converter: " + converter.getClass().getName()
            );
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
