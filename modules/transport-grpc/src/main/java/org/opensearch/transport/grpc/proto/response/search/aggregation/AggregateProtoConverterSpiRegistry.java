/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * SPI registry for AggregateProtoConverter implementations.
 * Mirrors the pattern from {@link org.opensearch.transport.grpc.proto.request.search.aggregation.AggregationBuilderProtoConverterSpiRegistry}.
 */
public class AggregateProtoConverterSpiRegistry implements AggregateProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(AggregateProtoConverterSpiRegistry.class);
    private final Map<Class<? extends InternalAggregation>, AggregateProtoConverter> converters = new HashMap<>();

    /**
     * Creates a new AggregateProtoConverterSpiRegistry.
     * External converters can be loaded via OpenSearch's ExtensiblePlugin mechanism
     * and registered manually via registerConverter() calls.
     */
    public AggregateProtoConverterSpiRegistry() {
        // External converters can be loaded via OpenSearch's ExtensiblePlugin mechanism
        // and registered manually via registerConverter() calls
    }

    /**
     * Registers a converter for a specific InternalAggregation type.
     *
     * @param converter The converter to register (must not be null)
     * @throws IllegalArgumentException if converter is null or handled type is null
     */
    public void registerConverter(AggregateProtoConverter converter) {
        if (converter == null) {
            throw new IllegalArgumentException("Converter cannot be null");
        }

        Class<? extends InternalAggregation> type = converter.getHandledAggregationType();
        if (type == null) {
            throw new IllegalArgumentException("Handled aggregation type cannot be null for converter: " + converter.getClass().getName());
        }

        AggregateProtoConverter existingConverter = converters.put(type, converter);
        if (existingConverter != null) {
            logger.warn(
                "Replacing existing converter for aggregation type {}: {} -> {}",
                type.getName(),
                existingConverter.getClass().getName(),
                converter.getClass().getName()
            );
        }

        logger.info("Registered aggregate converter for {}: {}", type.getName(), converter.getClass().getName());
    }

    /**
     * Converts an InternalAggregation to Aggregate protobuf.
     * Delegates type-specific conversion (including metadata) to registered converters.
     *
     * @param aggregation The InternalAggregation to convert (must not be null)
     * @return The corresponding Protocol Buffer Aggregate message
     * @throws IllegalArgumentException if aggregation is null or type is not supported
     * @throws IOException if an error occurs during protobuf conversion
     */
    @Override
    public Aggregate toProto(InternalAggregation aggregation) throws IOException {
        if (aggregation == null) {
            throw new IllegalArgumentException("Aggregation must not be null");
        }

        // First try exact class match
        AggregateProtoConverter converter = converters.get(aggregation.getClass());

        // If no exact match, walk up the class hierarchy to find a registered converter
        // This handles cases like Mockito proxies and subclasses
        if (converter == null) {
            for (Map.Entry<Class<? extends InternalAggregation>, AggregateProtoConverter> entry : converters.entrySet()) {
                if (entry.getKey().isInstance(aggregation)) {
                    converter = entry.getValue();
                    break;
                }
            }
        }

        if (converter == null) {
            throw new IllegalArgumentException("Unsupported aggregation type: " + aggregation.getClass().getName());
        }

        logger.debug("Using converter for {}: {}", aggregation.getClass().getName(), converter.getClass().getName());

        return converter.toProto(aggregation).build();
    }

    /**
     * Returns the number of registered converters.
     *
     * @return The converter count
     */
    public int size() {
        return converters.size();
    }
}
