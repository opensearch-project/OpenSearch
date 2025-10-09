/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for QueryBuilderProtoConverter implementations.
 * This class discovers and manages all available converters.
 */
@Singleton
public class QueryBuilderProtoConverterSpiRegistry {

    private static final Logger logger = LogManager.getLogger(QueryBuilderProtoConverterSpiRegistry.class);
    private final Map<QueryContainer.QueryContainerCase, QueryBuilderProtoConverter> converterMap = new HashMap<>();

    /**
    * Creates a new registry. External converters will be registered
    * via the OpenSearch ExtensiblePlugin mechanism.
    */
    @Inject
    public QueryBuilderProtoConverterSpiRegistry() {
        // External converters are loaded via OpenSearch's ExtensiblePlugin mechanism
        // and registered manually via registerConverter() calls
    }

    /**
     * Converts a protobuf query container to an OpenSearch QueryBuilder.
     *
     * @param queryContainer The protobuf query container
     * @return The corresponding OpenSearch QueryBuilder
     * @throws IllegalArgumentException if no converter can handle the query
     */
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null) {
            throw new IllegalArgumentException("Query container cannot be null");
        }

        // Use direct map lookup for better performance
        QueryContainer.QueryContainerCase queryCase = queryContainer.getQueryContainerCase();
        QueryBuilderProtoConverter converter = converterMap.get(queryCase);

        if (converter != null) {
            logger.debug("Using converter for {}: {}", queryCase, converter.getClass().getName());
            return converter.fromProto(queryContainer);
        }

        throw new IllegalArgumentException("Unsupported query type in container: " + queryContainer + " (case: " + queryCase + ")");
    }

    /**
     * Gets the number of registered converters.
     *
     * @return The number of registered converters
     */
    public int size() {
        return converterMap.size();
    }

    /**
     * Sets the registry on all registered converters.
     * This is used to inject the complete registry into converters that need it (like BoolQueryBuilderProtoConverter).
     *
     * @param registry The registry to inject into all converters
     */
    public void setRegistryOnAllConverters(QueryBuilderProtoConverterRegistry registry) {
        for (QueryBuilderProtoConverter converter : converterMap.values()) {
            converter.setRegistry(registry);
        }
        logger.info("Set registry on {} converters", converterMap.size());
    }

    /**
     * Registers a new converter.
     *
     * @param converter The converter to register
     * @throws IllegalArgumentException if the converter is null or its handled query case is invalid
     */
    public void registerConverter(QueryBuilderProtoConverter converter) {
        if (converter == null) {
            throw new IllegalArgumentException("Converter cannot be null");
        }

        QueryContainer.QueryContainerCase queryCase = converter.getHandledQueryCase();

        if (queryCase == null) {
            throw new IllegalArgumentException("Handled query case cannot be null for converter: " + converter.getClass().getName());
        }

        if (queryCase == QueryContainer.QueryContainerCase.QUERYCONTAINER_NOT_SET) {
            throw new IllegalArgumentException(
                "Cannot register converter for QUERYCONTAINER_NOT_SET case: " + converter.getClass().getName()
            );
        }

        QueryBuilderProtoConverter existingConverter = converterMap.put(queryCase, converter);
        if (existingConverter != null) {
            logger.warn(
                "Replacing existing converter for query type {}: {} -> {}",
                queryCase,
                existingConverter.getClass().getName(),
                converter.getClass().getName()
            );
        }

        logger.info("Registered query converter for {}: {}", queryCase, converter.getClass().getName());
    }
}
