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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Registry for QueryBuilderProtoConverter implementations.
 * This class discovers and manages all available converters.
 */
@Singleton
public class QueryBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(QueryBuilderProtoConverterRegistry.class);
    private final Map<QueryContainer.QueryContainerCase, QueryBuilderProtoConverter> converterMap = new HashMap<>();

    /**
     * Creates a new registry and loads all available converters.
     */
    @Inject
    public QueryBuilderProtoConverterRegistry() {
        // Load built-in converters
        registerBuiltInConverters();

        // Discover converters from other plugins using Java's ServiceLoader
        loadExternalConverters();
    }

    /**
     * Registers the built-in converters.
     * Protected for testing purposes.
     */
    protected void registerBuiltInConverters() {
        // Add built-in converters
        registerConverter(new MatchAllQueryBuilderProtoConverter());
        registerConverter(new MatchNoneQueryBuilderProtoConverter());
        registerConverter(new TermQueryBuilderProtoConverter());
        registerConverter(new TermsQueryBuilderProtoConverter());

        logger.info("Registered {} built-in query converters", converterMap.size());
    }

    /**
     * Loads external converters using Java's ServiceLoader mechanism.
     * Protected for testing purposes.
     */
    protected void loadExternalConverters() {
        ServiceLoader<QueryBuilderProtoConverter> serviceLoader = ServiceLoader.load(QueryBuilderProtoConverter.class);
        Iterator<QueryBuilderProtoConverter> iterator = serviceLoader.iterator();

        int count = 0;
        int failedCount = 0;
        while (iterator.hasNext()) {
            try {
                QueryBuilderProtoConverter converter = iterator.next();
                registerConverter(converter);
                count++;
                logger.info("Loaded external query converter for {}: {}", converter.getHandledQueryCase(), converter.getClass().getName());
            } catch (Exception e) {
                failedCount++;
                logger.error("Failed to load external query converter", e);
            }
        }

        logger.info("Loaded {} external query converters ({} failed)", count, failedCount);
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

        logger.debug("Registered query converter for {}: {}", queryCase, converter.getClass().getName());
    }
}
