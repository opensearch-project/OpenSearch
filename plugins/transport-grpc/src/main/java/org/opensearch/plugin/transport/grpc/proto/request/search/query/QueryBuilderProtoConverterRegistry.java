/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Registry for QueryBuilderProtoConverter implementations.
 * This class discovers and manages all available converters.
 */
@Singleton
public class QueryBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(QueryBuilderProtoConverterRegistry.class);
    private final List<QueryBuilderProtoConverter> converters = new ArrayList<>();

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
        converters.add(new MatchAllQueryBuilderProtoConverter());
        converters.add(new MatchNoneQueryBuilderProtoConverter());
        converters.add(new TermQueryBuilderProtoConverter());
        converters.add(new TermsQueryBuilderProtoConverter());
        // converters.add(new KNNQueryBuilderProtoConverter());

        logger.debug("Registered {} built-in query converters", converters.size());
    }

    /**
     * Loads external converters using Java's ServiceLoader mechanism.
     * Protected for testing purposes.
     */
    protected void loadExternalConverters() {
        ServiceLoader<QueryBuilderProtoConverter> serviceLoader = ServiceLoader.load(QueryBuilderProtoConverter.class);
        Iterator<QueryBuilderProtoConverter> iterator = serviceLoader.iterator();

        int count = 0;
        while (iterator.hasNext()) {
            QueryBuilderProtoConverter converter = iterator.next();
            converters.add(converter);
            count++;
            logger.debug("Loaded external query converter: {}", converter.getClass().getName());
        }

        logger.debug("Loaded {} external query converters", count);
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

        for (QueryBuilderProtoConverter converter : converters) {
            if (converter.canHandle(queryContainer)) {
                return converter.fromProto(queryContainer);
            }
        }

        throw new IllegalArgumentException("Unsupported query type in container: " + queryContainer);
    }

    /**
     * Registers a new converter.
     *
     * @param converter The converter to register
     */
    public void registerConverter(QueryBuilderProtoConverter converter) {
        if (converter != null) {
            converters.add(converter);
            logger.debug("Manually registered query converter: {}", converter.getClass().getName());
        }
    }
}
