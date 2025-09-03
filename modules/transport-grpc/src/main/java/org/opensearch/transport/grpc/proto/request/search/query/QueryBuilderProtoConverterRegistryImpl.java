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

/**
 * Registry for QueryBuilderProtoConverter implementations.
 * This class wraps the SPI registry and adds built-in converters for the transport-grpc module.
 */
@Singleton
public class QueryBuilderProtoConverterRegistryImpl implements QueryBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(QueryBuilderProtoConverterRegistryImpl.class);
    private final QueryBuilderProtoConverterSpiRegistry delegate;

    /**
     * Creates a new registry and loads all available converters.
     */
    @Inject
    public QueryBuilderProtoConverterRegistryImpl() {
        // Create the SPI registry which loads external converters
        this.delegate = new QueryBuilderProtoConverterSpiRegistry();

        // Register built-in converters for this module
        registerBuiltInConverters();
    }

    /**
     * Registers the built-in converters.
     * Protected for testing purposes.
     */
    protected void registerBuiltInConverters() {
        // Add built-in converters
        delegate.registerConverter(new MatchAllQueryBuilderProtoConverter());
        delegate.registerConverter(new MatchNoneQueryBuilderProtoConverter());
        delegate.registerConverter(new TermQueryBuilderProtoConverter());
        delegate.registerConverter(new TermsQueryBuilderProtoConverter());

        logger.info("Registered {} built-in query converters", delegate.size());
    }

    /**
     * Converts a protobuf query container to an OpenSearch QueryBuilder.
     *
     * @param queryContainer The protobuf query container
     * @return The corresponding OpenSearch QueryBuilder
     * @throws IllegalArgumentException if the query cannot be converted
     */
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        return delegate.fromProto(queryContainer);
    }

    /**
     * Registers a new converter.
     *
     * @param converter The converter to register
     */
    public void registerConverter(QueryBuilderProtoConverter converter) {
        delegate.registerConverter(converter);
    }
}
