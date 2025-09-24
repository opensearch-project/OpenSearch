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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * A test-specific implementation of QueryBuilderProtoConverterRegistry that starts with no converters.
 * This class is used for testing scenarios where we need a clean registry without any built-in converters.
 */
public class EmptyQueryBuilderProtoConverterRegistry implements QueryBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(EmptyQueryBuilderProtoConverterRegistry.class);
    private final QueryBuilderProtoConverterSpiRegistry delegate;

    /**
     * Creates a new empty registry with no converters.
     */
    public EmptyQueryBuilderProtoConverterRegistry() {
        this.delegate = new QueryBuilderProtoConverterSpiRegistry();
        logger.debug("Created empty registry for testing");
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        return delegate.fromProto(queryContainer);
    }

    public void registerConverter(QueryBuilderProtoConverter converter) {
        delegate.registerConverter(converter);
    }
}
