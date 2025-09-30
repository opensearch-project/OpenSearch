/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Converter for Bool queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Bool query support
 * for the gRPC transport module.
 */
public class BoolQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Default constructor for BoolQueryBuilderProtoConverter.
     */
    public BoolQueryBuilderProtoConverter() {}

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.BOOL;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || queryContainer.getQueryContainerCase() != QueryContainer.QueryContainerCase.BOOL) {
            throw new IllegalArgumentException("QueryContainer does not contain a Bool query");
        }

        return BoolQueryBuilderProtoUtils.fromProto(queryContainer.getBool(), registry);
    }
}
