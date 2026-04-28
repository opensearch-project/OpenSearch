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

/**
 * Converter for MultiMatch queries.
 * This class implements the QueryBuilderProtoConverter interface to provide MultiMatch query support
 * for the gRPC transport module.
 */
public class MultiMatchQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new MultiMatchQueryBuilderProtoConverter.
     */
    public MultiMatchQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.MULTI_MATCH;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasMultiMatch()) {
            throw new IllegalArgumentException("QueryContainer does not contain a MultiMatch query");
        }

        return MultiMatchQueryBuilderProtoUtils.fromProto(queryContainer.getMultiMatch());
    }
}
