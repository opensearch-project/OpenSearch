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
 * Converter for MatchNone queries.
 * This class implements the QueryBuilderProtoConverter interface to provide MatchNone query support
 * for the gRPC transport module.
 */
public class MatchNoneQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new MatchNoneQueryBuilderProtoConverter.
     */
    public MatchNoneQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.MATCH_NONE;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasMatchNone()) {
            throw new IllegalArgumentException("QueryContainer does not contain a MatchNone query");
        }

        return MatchNoneQueryBuilderProtoUtils.fromProto(queryContainer.getMatchNone());
    }
}
