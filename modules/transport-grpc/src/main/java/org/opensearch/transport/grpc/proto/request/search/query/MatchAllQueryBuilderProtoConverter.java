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

/**
 * Converter for MatchAll queries.
 * This class implements the QueryBuilderProtoConverter interface to provide MatchAll query support
 * for the gRPC transport module.
 */
public class MatchAllQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new MatchAllQueryBuilderProtoConverter.
     */
    public MatchAllQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.MATCH_ALL;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasMatchAll()) {
            throw new IllegalArgumentException("QueryContainer does not contain a MatchAll query");
        }

        return MatchAllQueryBuilderProtoUtils.fromProto(queryContainer.getMatchAll());
    }
}
