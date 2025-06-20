/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;

/**
 * Converter for MatchNone queries.
 * This class implements the QueryBuilderProtoConverter interface to provide MatchNone query support
 * for the gRPC transport plugin.
 */
public class MatchNoneQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new MatchNoneQueryBuilderProtoConverter.
     */
    public MatchNoneQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public boolean canHandle(QueryContainer queryContainer) {
        return queryContainer != null && queryContainer.hasMatchNone();
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (!canHandle(queryContainer)) {
            throw new IllegalArgumentException("QueryContainer does not contain a MatchNone query");
        }

        return MatchNoneQueryBuilderProtoUtils.fromProto(queryContainer.getMatchNone());
    }
}
