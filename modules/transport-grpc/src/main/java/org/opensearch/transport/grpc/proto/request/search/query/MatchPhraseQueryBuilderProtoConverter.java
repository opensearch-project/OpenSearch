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
 * Converter for MatchPhrase queries.
 * This class implements the QueryBuilderProtoConverter interface to provide MatchPhrase query support
 * for the gRPC transport module.
 */
public class MatchPhraseQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new MatchPhraseQueryBuilderProtoConverter.
     */
    public MatchPhraseQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.MATCH_PHRASE;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasMatchPhrase()) {
            throw new IllegalArgumentException("QueryContainer does not contain a MatchPhrase query");
        }

        return MatchPhraseQueryBuilderProtoUtils.fromProto(queryContainer.getMatchPhrase());
    }
}
