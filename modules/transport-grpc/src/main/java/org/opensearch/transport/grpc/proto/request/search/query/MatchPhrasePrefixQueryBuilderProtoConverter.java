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
 * Converter for handling MatchPhrasePrefixQuery Protocol Buffer messages.
 * This class converts a QueryContainer containing a match_phrase_prefix query from Protocol Buffer format
 * into an OpenSearch QueryBuilder object for search operations.
 */
public class MatchPhrasePrefixQueryBuilderProtoConverter implements QueryBuilderProtoConverter {
    /**
     * Constructs a new MatchPhrasePrefixQueryBuilderProtoConverter.
     */
    public MatchPhrasePrefixQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.MATCH_PHRASE_PREFIX;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasMatchPhrasePrefix()) {
            throw new IllegalArgumentException("QueryContainer does not contain a MatchPhrasePrefix query");
        }

        return MatchPhrasePrefixQueryBuilderProtoUtils.fromProto(queryContainer.getMatchPhrasePrefix());
    }
}
