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
 * Converter for Terms queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Terms query support
 * for the gRPC transport module.
 */
public class TermsQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new TermsQueryBuilderProtoConverter.
     */
    public TermsQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.TERMS;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasTerms()) {
            throw new IllegalArgumentException("QueryContainer does not contain a Terms query");
        }

        return TermsQueryBuilderProtoUtils.fromProto(queryContainer.getTerms());
    }
}
