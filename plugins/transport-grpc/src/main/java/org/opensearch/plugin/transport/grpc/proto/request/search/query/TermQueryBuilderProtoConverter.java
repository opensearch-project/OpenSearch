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
 * Converter for Term queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Term query support
 * for the gRPC transport plugin.
 */
public class TermQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new TermQueryBuilderProtoConverter.
     */
    public TermQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public boolean canHandle(QueryContainer queryContainer) {
        return queryContainer != null && queryContainer.getTermCount() > 0;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (!canHandle(queryContainer)) {
            throw new IllegalArgumentException("QueryContainer does not contain a Term query");
        }

        return TermQueryBuilderProtoUtils.fromProto(queryContainer.getTermMap());
    }
}
