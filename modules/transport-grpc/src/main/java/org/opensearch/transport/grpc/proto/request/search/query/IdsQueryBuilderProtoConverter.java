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
 * Converter for Ids queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Ids query support
 * for the gRPC transport module.
 */
public class IdsQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new IdsQueryBuilderProtoConverter.
     */
    public IdsQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.IDS;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasIds()) {
            throw new IllegalArgumentException("QueryContainer does not contain an Ids query");
        }

        return IdsQueryBuilderProtoUtils.fromProto(queryContainer.getIds());
    }
}
