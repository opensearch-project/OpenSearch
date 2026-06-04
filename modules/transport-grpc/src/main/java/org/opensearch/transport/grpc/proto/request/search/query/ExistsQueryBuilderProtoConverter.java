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
 * Converter for Exists queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Exists query support
 * for the gRPC transport module.
 */
public class ExistsQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new ExistsQueryBuilderProtoConverter.
     */
    public ExistsQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.EXISTS;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasExists()) {
            throw new IllegalArgumentException("QueryContainer does not contain an Exists query");
        }

        return ExistsQueryBuilderProtoUtils.fromProto(queryContainer.getExists());
    }
}
