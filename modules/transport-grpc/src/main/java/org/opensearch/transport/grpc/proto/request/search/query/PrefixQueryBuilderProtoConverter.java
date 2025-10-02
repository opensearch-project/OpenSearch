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
 * Converter for Prefix queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Prefix query support
 * for the gRPC transport module.
 */
public class PrefixQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new PrefixQueryBuilderProtoConverter.
     */
    public PrefixQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.PREFIX;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || !queryContainer.hasPrefix()) {
            throw new IllegalArgumentException("QueryContainer does not contain a Prefix query");
        }

        return PrefixQueryBuilderProtoUtils.fromProto(queryContainer.getPrefix());
    }
}
