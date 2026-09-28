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
 * Protocol Buffer converter for RangeQuery.
 * This converter handles the transformation of Protocol Buffer RangeQuery objects
 * into OpenSearch RangeQueryBuilder instances for range search operations.
 *
 * Range queries are handled as a map where the key is the field name and the value is the RangeQuery.
 */
public class RangeQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Default constructor for RangeQueryBuilderProtoConverter.
     */
    public RangeQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.RANGE;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || queryContainer.getQueryContainerCase() != QueryContainer.QueryContainerCase.RANGE) {
            throw new IllegalArgumentException("QueryContainer must contain a RangeQuery");
        }

        return RangeQueryBuilderProtoUtils.fromProto(queryContainer.getRange());
    }
}
