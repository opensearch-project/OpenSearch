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
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Converter for NestedQuery protobuf messages to OpenSearch QueryBuilder objects.
 * Handles the conversion of nested query protobuf messages to OpenSearch NestedQueryBuilder.
 */
public class NestedQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Default constructor for NestedQueryBuilderProtoConverter.
     */
    public NestedQueryBuilderProtoConverter() {}

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        this.registry = registry;
        // The utility class no longer stores the registry statically, it's passed directly to fromProto
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.NESTED;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || queryContainer.getQueryContainerCase() != QueryContainer.QueryContainerCase.NESTED) {
            throw new IllegalArgumentException("QueryContainer must contain a NestedQuery");
        }
        return NestedQueryBuilderProtoUtils.fromProto(queryContainer.getNested(), registry);
    }
}
