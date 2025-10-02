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
 * Converter for ConstantScore queries.
 * This class implements the QueryBuilderProtoConverter interface to provide ConstantScore query support
 * for the gRPC transport module.
 */
public class ConstantScoreQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Default constructor for ConstantScoreQueryBuilderProtoConverter.
     */
    public ConstantScoreQueryBuilderProtoConverter() {}

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.CONSTANT_SCORE;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || queryContainer.getQueryContainerCase() != QueryContainer.QueryContainerCase.CONSTANT_SCORE) {
            throw new IllegalArgumentException("QueryContainer does not contain a ConstantScore query");
        }

        return ConstantScoreQueryBuilderProtoUtils.fromProto(queryContainer.getConstantScore(), registry);
    }
}
