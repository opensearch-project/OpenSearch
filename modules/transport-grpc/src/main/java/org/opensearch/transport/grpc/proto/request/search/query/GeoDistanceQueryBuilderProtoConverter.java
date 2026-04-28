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
 * Converter for GeoDistance queries.
 * This class implements the QueryBuilderProtoConverter interface to provide GeoDistance query support
 * for the gRPC transport module.
 */
public class GeoDistanceQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Constructs a new GeoDistanceQueryBuilderProtoConverter.
     */
    public GeoDistanceQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.GEO_DISTANCE;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || queryContainer.getQueryContainerCase() != QueryContainer.QueryContainerCase.GEO_DISTANCE) {
            throw new IllegalArgumentException("QueryContainer does not contain a GeoDistance query");
        }
        return GeoDistanceQueryBuilderProtoUtils.fromProto(queryContainer.getGeoDistance());
    }
}
