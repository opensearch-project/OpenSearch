/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;

/**
 * Protocol Buffer converter for GeoBoundingBoxQuery.
 * This converter handles the transformation of Protocol Buffer GeoBoundingBoxQuery objects
 * into OpenSearch GeoBoundingBoxQueryBuilder instances for geo bounding box search operations.
 *
 */
public class GeoBoundingBoxQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    /**
     * Default constructor for GeoBoundingBoxQueryBuilderProtoConverter.
     */
    public GeoBoundingBoxQueryBuilderProtoConverter() {
        // Default constructor
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.GEO_BOUNDING_BOX;
    }

    @Override
    public GeoBoundingBoxQueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || queryContainer.getQueryContainerCase() != QueryContainer.QueryContainerCase.GEO_BOUNDING_BOX) {
            throw new IllegalArgumentException("QueryContainer must contain a GeoBoundingBoxQuery");
        }

        return GeoBoundingBoxQueryBuilderProtoUtils.fromProto(queryContainer.getGeoBoundingBox());
    }
}
