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
 * Interface for converting protobuf query messages to OpenSearch QueryBuilder objects.
 * External plugins can implement this interface to provide their own query types.
 */
public interface QueryBuilderProtoConverter {

    /**
     * Returns the QueryContainerCase this converter handles.
     *
     * @return The QueryContainerCase
     */
    QueryContainer.QueryContainerCase getHandledQueryCase();

    /**
     * Converts a protobuf query container to an OpenSearch QueryBuilder.
     *
     * @param queryContainer The protobuf query container
     * @return The corresponding OpenSearch QueryBuilder
     * @throws IllegalArgumentException if the query cannot be converted
     */
    QueryBuilder fromProto(QueryContainer queryContainer);
}
