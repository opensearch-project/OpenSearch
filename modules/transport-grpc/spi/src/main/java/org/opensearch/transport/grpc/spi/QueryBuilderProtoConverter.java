/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.spi;

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

    /**
     * Sets the registry for converting nested queries.
     * This method is called by the gRPC plugin to inject the populated registry
     * into converters that need to handle nested query types.
     *
     * @param registry The registry containing all available converters
     */
    default void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        // By default, converters don't need a registry
        // Converters that handle nested queries should override this method
    }
}
