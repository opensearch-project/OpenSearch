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
 * Interface for a registry that can convert protobuf queries to OpenSearch QueryBuilder objects.
 * This interface provides a clean abstraction for plugins that need to convert nested queries
 * without exposing the internal implementation details of the registry.
 */
public interface QueryBuilderProtoConverterRegistry {

    /**
     * Converts a protobuf query container to an OpenSearch QueryBuilder.
     * This method handles the lookup and delegation to the appropriate converter.
     *
     * @param queryContainer The protobuf query container to convert
     * @return The corresponding OpenSearch QueryBuilder
     * @throws IllegalArgumentException if no converter can handle the query type
     */
    QueryBuilder fromProto(QueryContainer queryContainer);
}
