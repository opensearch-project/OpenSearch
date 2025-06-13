/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;

/**
 * Utility class for converting Protocol Buffer query representations to OpenSearch QueryBuilder objects.
 * This class provides methods to parse different types of query containers and transform them
 * into their corresponding OpenSearch QueryBuilder implementations for search operations.
 * <p>
 * Supported query types include:
 * <ul>
 *   <li>MatchAll - Returns all documents</li>
 *   <li>MatchNone - Returns no documents</li>
 *   <li>Term - Exact term matching</li>
 *   <li>Terms - Multiple terms matching</li>
 *   <li>KNN - K-nearest neighbors vector search</li>
 * </ul>
 */
public class AbstractQueryBuilderProtoUtils {

    private static QueryBuilderProtoConverterRegistry registry;

    private AbstractQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Sets the registry to use for query conversion.
     * This method should be called during plugin initialization.
     *
     * @param registry The registry to use
     */
    public static void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        AbstractQueryBuilderProtoUtils.registry = registry;
    }

    /**
     * Gets the current registry.
     * This method is primarily for testing purposes.
     *
     * @return The current registry
     */
    public static QueryBuilderProtoConverterRegistry getRegistry() {
        return registry;
    }

    /**
     * Parse a query from its Protocol Buffer representation.
     * Similar to {@link AbstractQueryBuilder#parseInnerQueryBuilder(XContentParser)}, this method
     * determines the query type from the Protocol Buffer container and delegates to the appropriate
     * specialized parser.
     *
     * @param queryContainer The Protocol Buffer query container that holds various query type options
     * @return A QueryBuilder instance configured according to the input query parameters
     * @throws UnsupportedOperationException if the query type is not supported
     */
    public static QueryBuilder parseInnerQueryBuilderProto(QueryContainer queryContainer) throws UnsupportedOperationException {
        // Validate input
        if (queryContainer == null) {
            throw new IllegalArgumentException("Query container cannot be null");
        }

        // If the registry is set, use it to convert the query
        if (registry != null) {
            return registry.fromProto(queryContainer);
        }

        throw new IllegalArgumentException("Registry must be set.");
    }
}
