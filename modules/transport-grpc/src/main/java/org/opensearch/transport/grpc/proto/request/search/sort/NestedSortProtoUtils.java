/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.NestedSortValue;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Utility class for converting NestedSortValue Protocol Buffers to OpenSearch NestedSortBuilder objects.
 * Similar to {@link NestedSortBuilder#fromXContent}, this class handles the conversion of
 * Protocol Buffer representations to properly configured NestedSortBuilder objects with
 * path, filter, max_children, and recursive nested sorting settings.
 *
 * @opensearch.internal
 */
public class NestedSortProtoUtils {

    // Registry for query conversion - injected by the gRPC plugin
    private static QueryBuilderProtoConverterRegistry REGISTRY;

    private NestedSortProtoUtils() {
        // Utility class
    }

    /**
     * Sets the registry injected by the gRPC plugin.
     * This method is called when the NestedSort converter receives the populated registry.
     *
     * @param registry The registry to use
     */
    public static void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        REGISTRY = registry;
    }

    /**
     * Gets the current registry.
     *
     * @return The current registry
     */
    public static QueryBuilderProtoConverterRegistry getRegistry() {
        return REGISTRY;
    }

    /**
     * Converts a Protocol Buffer NestedSortValue to a NestedSortBuilder.
     * Similar to {@link NestedSortBuilder#fromXContent}, this method parses the
     * Protocol Buffer representation and creates a properly configured NestedSortBuilder
     * with the appropriate path, filter, max_children, and recursive nested sorting settings.
     *
     * @param nestedSortValue The Protocol Buffer NestedSortValue to convert
     * @return A configured NestedSortBuilder
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public static NestedSortBuilder fromProto(NestedSortValue nestedSortValue) {
        if (nestedSortValue == null) {
            throw new IllegalArgumentException("NestedSortValue cannot be null");
        }

        String path = nestedSortValue.getPath();
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Path is required for nested sort");
        }

        QueryBuilder filter = null;
        int maxChildren = Integer.MAX_VALUE;
        NestedSortBuilder nestedSort = null;

        if (nestedSortValue.hasFilter()) {
            if (REGISTRY == null) {
                throw new IllegalStateException("QueryBuilderProtoConverterRegistry not set. Call setRegistry() first.");
            }
            try {
                filter = REGISTRY.fromProto(nestedSortValue.getFilter());
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to convert nested sort filter: " + e.getMessage(), e);
            }
        }

        if (nestedSortValue.hasMaxChildren()) {
            maxChildren = nestedSortValue.getMaxChildren();
        }

        if (nestedSortValue.hasNested()) {
            nestedSort = fromProto(nestedSortValue.getNested());
        }

        return new NestedSortBuilder(path).setFilter(filter).setMaxChildren(maxChildren).setNestedSort(nestedSort);
    }
}
