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
class NestedSortProtoUtils {

    private NestedSortProtoUtils() {
        // Utility class
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
    static NestedSortBuilder fromProto(NestedSortValue nestedSortValue, QueryBuilderProtoConverterRegistry registry) {
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
            if (registry == null) {
                throw new IllegalStateException("QueryBuilderProtoConverterRegistry cannot be null.");
            }
            try {
                filter = registry.fromProto(nestedSortValue.getFilter());
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to convert nested sort filter: " + e.getMessage(), e);
            }
        }

        if (nestedSortValue.hasMaxChildren()) {
            maxChildren = nestedSortValue.getMaxChildren();
        }

        if (nestedSortValue.hasNested()) {
            nestedSort = fromProto(nestedSortValue.getNested(), registry);
        }

        return new NestedSortBuilder(path).setFilter(filter).setMaxChildren(maxChildren).setNestedSort(nestedSort);
    }
}
