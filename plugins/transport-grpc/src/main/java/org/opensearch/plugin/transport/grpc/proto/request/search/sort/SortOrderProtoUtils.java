/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.GeoDistanceSort;
import org.opensearch.protobufs.ScoreSort;
import org.opensearch.protobufs.ScriptSort;
import org.opensearch.search.sort.SortOrder;

/**
 * Utility class for converting SortOrder Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of sort orders
 * from various sort types into their corresponding OpenSearch SortOrder enums for search operations.
 */
public class SortOrderProtoUtils {

    private SortOrderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer ScoreSort.SortOrder to an OpenSearch SortOrder.
     * Similar to {@link SortOrder#fromString(String)}, this method maps the Protocol Buffer
     * sort order enum values to their corresponding OpenSearch SortOrder values.
     *
     * @param sortOrder The Protocol Buffer ScoreSort.SortOrder to convert
     * @return The corresponding OpenSearch SortOrder
     * @throws IllegalArgumentException if the sort order is unspecified or invalid
     */
    public static SortOrder fromProto(ScoreSort.SortOrder sortOrder) {
        switch (sortOrder) {
            case SORT_ORDER_ASC:
                return SortOrder.ASC;
            case SORT_ORDER_DESC:
                return SortOrder.DESC;
            case SORT_ORDER_UNSPECIFIED:
            default:
                throw new IllegalArgumentException("Must provide oneof sort combinations");
        }
    }

    /**
     * Converts a Protocol Buffer GeoDistanceSort.SortOrder to an OpenSearch SortOrder.
     * Similar to {@link SortOrder#fromString(String)}, this method maps the Protocol Buffer
     * sort order enum values to their corresponding OpenSearch SortOrder values.
     *
     * @param sortOrder The Protocol Buffer GeoDistanceSort.SortOrder to convert
     * @return The corresponding OpenSearch SortOrder
     * @throws IllegalArgumentException if the sort order is unspecified or invalid
     */
    public static SortOrder fromProto(GeoDistanceSort.SortOrder sortOrder) {
        switch (sortOrder) {
            case SORT_ORDER_ASC:
                return SortOrder.ASC;
            case SORT_ORDER_DESC:
                return SortOrder.DESC;
            case SORT_ORDER_UNSPECIFIED:
            default:
                throw new IllegalArgumentException("Must provide oneof sort combinations");
        }
    }

    /**
     * Converts a Protocol Buffer ScriptSort.SortOrder to an OpenSearch SortOrder.
     * Similar to {@link SortOrder#fromString(String)}, this method maps the Protocol Buffer
     * sort order enum values to their corresponding OpenSearch SortOrder values.
     *
     * @param sortOrder The Protocol Buffer ScriptSort.SortOrder to convert
     * @return The corresponding OpenSearch SortOrder
     * @throws IllegalArgumentException if the sort order is unspecified or invalid
     */
    public static SortOrder fromProto(ScriptSort.SortOrder sortOrder) {
        switch (sortOrder) {
            case SORT_ORDER_ASC:
                return SortOrder.ASC;
            case SORT_ORDER_DESC:
                return SortOrder.DESC;
            case SORT_ORDER_UNSPECIFIED:
            default:
                throw new IllegalArgumentException("Must provide oneof sort combinations");
        }
    }

}
