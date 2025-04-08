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
 * Utility class for converting SearchSourceBuilder Protocol Buffers to objects
 *
 */
public class SortOrderProtoUtils {

    private SortOrderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link SortOrder#fromString(String)}
     * @param sortOrder
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
     * Similar to {@link SortOrder#fromString(String)}
     * @param sortOrder
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
     * Similar to {@link SortOrder#fromString(String)}
     * @param sortOrder
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
