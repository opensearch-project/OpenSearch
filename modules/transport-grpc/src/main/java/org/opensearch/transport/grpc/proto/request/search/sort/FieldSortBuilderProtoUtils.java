/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.FieldSort;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

import static org.opensearch.transport.grpc.proto.request.search.sort.SortBuilderProtoUtils.SCORE_NAME;

/**
 * Utility class for converting FieldSort Protocol Buffers to OpenSearch FieldSortBuilder objects.
 * Similar to {@link FieldSortBuilder#fromXContent}, this class handles the conversion of
 * Protocol Buffer representations to properly configured FieldSortBuilder objects with
 * field sorting, missing values, sort modes, numeric types, and nested sorting settings.
 */
public class FieldSortBuilderProtoUtils {
    private FieldSortBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer FieldSort to a FieldSortBuilder with complex options.
     * Similar to {@link FieldSortBuilder#fromXContent}, this method parses the
     * Protocol Buffer representation and creates a properly configured FieldSortBuilder
     * with the appropriate field name, sort order, missing values, modes, types, and nested sorting settings.
     *
     * @param fieldName The name of the field to sort by
     * @param fieldSort The Protocol Buffer FieldSort containing sorting options
     * @param registry The registry for query conversion (needed for nested sorts with filters)
     * @return A configured FieldSortBuilder
     * @throws IllegalArgumentException if fieldName is null or empty, or fieldSort is null
     */
    public static FieldSortBuilder fromProto(String fieldName, FieldSort fieldSort, QueryBuilderProtoConverterRegistry registry) {
        if (fieldName == null || fieldName.isEmpty() || fieldName.equals(SCORE_NAME)) {
            throw new IllegalArgumentException("Field name is required and cannot be '_score'. Use ScoreSort for score-based sorting.");
        }

        if (fieldSort == null) {
            throw new IllegalArgumentException("FieldSort cannot be null");
        }

        FieldSortBuilder builder = new FieldSortBuilder(fieldName);

        if (fieldSort.hasOrder()) {
            builder.order(SortOrder.fromString(ProtobufEnumUtils.convertToString(fieldSort.getOrder())));
        }

        if (fieldSort.hasMissing()) {
            Object missing = FieldValueProtoUtils.fromProto(fieldSort.getMissing(), false);
            builder.missing(missing);
        }

        if (fieldSort.hasMode()) {
            builder.sortMode(SortMode.fromString(ProtobufEnumUtils.convertToString(fieldSort.getMode())));
        }

        if (fieldSort.hasNumericType()) {
            builder.setNumericType(ProtobufEnumUtils.convertToString(fieldSort.getNumericType()));
        }

        if (fieldSort.hasUnmappedType()) {
            builder.unmappedType(ProtobufEnumUtils.convertToString(fieldSort.getUnmappedType()));
        }

        if (fieldSort.hasNested()) {
            builder.setNestedSort(NestedSortProtoUtils.fromProto(fieldSort.getNested(), registry));
        }

        return builder;
    }
}
