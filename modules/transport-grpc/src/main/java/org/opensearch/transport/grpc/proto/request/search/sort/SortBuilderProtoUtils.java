/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting SortBuilder Protocol Buffers to OpenSearch objects.
 * Similar to {@link SortBuilder#fromXContent}, this class handles the conversion of
 * Protocol Buffer representations to properly configured SortBuilder implementations
 * by delegating to specific sort utilities for different sort types.
 */
public class SortBuilderProtoUtils {

    /** The field name used for score-based sorting. */
    public static final String SCORE_NAME = "_score";

    private SortBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a list of Protocol Buffer SortCombinations to a list of OpenSearch SortBuilder objects.
     * Similar to {@link SortBuilder#fromXContent(XContentParser)}, this method parses the
     * Protocol Buffer representations and creates properly configured SortBuilder instances
     * by delegating to specific sort utilities based on the SortCombinations structure.
     *
     * @param sortProto The list of Protocol Buffer SortCombinations to convert
     * @param registry The registry for query conversion (needed for nested sorts with filters)
     * @return A list of configured SortBuilder instances
     * @throws IllegalArgumentException if invalid sort combinations are provided
     */
    public static List<SortBuilder<?>> fromProto(List<SortCombinations> sortProto, QueryBuilderProtoConverterRegistry registry) {
        List<SortBuilder<?>> sortFields = new ArrayList<>(sortProto.size());

        for (SortCombinations sortCombination : sortProto) {
            SortBuilder<?> sortBuilder = null;

            switch (sortCombination.getSortCombinationsCase()) {
                case FIELD:
                    String fieldName = sortCombination.getField();
                    sortBuilder = fieldOrScoreSort(fieldName);
                    break;

                case FIELD_WITH_DIRECTION:
                    sortBuilder = fromSortOrderMap(sortCombination.getFieldWithDirection());
                    break;

                case FIELD_WITH_ORDER:
                    sortBuilder = fromFieldSortMap(sortCombination.getFieldWithOrder(), registry);
                    break;

                case OPTIONS:
                    sortBuilder = fromSortOptions(sortCombination.getOptions(), registry);
                    break;

                case SORTCOMBINATIONS_NOT_SET:
                    continue;

                default:
                    throw new IllegalArgumentException("Unsupported sort combination case: " + sortCombination.getSortCombinationsCase());
            }

            if (sortBuilder != null) {
                sortFields.add(sortBuilder);
            }
        }
        return sortFields;
    }

    /**
     * Creates either a ScoreSortBuilder or FieldSortBuilder based on the field name.
     * Similar to {@link SortBuilder#fieldOrScoreSort(String)}, this method returns
     * a ScoreSortBuilder if the field name is "_score", otherwise it returns a
     * FieldSortBuilder with the specified field name.
     *
     * @param fieldName The name of the field to sort by, or "_score" for score-based sorting
     * @return A SortBuilder instance (either ScoreSortBuilder or FieldSortBuilder)
     */
    public static SortBuilder<?> fieldOrScoreSort(String fieldName) {
        if (SCORE_NAME.equals(fieldName)) {
            return new ScoreSortBuilder();
        } else {
            return new FieldSortBuilder(fieldName);
        }
    }

    /**
     * Converts SortOrderMap (field with direction) to SortBuilder.
     */
    private static SortBuilder<?> fromSortOrderMap(org.opensearch.protobufs.SortOrderMap sortOrderMap) {
        if (sortOrderMap.getSortOrderMapMap().isEmpty() || sortOrderMap.getSortOrderMapMap().size() > 1) {
            throw new IllegalArgumentException("SortOrderMap cannot be empty or contain multiple entries");
        }

        String fieldName = sortOrderMap.getSortOrderMapMap().keySet().iterator().next();
        org.opensearch.protobufs.SortOrder direction = sortOrderMap.getSortOrderMapMap().get(fieldName);

        SortOrder order = parseSortOrder(direction);
        SortBuilder<?> sortBuilder = fieldOrScoreSort(fieldName).order(order);

        return sortBuilder;
    }

    /**
     * Converts FieldSortMap (field with complex options) to SortBuilder.
     */
    private static SortBuilder<?> fromFieldSortMap(
        org.opensearch.protobufs.FieldSortMap fieldSortMap,
        QueryBuilderProtoConverterRegistry registry
    ) {
        if (fieldSortMap.getFieldSortMapMap().isEmpty() || fieldSortMap.getFieldSortMapMap().size() > 1) {
            throw new IllegalArgumentException("FieldSortMap cannot be empty or contain multiple entries");
        }

        String fieldName = fieldSortMap.getFieldSortMapMap().keySet().iterator().next();
        org.opensearch.protobufs.FieldSort fieldSort = fieldSortMap.getFieldSortMapMap().get(fieldName);

        return FieldSortBuilderProtoUtils.fromProto(fieldName, fieldSort, registry);
    }

    /**
     * Converts SortOptions to SortBuilder.
     */
    private static SortBuilder<?> fromSortOptions(
        org.opensearch.protobufs.SortOptions sortOptions,
        QueryBuilderProtoConverterRegistry registry
    ) {
        if (sortOptions.hasXScore()) {
            return ScoreSortProtoUtils.fromProto(sortOptions.getXScore());
        } else if (sortOptions.hasXGeoDistance()) {
            return GeoDistanceSortProtoUtils.fromProto(sortOptions.getXGeoDistance(), registry);
        } else if (sortOptions.hasXScript()) {
            return ScriptSortProtoUtils.fromProto(sortOptions.getXScript(), registry);
        } else {
            throw new IllegalArgumentException("Unknown sort options type");
        }
    }

    /**
     * Converts Protocol Buffer SortOrder to OpenSearch SortOrder.
     */
    private static SortOrder parseSortOrder(org.opensearch.protobufs.SortOrder protobufOrder) {
        String orderString = ProtobufEnumUtils.convertToString(protobufOrder);
        return SortOrder.fromString(orderString);
    }
}
