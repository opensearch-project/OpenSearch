/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.sort;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.FieldWithOrderMap;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting SearchSourceBuilder Protocol Buffers to objects
 *
 */
public class SortBuilderProtoUtils {

    private SortBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link SortBuilder#fromXContent(XContentParser)}
     * @param sortProto
     */
    public static List<SortBuilder<?>> fromProto(List<SortCombinations> sortProto) {
        List<SortBuilder<?>> sortFields = new ArrayList<>(2);

        for (SortCombinations sortCombinations : sortProto) {
            switch (sortCombinations.getSortCombinationsCase()) {
                case STRING_VALUE:
                    String name = sortCombinations.getStringValue();
                    sortFields.add(fieldOrScoreSort(name));
                    break;
                case FIELD_WITH_ORDER_MAP:
                    FieldWithOrderMap fieldWithOrderMap = sortCombinations.getFieldWithOrderMap();
                    FieldSortBuilderProtoUtils.fromProto(sortFields, fieldWithOrderMap);
                    break;
                case SORT_OPTIONS:
                    throw new UnsupportedOperationException("sort options not supported yet");
                /*
                SortOptions sortOptions = sortCombinations.getSortOptions();
                String fieldName;
                SortOrder order;
                switch(sortOptions.getSortOptionsCase()) {
                    case SCORE:
                        fieldName = ScoreSortBuilder.NAME;
                        order = SortOrderProtoUtils.fromProto(sortOptions.getScore().getOrder());
                        // TODO add other fields from ScoreSortBuilder
                        break;
                    case DOC:
                        fieldName = FieldSortBuilder.DOC_FIELD_NAME;
                        order = SortOrderProtoUtils.fromProto(sortOptions.getDoc().getOrder());
                        // TODO add other fields from FieldSortBuilder
                        break;
                    case GEO_DISTANCE:
                        fieldName = GeoDistanceAggregationBuilder.NAME;
                        order = SortOrderProtoUtils.fromProto(sortOptions.getGeoDistance().getOrder());
                        // TODO add other fields from GeoDistanceBuilder
                        break;
                    case SCRIPT:
                        fieldName = ScriptSortBuilder.NAME;
                        order = SortOrderProtoUtils.fromProto(sortOptions.getScript().getOrder());
                        // TODO add other fields from ScriptSortBuilder
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid sort options provided: "+ sortCombinations.getSortOptions().getSortOptionsCase());
                }
                // TODO add other fields from ScoreSortBuilder, FieldSortBuilder, GeoDistanceBuilder, ScriptSortBuilder too
                sortFields.add(fieldOrScoreSort(fieldName).order(order));
                break;
                */
                default:
                    throw new IllegalArgumentException("Invalid sort combinations provided: " + sortCombinations.getSortCombinationsCase());
            }
        }
        return sortFields;
    }

    /**
     * Similar to {@link SortBuilder#fieldOrScoreSort(String)}
     * @param fieldName
     * @return
     */
    public static SortBuilder<?> fieldOrScoreSort(String fieldName) {
        if (fieldName.equals("score")) {
            return new ScoreSortBuilder();
        } else {
            return new FieldSortBuilder(fieldName);
        }
    }
}
