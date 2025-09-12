/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.FieldWithOrderMap;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for converting SortBuilder Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of sort
 * specifications into their corresponding OpenSearch SortBuilder implementations for
 * search result sorting.
 */
public class SortBuilderProtoUtils {

    private SortBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a list of Protocol Buffer SortCombinations to a list of OpenSearch SortBuilder objects.
     * Similar to {@link SortBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates properly configured
     * SortBuilder instances with the appropriate settings.
     *
     * @param sortProto The list of Protocol Buffer SortCombinations to convert
     * @return A list of configured SortBuilder instances
     * @throws IllegalArgumentException if invalid sort combinations are provided
     * @throws UnsupportedOperationException if sort options are not yet supported
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
     * Creates either a ScoreSortBuilder or FieldSortBuilder based on the field name.
     * Similar to {@link SortBuilder#fieldOrScoreSort(String)}, this method returns
     * a ScoreSortBuilder if the field name is "score", otherwise it returns a
     * FieldSortBuilder with the specified field name.
     *
     * @param fieldName The name of the field to sort by, or "score" for score-based sorting
     * @return A SortBuilder instance (either ScoreSortBuilder or FieldSortBuilder)
     */
    public static SortBuilder<?> fieldOrScoreSort(String fieldName) {
        if (fieldName.equals("score")) {
            return new ScoreSortBuilder();
        } else {
            return new FieldSortBuilder(fieldName);
        }
    }
}
