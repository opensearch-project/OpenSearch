/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.SortOptions;
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
    public static List<SortBuilder<?>> fromProto(List<SortOptions> sortProto) {
        List<SortBuilder<?>> sortFields = new ArrayList<>(2);

        for (SortOptions sortOptions : sortProto) {
            switch (sortOptions.getSortOptionsCase()) {
                case STRING:
                    String name = sortOptions.getString();
                    sortFields.add(fieldOrScoreSort(name));
                    break;
                case SORT_OPTIONS_SCORE:
                    sortFields.add(new ScoreSortBuilder());
                    break;
                case SORT_OPTIONS_DOC:
                    sortFields.add(new FieldSortBuilder("_doc"));
                    break;
                case FIELD_SORT:
                    // Handle field sort - this is more complex and would need FieldSort parsing
                    throw new UnsupportedOperationException("Field sort not implemented yet");
                case SORT_OPTIONS_ONE_OF:
                    throw new UnsupportedOperationException("Sort options oneof not supported yet");
                default:
                    throw new IllegalArgumentException("Invalid sort options provided: " + sortOptions.getSortOptionsCase());
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
