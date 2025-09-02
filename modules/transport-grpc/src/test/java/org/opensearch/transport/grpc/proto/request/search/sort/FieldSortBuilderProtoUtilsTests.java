/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.FieldWithOrderMap;
import org.opensearch.protobufs.ScoreSort;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class FieldSortBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithEmptyMap() {
        // Create an empty FieldWithOrderMap
        FieldWithOrderMap fieldWithOrderMap = FieldWithOrderMap.newBuilder().build();

        // Create a list to populate
        List<SortBuilder<?>> sortBuilders = new ArrayList<>();

        // Call the method under test
        FieldSortBuilderProtoUtils.fromProto(sortBuilders, fieldWithOrderMap);

        // Verify the result
        assertTrue("SortBuilders list should be empty", sortBuilders.isEmpty());
    }

    public void testFromProtoWithSingleField() {
        // Create a FieldWithOrderMap with a single field
        FieldWithOrderMap.Builder builder = FieldWithOrderMap.newBuilder();
        builder.putFieldWithOrderMap("field1", ScoreSort.newBuilder().setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_ASC).build());
        FieldWithOrderMap fieldWithOrderMap = builder.build();

        // Create a list to populate
        List<SortBuilder<?>> sortBuilders = new ArrayList<>();

        // Call the method under test
        FieldSortBuilderProtoUtils.fromProto(sortBuilders, fieldWithOrderMap);

        // Verify the result
        assertEquals("SortBuilders list should have 1 element", 1, sortBuilders.size());
        assertTrue("SortBuilder should be a FieldSortBuilder", sortBuilders.get(0) instanceof FieldSortBuilder);
        FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilders.get(0);
        assertEquals("Field name should match", "field1", fieldSortBuilder.getFieldName());
        assertEquals("Sort order should be ASC", SortOrder.ASC, fieldSortBuilder.order());
    }

    public void testFromProtoWithMultipleFields() {
        // Create a FieldWithOrderMap with multiple fields
        FieldWithOrderMap.Builder builder = FieldWithOrderMap.newBuilder();
        builder.putFieldWithOrderMap("field1", ScoreSort.newBuilder().setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_ASC).build());
        builder.putFieldWithOrderMap("field2", ScoreSort.newBuilder().setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_DESC).build());
        FieldWithOrderMap fieldWithOrderMap = builder.build();

        // Create a list to populate
        List<SortBuilder<?>> sortBuilders = new ArrayList<>();

        // Call the method under test
        FieldSortBuilderProtoUtils.fromProto(sortBuilders, fieldWithOrderMap);

        // Verify the result
        assertEquals("SortBuilders list should have 2 elements", 2, sortBuilders.size());

        // Since the order of entries in a map is not guaranteed, we need to check both fields
        boolean foundField1 = false;
        boolean foundField2 = false;

        for (SortBuilder<?> sortBuilder : sortBuilders) {
            assertTrue("SortBuilder should be a FieldSortBuilder", sortBuilder instanceof FieldSortBuilder);
            FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilder;

            if (fieldSortBuilder.getFieldName().equals("field1")) {
                foundField1 = true;
                assertEquals("Sort order for field1 should be ASC", SortOrder.ASC, fieldSortBuilder.order());
            } else if (fieldSortBuilder.getFieldName().equals("field2")) {
                foundField2 = true;
                assertEquals("Sort order for field2 should be DESC", SortOrder.DESC, fieldSortBuilder.order());
            }
        }

        assertTrue("Should have found field1", foundField1);
        assertTrue("Should have found field2", foundField2);
    }

    public void testFromProtoWithScoreField() {
        // Create a FieldWithOrderMap with the special "score" field
        FieldWithOrderMap.Builder builder = FieldWithOrderMap.newBuilder();
        builder.putFieldWithOrderMap("score", ScoreSort.newBuilder().setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_DESC).build());
        FieldWithOrderMap fieldWithOrderMap = builder.build();

        // Create a list to populate
        List<SortBuilder<?>> sortBuilders = new ArrayList<>();

        // Call the method under test
        FieldSortBuilderProtoUtils.fromProto(sortBuilders, fieldWithOrderMap);

        // Verify the result
        assertEquals("SortBuilders list should have 1 element", 1, sortBuilders.size());
        assertTrue("SortBuilder should be a ScoreSortBuilder", sortBuilders.get(0) instanceof ScoreSortBuilder);
        ScoreSortBuilder scoreSortBuilder = (ScoreSortBuilder) sortBuilders.get(0);
        assertEquals("Sort order should be DESC", SortOrder.DESC, scoreSortBuilder.order());
    }
}
