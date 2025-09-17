/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.SortOptions;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SortBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithStringSort() {
        SortOptions sortOptions = SortOptions.newBuilder().setString("field_name").build();
        List<SortOptions> sortProto = Collections.singletonList(sortOptions);

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto);

        assertEquals("Should return one sort builder", 1, result.size());
        assertTrue("Should be FieldSortBuilder", result.get(0) instanceof FieldSortBuilder);
        FieldSortBuilder fieldSort = (FieldSortBuilder) result.get(0);
        assertEquals("Field name should match", "field_name", fieldSort.getFieldName());
    }

    public void testFromProtoWithScoreSort() {
        SortOptions sortOptions = SortOptions.newBuilder().setSortOptionsScore(true).build();
        List<SortOptions> sortProto = Collections.singletonList(sortOptions);

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto);

        assertEquals("Should return one sort builder", 1, result.size());
        assertTrue("Should be ScoreSortBuilder", result.get(0) instanceof ScoreSortBuilder);
    }

    public void testFromProtoWithDocSort() {
        SortOptions sortOptions = SortOptions.newBuilder().setSortOptionsDoc(true).build();
        List<SortOptions> sortProto = Collections.singletonList(sortOptions);

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto);

        assertEquals("Should return one sort builder", 1, result.size());
        assertTrue("Should be FieldSortBuilder", result.get(0) instanceof FieldSortBuilder);
        FieldSortBuilder fieldSort = (FieldSortBuilder) result.get(0);
        assertEquals("Field name should be _doc", "_doc", fieldSort.getFieldName());
    }

    public void testFromProtoWithMultipleSorts() {
        SortOptions scoreSort = SortOptions.newBuilder().setSortOptionsScore(true).build();
        SortOptions fieldSort = SortOptions.newBuilder().setString("another_field").build();
        List<SortOptions> sortProto = Arrays.asList(scoreSort, fieldSort);

        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(sortProto);

        assertEquals("Should return two sort builders", 2, result.size());
        assertTrue("First should be ScoreSortBuilder", result.get(0) instanceof ScoreSortBuilder);
        assertTrue("Second should be FieldSortBuilder", result.get(1) instanceof FieldSortBuilder);
    }

    public void testFromProtoWithEmptyList() {
        List<SortBuilder<?>> result = SortBuilderProtoUtils.fromProto(Collections.emptyList());
        assertEquals("Should return empty list", 0, result.size());
    }

    public void testFromProtoWithFieldSortThrowsException() {
        SortOptions sortOptions = SortOptions.newBuilder().setFieldSort(org.opensearch.protobufs.FieldSort.newBuilder().build()).build();
        List<SortOptions> sortProto = Collections.singletonList(sortOptions);

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto)
        );
        assertEquals("Exception message should match", "Field sort not implemented yet", exception.getMessage());
    }

    public void testFromProtoWithOneOfThrowsException() {
        SortOptions sortOptions = SortOptions.newBuilder()
            .setSortOptionsOneOf(org.opensearch.protobufs.SortOptionsOneOf.newBuilder().build())
            .build();
        List<SortOptions> sortProto = Collections.singletonList(sortOptions);

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto)
        );
        assertEquals("Exception message should match", "Sort options oneof not supported yet", exception.getMessage());
    }

    public void testFromProtoWithInvalidCaseThrowsException() {
        SortOptions sortOptions = SortOptions.newBuilder().build();
        List<SortOptions> sortProto = Collections.singletonList(sortOptions);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortBuilderProtoUtils.fromProto(sortProto));
        assertTrue(
            "Exception message should mention invalid sort options",
            exception.getMessage().contains("Invalid sort options provided")
        );
    }

    public void testFieldOrScoreSortWithScore() {
        SortBuilder<?> result = SortBuilderProtoUtils.fieldOrScoreSort("score");
        assertTrue("Should return ScoreSortBuilder", result instanceof ScoreSortBuilder);
    }

    public void testFieldOrScoreSortWithFieldName() {
        SortBuilder<?> result = SortBuilderProtoUtils.fieldOrScoreSort("field_name");
        assertTrue("Should return FieldSortBuilder", result instanceof FieldSortBuilder);
        FieldSortBuilder fieldSort = (FieldSortBuilder) result;
        assertEquals("Field name should match", "field_name", fieldSort.getFieldName());
    }

    public void testFieldOrScoreSortWithEmptyString() {
        SortBuilder<?> result = SortBuilderProtoUtils.fieldOrScoreSort("");
        assertTrue("Should return FieldSortBuilder", result instanceof FieldSortBuilder);
        FieldSortBuilder fieldSort = (FieldSortBuilder) result;
        assertEquals("Field name should be empty string", "", fieldSort.getFieldName());
    }

    public void testFieldOrScoreSortWithNull() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> SortBuilderProtoUtils.fieldOrScoreSort(null));
        assertTrue("Exception should be related to null fieldName", exception.getMessage().contains("Cannot invoke"));
    }
}
