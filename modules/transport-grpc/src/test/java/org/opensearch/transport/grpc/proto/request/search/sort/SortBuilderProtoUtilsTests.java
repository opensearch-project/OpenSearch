/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.SortCombinations;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

public class SortBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoThrowsUnsupportedOperation() {
        // Create an empty list of SortCombinations
        List<SortCombinations> sortProto = new ArrayList<>();

        // This should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto)
        );

        assertEquals("sort not supported yet", exception.getMessage());
    }

    public void testFromProtoWithSortCombinationsThrowsUnsupportedOperation() {
        // Create a list with a SortCombination
        List<SortCombinations> sortProto = new ArrayList<>();
        sortProto.add(SortCombinations.newBuilder().build());

        // This should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SortBuilderProtoUtils.fromProto(sortProto)
        );

        assertEquals("sort not supported yet", exception.getMessage());
    }

    public void testFieldOrScoreSortWithScoreField() {
        // Test with "score" field
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("score");

        assertTrue("Should return ScoreSortBuilder for score field", sortBuilder instanceof ScoreSortBuilder);
    }

    public void testFieldOrScoreSortWithRegularField() {
        // Test with regular field name
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("username");

        assertTrue("Should return FieldSortBuilder for regular field", sortBuilder instanceof FieldSortBuilder);
        FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilder;
        assertEquals("Field name should match", "username", fieldSortBuilder.getFieldName());
    }

    public void testFieldOrScoreSortWithEmptyField() {
        // Test with empty field name
        SortBuilder<?> sortBuilder = SortBuilderProtoUtils.fieldOrScoreSort("");

        assertTrue("Should return FieldSortBuilder for empty field", sortBuilder instanceof FieldSortBuilder);
        FieldSortBuilder fieldSortBuilder = (FieldSortBuilder) sortBuilder;
        assertEquals("Field name should be empty", "", fieldSortBuilder.getFieldName());
    }

    public void testFieldOrScoreSortWithNullField() {
        // Test with null field name - should throw NullPointerException
        NullPointerException exception = expectThrows(NullPointerException.class, () -> SortBuilderProtoUtils.fieldOrScoreSort(null));

        // Verify the exception is thrown (the method doesn't handle null gracefully)
        assertNotNull("Should throw NullPointerException for null field", exception);
    }
}
