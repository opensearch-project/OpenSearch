/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.ScoreSort;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link ScoreSortProtoUtils}.
 */
public class ScoreSortProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProto_SortOrders() {
        // Test 1: Unspecified order (should default to DESC)
        ScoreSort scoreSortUnspecified = ScoreSort.newBuilder().setOrder(SortOrder.SORT_ORDER_UNSPECIFIED).build();
        ScoreSortBuilder resultUnspecified = ScoreSortProtoUtils.fromProto(scoreSortUnspecified);
        assertNotNull("Result should not be null for UNSPECIFIED order", resultUnspecified);
        assertEquals("Unspecified order should default to DESC", org.opensearch.search.sort.SortOrder.DESC, resultUnspecified.order());

        // Test 2: ASC order
        ScoreSort scoreSortAsc = ScoreSort.newBuilder().setOrder(SortOrder.SORT_ORDER_ASC).build();
        ScoreSortBuilder resultAsc = ScoreSortProtoUtils.fromProto(scoreSortAsc);
        assertNotNull("Result should not be null for ASC order", resultAsc);
        assertEquals("ASC order should be preserved", org.opensearch.search.sort.SortOrder.ASC, resultAsc.order());

        // Test 3: DESC order
        ScoreSort scoreSortDesc = ScoreSort.newBuilder().setOrder(SortOrder.SORT_ORDER_DESC).build();
        ScoreSortBuilder resultDesc = ScoreSortProtoUtils.fromProto(scoreSortDesc);
        assertNotNull("Result should not be null for DESC order", resultDesc);
        assertEquals("DESC order should be preserved", org.opensearch.search.sort.SortOrder.DESC, resultDesc.order());
    }

    public void testFromProto_NullInput() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { ScoreSortProtoUtils.fromProto(null); });
        assertEquals("ScoreSort cannot be null", exception.getMessage());
    }
}
