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

    public void testFromProto_WithUnspecifiedOrder() {
        ScoreSort scoreSort = ScoreSort.newBuilder().setOrder(SortOrder.SORT_ORDER_UNSPECIFIED).build();

        ScoreSortBuilder result = ScoreSortProtoUtils.fromProto(scoreSort);

        assertNotNull(result);
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.order());
    }

    public void testFromProto_WithAscOrder() {
        ScoreSort scoreSort = ScoreSort.newBuilder().setOrder(SortOrder.SORT_ORDER_ASC).build();

        ScoreSortBuilder result = ScoreSortProtoUtils.fromProto(scoreSort);

        assertNotNull(result);
        assertEquals(org.opensearch.search.sort.SortOrder.ASC, result.order());
    }

    public void testFromProto_WithDescOrder() {
        ScoreSort scoreSort = ScoreSort.newBuilder().setOrder(SortOrder.SORT_ORDER_DESC).build();

        ScoreSortBuilder result = ScoreSortProtoUtils.fromProto(scoreSort);

        assertNotNull(result);
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.order());
    }

    public void testFromProto_NullInput() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { ScoreSortProtoUtils.fromProto(null); });
        assertEquals("ScoreSort cannot be null", exception.getMessage());
    }
}
