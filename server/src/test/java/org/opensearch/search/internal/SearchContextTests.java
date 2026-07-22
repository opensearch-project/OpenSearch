/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.opensearch.common.lease.Releasable;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

/**
 * Unit tests for the thread-scoped partition doc-id range on {@link SearchContext} that carries a partition's
 * {@code [minDocId, maxDocId)} to segment-level aggregation optimizations under intra-segment search. The
 * contract that matters: the range is visible only within the {@link Releasable} scope, is restored (not
 * leaked) on close even when scopes nest, and defaults to {@code null} (whole segment) otherwise.
 */
public class SearchContextTests extends OpenSearchTestCase {

    public void testPartitionDocIdRangeDefaultsToNull() {
        SearchContext context = new TestSearchContext(null);
        assertNull(context.getPartitionDocIdRange());
    }

    public void testPartitionDocIdRangeVisibleWithinScope() {
        SearchContext context = new TestSearchContext(null);
        try (Releasable ignore = context.withPartitionDocIdRange(10, 20)) {
            int[] range = context.getPartitionDocIdRange();
            assertNotNull(range);
            assertArrayEquals(new int[] { 10, 20 }, range);
        }
    }

    public void testPartitionDocIdRangeClearedAfterScope() {
        SearchContext context = new TestSearchContext(null);
        try (Releasable ignore = context.withPartitionDocIdRange(10, 20)) {
            assertNotNull(context.getPartitionDocIdRange());
        }
        // must be fully removed (not left as a stale value) once the scope closes
        assertNull(context.getPartitionDocIdRange());
    }

    public void testPartitionDocIdRangeRestoredForNestedScopes() {
        SearchContext context = new TestSearchContext(null);
        try (Releasable outer = context.withPartitionDocIdRange(0, 100)) {
            assertArrayEquals(new int[] { 0, 100 }, context.getPartitionDocIdRange());
            try (Releasable inner = context.withPartitionDocIdRange(40, 60)) {
                assertArrayEquals(new int[] { 40, 60 }, context.getPartitionDocIdRange());
            }
            // closing the inner scope restores the outer range, not null
            assertArrayEquals(new int[] { 0, 100 }, context.getPartitionDocIdRange());
        }
        assertNull(context.getPartitionDocIdRange());
    }
}
