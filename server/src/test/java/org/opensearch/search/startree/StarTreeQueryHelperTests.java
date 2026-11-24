/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.util.FixedBitSet;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarTreeQueryHelperTests extends OpenSearchTestCase {

    public void testCanPrecomputeWithStarTree() {
        SearchContext context = mock(SearchContext.class);

        when(context.shouldUseConcurrentSearch()).thenReturn(false);
        when(context.shouldUseIntraSegmentSearch()).thenReturn(false);
        assertTrue(StarTreeQueryHelper.canPrecomputeWithStarTree(context));

        when(context.shouldUseConcurrentSearch()).thenReturn(true);
        when(context.shouldUseIntraSegmentSearch()).thenReturn(false);
        assertFalse(StarTreeQueryHelper.canPrecomputeWithStarTree(context));

        when(context.shouldUseConcurrentSearch()).thenReturn(false);
        when(context.shouldUseIntraSegmentSearch()).thenReturn(true);
        assertFalse(StarTreeQueryHelper.canPrecomputeWithStarTree(context));

        when(context.shouldUseConcurrentSearch()).thenReturn(true);
        when(context.shouldUseIntraSegmentSearch()).thenReturn(true);
        assertFalse(StarTreeQueryHelper.canPrecomputeWithStarTree(context));
    }

    public void testPrecomputeLeafUsingStarTreeGatedOnConcurrentSearch() throws IOException {
        SearchContext context = mock(SearchContext.class);
        when(context.shouldUseConcurrentSearch()).thenReturn(true);
        when(context.shouldUseIntraSegmentSearch()).thenReturn(false);

        AtomicBoolean finalConsumerCalled = new AtomicBoolean(false);
        boolean precomputed = StarTreeQueryHelper.precomputeLeafUsingStarTree(
            context,
            null,
            null,
            null,
            "sum",
            v -> fail("valueConsumer should not be invoked when concurrent search is enabled"),
            () -> finalConsumerCalled.set(true)
        );
        assertFalse(precomputed);
        assertFalse(finalConsumerCalled.get());
    }

    public void testPreComputeBucketsWithStarTreeGatedOnConcurrentSearch() throws IOException {
        SearchContext context = mock(SearchContext.class);
        FixedBitSet matchingDocs = new FixedBitSet(4);
        matchingDocs.set(0);
        matchingDocs.set(2);
        AtomicInteger collectedEntries = new AtomicInteger();

        StarTreeBucketCollector collector = new StarTreeBucketCollector(null, matchingDocs) {
            @Override
            public void collectStarTreeEntry(int starTreeEntry, long bucket) {
                collectedEntries.incrementAndGet();
            }
        };

        when(context.shouldUseConcurrentSearch()).thenReturn(true);
        when(context.shouldUseIntraSegmentSearch()).thenReturn(false);
        assertFalse(StarTreeQueryHelper.preComputeBucketsWithStarTree(context, collector));
        assertEquals(0, collectedEntries.get());

        when(context.shouldUseConcurrentSearch()).thenReturn(false);
        when(context.shouldUseIntraSegmentSearch()).thenReturn(false);
        assertTrue(StarTreeQueryHelper.preComputeBucketsWithStarTree(context, collector));
        assertEquals(2, collectedEntries.get());
    }
}
