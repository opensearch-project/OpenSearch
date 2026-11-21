/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.Bits;
import org.opensearch.search.profile.ProfileMetricUtil;
import org.opensearch.search.profile.Timer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class ProfileBulkScorerTests extends OpenSearchTestCase {

    public void testDelegationAndTiming() throws IOException {
        LeafCollector collector = mock(LeafCollector.class);
        Bits acceptDocs = new Bits.MatchAllBits(100);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        BulkScorer delegate = new BulkScorer() {
            @Override
            public int score(LeafCollector in, Bits docs, int min, int max) {
                assertSame(collector, in);
                assertSame(acceptDocs, docs);
                assertEquals(5, min);
                assertEquals(20, max);
                return 25;
            }

            @Override
            public long cost() {
                return 42;
            }
        };
        ProfileBulkScorer scorer = new ProfileBulkScorer(delegate, profile);
        assertSame(delegate, scorer.getDelegate());
        assertEquals(42L, scorer.cost());
        assertEquals(25, scorer.score(collector, acceptDocs, 5, 20));
        assertEquals(25, scorer.score(collector, acceptDocs, 5, 20));
        Timer timer = profile.getTimer(QueryTimingType.BULK_SCORE);
        assertEquals(2L, timer.getCount());
        assertTrue(timer.getApproximateTiming() > 0L);
        assertEquals(timer.getApproximateTiming(), profile.toNodeTime());
    }

    public void testTimerStopsOnExceptionAndEarlyTermination() throws IOException {
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        IOException failure = new IOException("test failure");
        CollectionTerminatedException terminated = new CollectionTerminatedException();
        BulkScorer delegate = new BulkScorer() {
            @Override
            public int score(LeafCollector collector, Bits docs, int min, int max) throws IOException {
                if (min == 0) {
                    throw failure;
                }
                throw terminated;
            }

            @Override
            public long cost() {
                return 1;
            }
        };
        ProfileBulkScorer scorer = new ProfileBulkScorer(delegate, profile);
        assertSame(failure, expectThrows(IOException.class, () -> scorer.score(null, null, 0, 20)));
        assertSame(terminated, expectThrows(CollectionTerminatedException.class, () -> scorer.score(null, null, 1, 20)));
        Timer timer = profile.getTimer(QueryTimingType.BULK_SCORE);
        assertEquals(2L, timer.getCount());
        assertTrue(timer.getApproximateTiming() > 0L);
    }
}
