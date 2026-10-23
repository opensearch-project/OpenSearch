/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.Bits;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.ProfilingWrapper;
import org.opensearch.search.profile.Timer;

import java.io.IOException;

/**
 * Times the delegate's bulk scoring without replacing its scoring strategy or collector.
 * The bulk time includes child query execution and collector work, so it must not be
 * added to the separately reported collector time or child query times. Method-level
 * timings are still recorded for calls intercepted by {@link ProfileScorer}. Bulk scorers
 * can perform scoring and pruning internally without those callbacks, so a zero method
 * count does not imply that the corresponding work was absent. Such work is covered by
 * the bulk timing rather than an estimated per-document timing.
 *
 * @opensearch.internal
 */
final class ProfileBulkScorer extends BulkScorer implements ProfilingWrapper<BulkScorer> {

    private final BulkScorer scorer;
    private final Timer timer;

    ProfileBulkScorer(BulkScorer scorer, AbstractProfileBreakdown profile) {
        this.scorer = scorer;
        timer = profile.getTimer(QueryTimingType.BULK_SCORE);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        timer.start();
        try {
            return scorer.score(collector, acceptDocs, min, max);
        } finally {
            timer.stop();
        }
    }

    @Override
    public long cost() {
        return scorer.cost();
    }

    @Override
    public BulkScorer getDelegate() {
        return scorer;
    }
}
