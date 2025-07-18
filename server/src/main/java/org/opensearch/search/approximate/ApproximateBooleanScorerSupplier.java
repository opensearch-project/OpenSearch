/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;

import java.io.IOException;

public class ApproximateBooleanScorerSupplier extends ScorerSupplier {

    /**
     * Get the {@link Scorer}. This may not return {@code null} and must be called at most once.
     *
     * @param leadCost Cost of the scorer that will be used in order to lead iteration. This can be
     *                 interpreted as an upper bound of the number of times that {@link DocIdSetIterator#nextDoc},
     *                 {@link DocIdSetIterator#advance} and TwoPhaseIterator#matches will be called. Under
     *                 doubt, pass {@link Long#MAX_VALUE}, which will produce a {@link Scorer} that has good
     *                 iteration capabilities.
     */
    @Override
    public Scorer get(long leadCost) throws IOException {
        return null;
    }

    /**
     * Optional method: Get a scorer that is optimized for bulk-scoring. The default implementation
     * iterates matches from the {@link Scorer} but some queries can have more efficient approaches
     * for matching all hits.
     */
    public BulkScorer bulkScorer() throws IOException {
        return null;
    }

    /**
     * Get an estimate of the {@link Scorer} that would be returned by {@link #get}. This may be a
     * costly operation, so it should only be called if necessary.
     *
     * @see DocIdSetIterator#cost
     */
    @Override
    public long cost() {
        return 0;
    }
}
