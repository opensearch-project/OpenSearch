/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.opensearch.search.aggregations;

import org.apache.lucene.search.Scorable;
import org.opensearch.common.lucene.ScorerAware;

import java.io.IOException;

/**
 * A {@link LeafBucketCollector} that delegates all calls to the sub leaf
 * aggregator and sets the scorer on its source of values if it implements
 * {@link ScorerAware}.
 *
 * @opensearch.internal
 */
public class StarTreeLeafBucketCollectorBase extends StarTreeBucketCollector {
    private final LeafBucketCollector sub;
    private final ScorerAware values;

    /**
     * @param sub    The leaf collector for sub aggregations.
     * @param values The values. {@link ScorerAware#setScorer} will be called automatically on them if they implement {@link ScorerAware}.
     */
    public StarTreeLeafBucketCollectorBase(LeafBucketCollector sub, Object values) {
        this.sub = sub;
        if (values instanceof ScorerAware) {
            this.values = (ScorerAware) values;
        } else {
            this.values = null;
        }
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        sub.setScorer(s);
        if (values != null) {
            values.setScorer(s);
        }
    }

    @Override
    public void collect(int doc, long bucket) throws IOException {
        sub.collect(doc, bucket);
    }

    @Override
    public void collectStarEntry(int starTreeEntry, long bucket) throws IOException {}
}
