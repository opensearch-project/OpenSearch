/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.Timer;

import java.io.IOException;

/**
 * Weight wrapper that will compute how much time it takes to build the
 * {@link Scorer} and then return a {@link Scorer} that is wrapped in
 * order to compute timings as well.
 *
 * @opensearch.internal
 */
public final class ProfileWeight extends Weight {

    private final Weight subQueryWeight;
    private final ContextualProfileBreakdown profile;

    public ProfileWeight(Query query, Weight subQueryWeight, ContextualProfileBreakdown profile) throws IOException {
        super(query);
        this.subQueryWeight = subQueryWeight;
        this.profile = profile;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        Timer timer = profile.context(context).getTimer(QueryTimingType.BUILD_SCORER);
        timer.start();
        final ScorerSupplier subQueryScorerSupplier;
        try {
            subQueryScorerSupplier = subQueryWeight.scorerSupplier(context);
        } finally {
            timer.stop();
        }
        if (subQueryScorerSupplier == null) {
            return null;
        }

        return new ScorerSupplier() {

            @Override
            public Scorer get(long loadCost) throws IOException {
                timer.start();
                try {
                    return new ProfileScorer(subQueryScorerSupplier.get(loadCost), profile.context(context));
                } finally {
                    timer.stop();
                }
            }

            @Override
            public long cost() {
                timer.start();
                try {
                    return subQueryScorerSupplier.cost();
                } finally {
                    timer.stop();
                }
            }
        };
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return subQueryWeight.explain(context, doc);
    }

    @Override
    public int count(LeafReaderContext context) throws IOException {
        return subQueryWeight.count(context);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }

    public void associateCollectorToLeaves(LeafReaderContext leaf, Collector collector) {
        profile.associateCollectorToLeaves(collector, leaf);
    }
}
