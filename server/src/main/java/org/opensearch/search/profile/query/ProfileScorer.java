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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.Timer;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link Scorer} wrapper that will compute how much time is spent on moving
 * the iterator, confirming matches and computing scores.
 *
 * @opensearch.internal
 */
final class ProfileScorer extends Scorer {

    private final Scorer scorer;

    private final Timer scoreTimer, nextDocTimer, advanceTimer, matchTimer, shallowAdvanceTimer, computeMaxScoreTimer,
        setMinCompetitiveScoreTimer;

    ProfileScorer(Scorer scorer, AbstractProfileBreakdown profile) throws IOException {
        this.scorer = scorer;
        scoreTimer = profile.getTimer(QueryTimingType.SCORE);
        nextDocTimer = profile.getTimer(QueryTimingType.NEXT_DOC);
        advanceTimer = profile.getTimer(QueryTimingType.ADVANCE);
        matchTimer = profile.getTimer(QueryTimingType.MATCH);
        shallowAdvanceTimer = profile.getTimer(QueryTimingType.SHALLOW_ADVANCE);
        computeMaxScoreTimer = profile.getTimer(QueryTimingType.COMPUTE_MAX_SCORE);
        setMinCompetitiveScoreTimer = profile.getTimer(QueryTimingType.SET_MIN_COMPETITIVE_SCORE);
    }

    /**
     * Returns the wrapped scorer.
     * <p>
     * This is useful for plugin queries that extend the Scorer API with custom methods not part of the standard
     * Lucene Scorer interface. For example, neural-search's HybridQuery needs to access its HybridBulkScorer
     * to call custom methods when profiling is enabled.
     * </p>
     * <p>
     * <b>Note:</b> Calling mutation methods (like {@link #setMinCompetitiveScore(float)}) directly on the
     * wrapped scorer will bypass profiling instrumentation for those calls. For read-only access or accessing
     * custom methods not part of the standard Scorer API, this is safe and expected.
     * </p>
     *
     * @return the underlying wrapped scorer
     * @see ProfileCollector#getDelegate()
     */
    public Scorer getWrappedScorer() {
        return scorer;
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    @Override
    public float score() throws IOException {
        scoreTimer.start();
        try {
            return scorer.score();
        } finally {
            scoreTimer.stop();
        }
    }

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
        return scorer.getChildren();
    }

    @Override
    public DocIdSetIterator iterator() {
        final DocIdSetIterator in = scorer.iterator();
        return new DocIdSetIterator() {

            @Override
            public int advance(int target) throws IOException {
                advanceTimer.start();
                try {
                    return in.advance(target);
                } finally {
                    advanceTimer.stop();
                }
            }

            @Override
            public int nextDoc() throws IOException {
                nextDocTimer.start();
                try {
                    return in.nextDoc();
                } finally {
                    nextDocTimer.stop();
                }
            }

            @Override
            public int docID() {
                return in.docID();
            }

            @Override
            public long cost() {
                return in.cost();
            }
        };
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        final TwoPhaseIterator in = scorer.twoPhaseIterator();
        if (in == null) {
            return null;
        }
        final DocIdSetIterator inApproximation = in.approximation();
        final DocIdSetIterator approximation = new DocIdSetIterator() {

            @Override
            public int advance(int target) throws IOException {
                advanceTimer.start();
                try {
                    return inApproximation.advance(target);
                } finally {
                    advanceTimer.stop();
                }
            }

            @Override
            public int nextDoc() throws IOException {
                nextDocTimer.start();
                try {
                    return inApproximation.nextDoc();
                } finally {
                    nextDocTimer.stop();
                }
            }

            @Override
            public int docID() {
                return inApproximation.docID();
            }

            @Override
            public long cost() {
                return inApproximation.cost();
            }
        };
        return new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
                matchTimer.start();
                try {
                    return in.matches();
                } finally {
                    matchTimer.stop();
                }
            }

            @Override
            public float matchCost() {
                return in.matchCost();
            }
        };
    }

    @Override
    public int advanceShallow(int target) throws IOException {
        shallowAdvanceTimer.start();
        try {
            return scorer.advanceShallow(target);
        } finally {
            shallowAdvanceTimer.stop();
        }
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        computeMaxScoreTimer.start();
        try {
            return scorer.getMaxScore(upTo);
        } finally {
            computeMaxScoreTimer.stop();
        }
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
        setMinCompetitiveScoreTimer.start();
        try {
            scorer.setMinCompetitiveScore(minScore);
        } finally {
            setMinCompetitiveScoreTimer.stop();
        }
    }
}
