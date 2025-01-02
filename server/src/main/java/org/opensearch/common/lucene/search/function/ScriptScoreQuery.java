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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.ScoreScript.ExplanationHolder;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that uses a script to compute documents' scores.
 *
 * @opensearch.internal
 */
public class ScriptScoreQuery extends Query {
    private final Query subQuery;
    private final Script script;
    private final ScoreScript.LeafFactory scriptBuilder;
    private final Float minScore;
    private final String indexName;
    private final int shardId;
    private final Version indexVersion;
    private final String queryName;

    public ScriptScoreQuery(
        Query subQuery,
        Script script,
        ScoreScript.LeafFactory scriptBuilder,
        Float minScore,
        String indexName,
        int shardId,
        Version indexVersion
    ) {
        this(subQuery, null, script, scriptBuilder, minScore, indexName, shardId, indexVersion);
    }

    public ScriptScoreQuery(
        Query subQuery,
        @Nullable String queryName,
        Script script,
        ScoreScript.LeafFactory scriptBuilder,
        Float minScore,
        String indexName,
        int shardId,
        Version indexVersion
    ) {
        this.subQuery = subQuery;
        this.queryName = queryName;
        this.script = script;
        this.scriptBuilder = scriptBuilder;
        this.minScore = minScore;
        this.indexName = indexName;
        this.shardId = shardId;
        this.indexVersion = indexVersion;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query newQ = subQuery.rewrite(searcher);
        if (newQ != subQuery) {
            return new ScriptScoreQuery(newQ, queryName, script, scriptBuilder, minScore, indexName, shardId, indexVersion);
        }
        return super.rewrite(searcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (scoreMode == ScoreMode.COMPLETE_NO_SCORES && minScore == null) {
            return subQuery.createWeight(searcher, scoreMode, boost);
        }
        boolean needsScore = scriptBuilder.needs_score();
        ScoreMode subQueryScoreMode = needsScore ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        Weight subQueryWeight = subQuery.createWeight(searcher, subQueryScoreMode, 1.0f);

        return new Weight(this) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final Weight weight = this;
                return new ScorerSupplier() {
                    private Scorer scorer;
                    private BulkScorer bulkScorer;

                    @Override
                    public BulkScorer bulkScorer() throws IOException {
                        if (minScore == null) {
                            final BulkScorer subQueryBulkScorer = subQueryWeight.bulkScorer(context);
                            if (subQueryBulkScorer == null) {
                                bulkScorer = null;
                            } else {
                                bulkScorer = new ScriptScoreBulkScorer(
                                    subQueryBulkScorer,
                                    subQueryScoreMode,
                                    makeScoreScript(context),
                                    boost
                                );
                            }
                        } else {
                            final Scorer scorer = get(Long.MAX_VALUE);
                            if (scorer != null) {
                                bulkScorer = new Weight.DefaultBulkScorer(scorer);
                            }
                        }

                        return bulkScorer;
                    }

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        final Scorer subQueryScorer = subQueryWeight.scorer(context);

                        if (subQueryScorer == null) {
                            scorer = null;
                        } else {
                            Scorer scriptScorer = new ScriptScorer(
                                weight,
                                makeScoreScript(context),
                                subQueryScorer,
                                subQueryScoreMode,
                                boost,
                                null
                            );
                            if (minScore != null) {
                                scriptScorer = new MinScoreScorer(weight, scriptScorer, minScore);
                            }
                            scorer = scriptScorer;
                        }

                        return scorer;
                    }

                    @Override
                    public long cost() {
                        if (scorer != null) {
                            return scorer.iterator().cost();
                        } else if (bulkScorer != null) {
                            return bulkScorer.cost();
                        } else {
                            // We have no prior knowledge of how many docs might match for any given query term,
                            // so we assume that all docs could be a match.
                            return Integer.MAX_VALUE;
                        }
                    }
                };
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                Explanation subQueryExplanation = Functions.explainWithName(subQueryWeight.explain(context, doc), queryName);
                if (subQueryExplanation.isMatch() == false) {
                    return subQueryExplanation;
                }
                ExplanationHolder explanationHolder = new ExplanationHolder();
                Scorer scorer = new ScriptScorer(
                    this,
                    makeScoreScript(context),
                    subQueryWeight.scorer(context),
                    subQueryScoreMode,
                    1f,
                    explanationHolder
                );
                int newDoc = scorer.iterator().advance(doc);
                assert doc == newDoc; // subquery should have already matched above
                float score = scorer.score(); // score without boost

                Explanation explanation = explanationHolder.get(score, needsScore ? subQueryExplanation : null);
                if (explanation == null) {
                    // no explanation provided by user; give a simple one
                    String desc = "script score function, computed with script:\"" + script + "\"";
                    if (needsScore) {
                        Explanation scoreExp = Explanation.match(subQueryExplanation.getValue(), "_score: ", subQueryExplanation);
                        explanation = Explanation.match(score, desc, scoreExp);
                    } else {
                        explanation = Explanation.match(score, desc);
                    }
                }
                if (boost != 1f) {
                    explanation = Explanation.match(
                        boost * explanation.getValue().floatValue(),
                        "Boosted score, product of:",
                        Explanation.match(boost, "boost"),
                        explanation
                    );
                }
                if (minScore != null && minScore > explanation.getValue().floatValue()) {
                    explanation = Explanation.noMatch(
                        "Score value is too low, expected at least " + minScore + " but got " + explanation.getValue(),
                        explanation
                    );
                }
                return explanation;
            }

            private ScoreScript makeScoreScript(LeafReaderContext context) throws IOException {
                final ScoreScript scoreScript = scriptBuilder.newInstance(context);
                scoreScript._setIndexName(indexName);
                scoreScript._setShard(shardId);
                scoreScript._setIndexVersion(indexVersion);
                return scoreScript;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // If minScore is not null, then matches depend on statistics of the top-level reader.
                return minScore == null;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        // Highlighters must visit the child query to extract terms
        subQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("script_score (").append(subQuery.toString(field));
        sb.append(Functions.nameOrEmptyArg(queryName)).append(", script: ");
        sb.append("{" + script.toString() + "}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptScoreQuery that = (ScriptScoreQuery) o;
        return shardId == that.shardId
            && subQuery.equals(that.subQuery)
            && script.equals(that.script)
            && Objects.equals(minScore, that.minScore)
            && indexName.equals(that.indexName)
            && indexVersion.equals(that.indexVersion)
            && Objects.equals(queryName, that.queryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subQuery, script, minScore, indexName, shardId, indexVersion, queryName);
    }

    /**
     * A script scorer
     *
     * @opensearch.internal
     */
    private static class ScriptScorer extends Scorer {
        private final ScoreScript scoreScript;
        private final Scorer subQueryScorer;
        private final float boost;
        private final ExplanationHolder explanation;

        ScriptScorer(
            Weight weight,
            ScoreScript scoreScript,
            Scorer subQueryScorer,
            ScoreMode subQueryScoreMode,
            float boost,
            ExplanationHolder explanation
        ) {
            super();
            this.scoreScript = scoreScript;
            if (subQueryScoreMode == ScoreMode.COMPLETE) {
                scoreScript.setScorer(subQueryScorer);
            }
            this.subQueryScorer = subQueryScorer;
            this.boost = boost;
            this.explanation = explanation;
        }

        @Override
        public float score() throws IOException {
            int docId = docID();
            scoreScript.setDocument(docId);
            float score = (float) scoreScript.execute(explanation);
            if (score < 0f || Float.isNaN(score)) {
                throw new IllegalArgumentException(
                    "script_score script returned an invalid score ["
                        + score
                        + "] "
                        + "for doc ["
                        + docId
                        + "]. Must be a non-negative score!"
                );
            }
            return score * boost;
        }

        @Override
        public int docID() {
            return subQueryScorer.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return subQueryScorer.iterator();
        }

        @Override
        public TwoPhaseIterator twoPhaseIterator() {
            return subQueryScorer.twoPhaseIterator();
        }

        @Override
        public float getMaxScore(int upTo) {
            return Float.MAX_VALUE; // TODO: what would be a good upper bound?
        }

    }

    /**
     * A script scorable
     *
     * @opensearch.internal
     */
    private static class ScriptScorable extends Scorable {
        private final ScoreScript scoreScript;
        private final Scorable subQueryScorer;
        private final float boost;
        private final ExplanationHolder explanation;
        private int docId;

        ScriptScorable(
            ScoreScript scoreScript,
            Scorable subQueryScorer,
            ScoreMode subQueryScoreMode,
            float boost,
            ExplanationHolder explanation
        ) {
            this.scoreScript = scoreScript;
            if (subQueryScoreMode == ScoreMode.COMPLETE) {
                scoreScript.setScorer(subQueryScorer);
            }
            this.subQueryScorer = subQueryScorer;
            this.boost = boost;
            this.explanation = explanation;
        }

        void setDocument(int docId) {
            this.docId = docId;
        }

        @Override
        public float score() throws IOException {
            scoreScript.setDocument(docId);
            float score = (float) scoreScript.execute(explanation);
            if (score < 0f || Float.isNaN(score)) {
                throw new IllegalArgumentException(
                    "script_score script returned an invalid score ["
                        + score
                        + "] "
                        + "for doc ["
                        + docId
                        + "]. Must be a non-negative score!"
                );
            }
            return score * boost;
        }
    }

    /**
     * Use the {@link BulkScorer} of the sub-query,
     * as it may be significantly faster (e.g. BooleanScorer) than iterating over the scorer
     *
     * @opensearch.internal
     */
    private static class ScriptScoreBulkScorer extends BulkScorer {
        private final BulkScorer subQueryBulkScorer;
        private final ScoreMode subQueryScoreMode;
        private final ScoreScript scoreScript;
        private final float boost;

        ScriptScoreBulkScorer(BulkScorer subQueryBulkScorer, ScoreMode subQueryScoreMode, ScoreScript scoreScript, float boost) {
            this.subQueryBulkScorer = subQueryBulkScorer;
            this.subQueryScoreMode = subQueryScoreMode;
            this.scoreScript = scoreScript;
            this.boost = boost;
        }

        @Override
        public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
            return subQueryBulkScorer.score(wrapCollector(collector), acceptDocs, min, max);
        }

        private LeafCollector wrapCollector(LeafCollector collector) {
            return new FilterLeafCollector(collector) {
                private ScriptScorable scriptScorable;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    scriptScorable = new ScriptScorable(scoreScript, scorer, subQueryScoreMode, boost, null);
                    in.setScorer(scriptScorable);
                }

                @Override
                public void collect(int doc) throws IOException {
                    scriptScorable.setDocument(doc);
                    super.collect(doc);
                }
            };
        }

        @Override
        public long cost() {
            return subQueryBulkScorer.cost();
        }

    }

}
