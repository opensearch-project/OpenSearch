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

package org.opensearch.search.rescore;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.search.rescore.QueryRescorer.QueryRescoreContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * A concrete query rescorer used to re-rank the Top-K results of a previously
 * executed search.
 *
 * @opensearch.internal
 */
public final class QueryRescorer implements Rescorer {

    public static final Rescorer INSTANCE = new QueryRescorer();

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {

        assert rescoreContext != null;
        if (topDocs == null || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }

        final QueryRescoreContext rescore = (QueryRescoreContext) rescoreContext;

        org.apache.lucene.search.Rescorer rescorer = new org.apache.lucene.search.QueryRescorer(rescore.parsedQuery().query()) {

            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                if (secondPassMatches) {
                    return rescore.scoreMode.combine(
                        firstPassScore * rescore.queryWeight(),
                        secondPassScore * rescore.rescoreQueryWeight()
                    );
                }
                // TODO: shouldn't this be up to the ScoreMode? I.e., we should just invoke ScoreMode.combine, passing 0.0f for the
                // secondary score?
                return firstPassScore * rescore.queryWeight();
            }
        };

        // First take top slice of incoming docs, to be rescored:
        TopDocs topNFirstPass = topN(topDocs, rescoreContext.getWindowSize());

        // Save doc IDs for which rescoring was applied to be used in score explanation
        Set<Integer> topNDocIDs = Collections.unmodifiableSet(
            Arrays.stream(topNFirstPass.scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(toSet())
        );
        rescoreContext.setRescoredDocs(topNDocIDs);

        // Rescore them:
        TopDocs rescored = rescorer.rescore(searcher, topNFirstPass, rescoreContext.getWindowSize());

        // Splice back to non-topN hits and resort all of them:
        return combine(topDocs, rescored, (QueryRescoreContext) rescoreContext);
    }

    @Override
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext, Explanation sourceExplanation)
        throws IOException {
        if (sourceExplanation == null) {
            // this should not happen but just in case
            return Explanation.noMatch("nothing matched");
        }
        QueryRescoreContext rescore = (QueryRescoreContext) rescoreContext;
        float primaryWeight = rescore.queryWeight();
        Explanation prim;
        if (sourceExplanation.isMatch()) {
            prim = Explanation.match(
                sourceExplanation.getValue().floatValue() * primaryWeight,
                "product of:",
                sourceExplanation,
                Explanation.match(primaryWeight, "primaryWeight")
            );
        } else {
            prim = Explanation.noMatch("First pass did not match", sourceExplanation);
        }
        if (rescoreContext.isRescored(topLevelDocId)) {
            Explanation rescoreExplain = searcher.explain(rescore.parsedQuery().query(), topLevelDocId);
            // NOTE: we don't use Lucene's Rescorer.explain because we want to insert our own description with which ScoreMode was used.
            // Maybe we should add QueryRescorer.explainCombine to Lucene?
            if (rescoreExplain != null && rescoreExplain.isMatch()) {
                float secondaryWeight = rescore.rescoreQueryWeight();
                Explanation sec = Explanation.match(
                    rescoreExplain.getValue().floatValue() * secondaryWeight,
                    "product of:",
                    rescoreExplain,
                    Explanation.match(secondaryWeight, "secondaryWeight")
                );
                QueryRescoreMode scoreMode = rescore.scoreMode();
                return Explanation.match(
                    scoreMode.combine(prim.getValue().floatValue(), sec.getValue().floatValue()),
                    scoreMode + " of:",
                    prim,
                    sec
                );
            }
        }
        return prim;
    }

    private static final Comparator<ScoreDoc> SCORE_DOC_COMPARATOR = new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc o1, ScoreDoc o2) {
            int cmp = Float.compare(o2.score, o1.score);
            return cmp == 0 ? Integer.compare(o1.doc, o2.doc) : cmp;
        }
    };

    /** Returns a new {@link TopDocs} with the topN from the incoming one, or the same TopDocs if the number of hits is already &lt;=
     *  topN. */
    private TopDocs topN(TopDocs in, int topN) {
        if (in.scoreDocs.length < topN) {
            return in;
        }

        ScoreDoc[] subset = new ScoreDoc[topN];
        System.arraycopy(in.scoreDocs, 0, subset, 0, topN);

        return new TopDocs(in.totalHits, subset);
    }

    /** Modifies incoming TopDocs (in) by replacing the top hits with resorted's hits, and then resorting all hits. */
    private TopDocs combine(TopDocs in, TopDocs resorted, QueryRescoreContext ctx) {

        System.arraycopy(resorted.scoreDocs, 0, in.scoreDocs, 0, resorted.scoreDocs.length);
        if (in.scoreDocs.length > resorted.scoreDocs.length) {
            // These hits were not rescored (beyond the rescore window), so we treat them the same as a hit that did get rescored but did
            // not match the 2nd pass query:
            for (int i = resorted.scoreDocs.length; i < in.scoreDocs.length; i++) {
                // TODO: shouldn't this be up to the ScoreMode? I.e., we should just invoke ScoreMode.combine, passing 0.0f for the
                // secondary score?
                in.scoreDocs[i].score *= ctx.queryWeight();
            }

            // TODO: this is wrong, i.e. we are comparing apples and oranges at this point. It would be better if we always rescored all
            // incoming first pass hits, instead of allowing recoring of just the top subset:
            Arrays.sort(in.scoreDocs, SCORE_DOC_COMPARATOR);
        }
        return in;
    }

    /**
     * Context used during query rescoring
     *
     * @opensearch.internal
     */
    public static class QueryRescoreContext extends RescoreContext {
        private ParsedQuery parsedQuery;
        private float queryWeight = 1.0f;
        private float rescoreQueryWeight = 1.0f;
        private QueryRescoreMode scoreMode;

        public QueryRescoreContext(int windowSize) {
            super(windowSize, QueryRescorer.INSTANCE);
            this.scoreMode = QueryRescoreMode.Total;
        }

        public void setParsedQuery(ParsedQuery parsedQuery) {
            this.parsedQuery = parsedQuery;
        }

        public ParsedQuery parsedQuery() {
            return parsedQuery;
        }

        @Override
        public List<Query> getQueries() {
            return parsedQuery != null ? Collections.singletonList(parsedQuery.query()) : Collections.emptyList();
        }

        @Override
        public List<ParsedQuery> getParsedQueries() {
            return parsedQuery != null ? Collections.singletonList(parsedQuery) : Collections.emptyList();
        }

        public float queryWeight() {
            return queryWeight;
        }

        public float rescoreQueryWeight() {
            return rescoreQueryWeight;
        }

        public QueryRescoreMode scoreMode() {
            return scoreMode;
        }

        public void setRescoreQueryWeight(float rescoreQueryWeight) {
            this.rescoreQueryWeight = rescoreQueryWeight;
        }

        public void setQueryWeight(float queryWeight) {
            this.queryWeight = queryWeight;
        }

        public void setScoreMode(QueryRescoreMode scoreMode) {
            this.scoreMode = scoreMode;
        }

        public void setScoreMode(String scoreMode) {
            setScoreMode(QueryRescoreMode.fromString(scoreMode));
        }
    }

}
