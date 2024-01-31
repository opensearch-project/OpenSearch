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
package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fetches queries that match the document during search phase
 *
 * @opensearch.internal
 */
public final class MatchedQueriesPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) throws IOException {
        Map<String, Query> namedQueries = new HashMap<>();
        if (context.parsedQuery() != null) {
            namedQueries.putAll(context.parsedQuery().namedFilters());
        }
        if (context.parsedPostFilter() != null) {
            namedQueries.putAll(context.parsedPostFilter().namedFilters());
        }
        if (namedQueries.isEmpty()) {
            return null;
        }

        Map<String, Weight> weights = prepareWeights(context, namedQueries);

        return context.includeNamedQueriesScore() ? createScoringProcessor(weights) : createNonScoringProcessor(weights);
    }

    private Map<String, Weight> prepareWeights(FetchContext context, Map<String, Query> namedQueries) throws IOException {
        Map<String, Weight> weights = new HashMap<>();
        ScoreMode scoreMode = context.includeNamedQueriesScore() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        for (Map.Entry<String, Query> entry : namedQueries.entrySet()) {
            weights.put(entry.getKey(), context.searcher().createWeight(context.searcher().rewrite(entry.getValue()), scoreMode, 1));
        }
        return weights;
    }

    private FetchSubPhaseProcessor createScoringProcessor(Map<String, Weight> weights) {
        return new FetchSubPhaseProcessor() {
            final Map<String, Scorer> matchingScorers = new HashMap<>();

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                matchingScorers.clear();
                for (Map.Entry<String, Weight> entry : weights.entrySet()) {
                    ScorerSupplier scorerSupplier = entry.getValue().scorerSupplier(readerContext);
                    if (scorerSupplier != null) {
                        Scorer scorer = scorerSupplier.get(0L);
                        if (scorer != null) {
                            matchingScorers.put(entry.getKey(), scorer);
                        }
                    }
                }
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Map<String, Float> matches = new LinkedHashMap<>();
                int docId = hitContext.docId();
                for (Map.Entry<String, Scorer> entry : matchingScorers.entrySet()) {
                    Scorer scorer = entry.getValue();
                    if (scorer.iterator().docID() < docId) {
                        scorer.iterator().advance(docId);
                    }
                    if (scorer.iterator().docID() == docId) {
                        matches.put(entry.getKey(), scorer.score());
                    }
                }
                hitContext.hit().matchedQueriesWithScores(matches);
            }
        };
    }

    private FetchSubPhaseProcessor createNonScoringProcessor(Map<String, Weight> weights) {
        return new FetchSubPhaseProcessor() {
            final Map<String, Bits> matchingBits = new HashMap<>();

            @Override
            public void setNextReader(LeafReaderContext readerContext) throws IOException {
                matchingBits.clear();
                for (Map.Entry<String, Weight> entry : weights.entrySet()) {
                    ScorerSupplier scorerSupplier = entry.getValue().scorerSupplier(readerContext);
                    if (scorerSupplier != null) {
                        Bits bits = Lucene.asSequentialAccessBits(readerContext.reader().maxDoc(), scorerSupplier);
                        matchingBits.put(entry.getKey(), bits);
                    }
                }
            }

            @Override
            public void process(HitContext hitContext) {
                List<String> matches = new ArrayList<>();
                int docId = hitContext.docId();
                for (Map.Entry<String, Bits> entry : matchingBits.entrySet()) {
                    if (entry.getValue().get(docId)) {
                        matches.add(entry.getKey());
                    }
                }
                hitContext.hit().matchedQueries(matches.toArray(new String[0]));
            }
        };
    }
}
