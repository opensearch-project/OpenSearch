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
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.SearchHit;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.SubSearchContext;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Context used for inner hits retrieval
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class InnerHitsContext {
    private final Map<String, InnerHitSubContext> innerHits;

    public InnerHitsContext() {
        this.innerHits = new HashMap<>();
    }

    InnerHitsContext(Map<String, InnerHitSubContext> innerHits) {
        this.innerHits = Objects.requireNonNull(innerHits);
    }

    public Map<String, InnerHitSubContext> getInnerHits() {
        return innerHits;
    }

    public void addInnerHitDefinition(InnerHitSubContext innerHit) {
        if (innerHits.containsKey(innerHit.getName())) {
            throw new IllegalArgumentException(
                "inner_hit definition with the name ["
                    + innerHit.getName()
                    + "] already exists. Use a different inner_hit name or define one explicitly"
            );
        }

        innerHits.put(innerHit.getName(), innerHit);
    }

    /**
     * A {@link SubSearchContext} that associates {@link TopDocs} to each {@link SearchHit}
     * in the parent search context
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public abstract static class InnerHitSubContext extends SubSearchContext {

        private final String name;
        protected final SearchContext context;
        private InnerHitsContext childInnerHits;
        private Weight innerHitQueryWeight;

        private String id;
        private SourceLookup rootLookup;

        protected InnerHitSubContext(String name, SearchContext context) {
            super(context);
            this.name = name;
            this.context = context;
        }

        public abstract TopDocsAndMaxScore topDocs(SearchHit hit) throws IOException;

        public String getName() {
            return name;
        }

        @Override
        public InnerHitsContext innerHits() {
            return childInnerHits;
        }

        public void setChildInnerHits(Map<String, InnerHitSubContext> childInnerHits) {
            this.childInnerHits = new InnerHitsContext(childInnerHits);
        }

        protected Weight getInnerHitQueryWeight() throws IOException {
            if (innerHitQueryWeight == null) {
                final boolean needsScores = size() != 0 && (sort() == null || sort().sort.needsScores());
                innerHitQueryWeight = context.searcher()
                    .createWeight(context.searcher().rewrite(query()), needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES, 1f);
            }
            return innerHitQueryWeight;
        }

        public SearchContext parentSearchContext() {
            return context;
        }

        /**
         * The _id of the root document.
         * <p>
         * Since this ID is available on the context, inner hits can avoid re-loading the root _id.
         */
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        /**
         * A source lookup for the root document.
         * <p>
         * This shared lookup allows inner hits to avoid re-loading the root _source.
         */
        public SourceLookup getRootLookup() {
            return rootLookup;
        }

        public void setRootLookup(SourceLookup rootLookup) {
            this.rootLookup = rootLookup;
        }
    }

    public static void intersect(Weight weight, Weight innerHitQueryWeight, Collector collector, LeafReaderContext ctx) throws IOException {
        ScorerSupplier scorerSupplier = weight.scorerSupplier(ctx);
        if (scorerSupplier == null) {
            return;
        }
        // use low leadCost since this scorer will be consumed on a minority of documents
        Scorer scorer = scorerSupplier.get(0);

        ScorerSupplier innerHitQueryScorerSupplier = innerHitQueryWeight.scorerSupplier(ctx);
        if (innerHitQueryScorerSupplier == null) {
            return;
        }
        // use low loadCost since this scorer will be consumed on a minority of documents
        Scorer innerHitQueryScorer = innerHitQueryScorerSupplier.get(0);

        final LeafCollector leafCollector;
        try {
            leafCollector = collector.getLeafCollector(ctx);
            // Just setting the innerHitQueryScorer is ok, because that is the actual scoring part of the query
            leafCollector.setScorer(innerHitQueryScorer);
        } catch (CollectionTerminatedException e) {
            return;
        }

        try {
            Bits acceptDocs = ctx.reader().getLiveDocs();
            DocIdSetIterator iterator = ConjunctionUtils.intersectIterators(
                Arrays.asList(innerHitQueryScorer.iterator(), scorer.iterator())
            );
            for (int docId = iterator.nextDoc(); docId < DocIdSetIterator.NO_MORE_DOCS; docId = iterator.nextDoc()) {
                if (acceptDocs == null || acceptDocs.get(docId)) {
                    leafCollector.collect(docId);
                }
            }
        } catch (CollectionTerminatedException e) {
            // ignore and continue
        }
    }
}
