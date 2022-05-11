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

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.MaxScoreCollector;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.LongObjectPagedHashMap;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.SubSearchContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregate all docs into the top hits
 *
 * @opensearch.internal
 */
class TopHitsAggregator extends MetricsAggregator {

    /**
     * Collectors for top hits
     *
     * @opensearch.internal
     */
    private static class Collectors {
        public final TopDocsCollector<?> topDocsCollector;
        public final MaxScoreCollector maxScoreCollector;
        public final Collector collector;

        Collectors(TopDocsCollector<?> topDocsCollector, MaxScoreCollector maxScoreCollector) {
            this.topDocsCollector = topDocsCollector;
            this.maxScoreCollector = maxScoreCollector;
            collector = MultiCollector.wrap(topDocsCollector, maxScoreCollector);
        }
    }

    private final FetchPhase fetchPhase;
    private final SubSearchContext subSearchContext;
    private final LongObjectPagedHashMap<Collectors> topDocsCollectors;

    TopHitsAggregator(
        FetchPhase fetchPhase,
        SubSearchContext subSearchContext,
        String name,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.fetchPhase = fetchPhase;
        topDocsCollectors = new LongObjectPagedHashMap<>(1, context.bigArrays());
        this.subSearchContext = subSearchContext;
    }

    @Override
    public ScoreMode scoreMode() {
        SortAndFormats sort = subSearchContext.sort();
        if (sort != null) {
            return sort.sort.needsScores() || subSearchContext.trackScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        } else {
            // sort by score
            return ScoreMode.COMPLETE;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Create leaf collectors here instead of at the aggregator level. Otherwise in case this collector get invoked
        // when post collecting then we have already replaced the leaf readers on the aggregator level have already been
        // replaced with the next leaf readers and then post collection pushes docids of the previous segment, which
        // then causes assertions to trip or incorrect top docs to be computed.
        final LongObjectHashMap<LeafCollector> leafCollectors = new LongObjectHashMap<>(1);
        return new LeafBucketCollectorBase(sub, null) {

            Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
                super.setScorer(scorer);
                for (ObjectCursor<LeafCollector> cursor : leafCollectors.values()) {
                    cursor.value.setScorer(scorer);
                }
            }

            @Override
            public void collect(int docId, long bucket) throws IOException {
                Collectors collectors = topDocsCollectors.get(bucket);
                if (collectors == null) {
                    SortAndFormats sort = subSearchContext.sort();
                    int topN = subSearchContext.from() + subSearchContext.size();
                    if (sort == null) {
                        for (RescoreContext rescoreContext : context.rescore()) {
                            topN = Math.max(rescoreContext.getWindowSize(), topN);
                        }
                    }
                    // In the QueryPhase we don't need this protection, because it is build into the IndexSearcher,
                    // but here we create collectors ourselves and we need prevent OOM because of crazy an offset and size.
                    topN = Math.min(topN, subSearchContext.searcher().getIndexReader().maxDoc());
                    if (sort == null) {
                        collectors = new Collectors(TopScoreDocCollector.create(topN, Integer.MAX_VALUE), null);
                    } else {
                        // TODO: can we pass trackTotalHits=subSearchContext.trackTotalHits(){
                        // Note that this would require to catch CollectionTerminatedException
                        collectors = new Collectors(
                            TopFieldCollector.create(sort.sort, topN, Integer.MAX_VALUE),
                            subSearchContext.trackScores() ? new MaxScoreCollector() : null
                        );
                    }
                    topDocsCollectors.put(bucket, collectors);
                }

                final LeafCollector leafCollector;
                final int key = leafCollectors.indexOf(bucket);
                if (key < 0) {
                    leafCollector = collectors.collector.getLeafCollector(ctx);
                    if (scorer != null) {
                        leafCollector.setScorer(scorer);
                    }
                    leafCollectors.indexInsert(key, bucket, leafCollector);
                } else {
                    leafCollector = leafCollectors.indexGet(key);
                }
                leafCollector.collect(docId);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        Collectors collectors = topDocsCollectors.get(owningBucketOrdinal);
        if (collectors == null) {
            return buildEmptyAggregation();
        }
        TopDocsCollector<?> topDocsCollector = collectors.topDocsCollector;
        TopDocs topDocs = topDocsCollector.topDocs();
        float maxScore = Float.NaN;
        if (subSearchContext.sort() == null) {
            for (RescoreContext ctx : context().rescore()) {
                try {
                    topDocs = ctx.rescorer().rescore(topDocs, context.searcher(), ctx);
                } catch (IOException e) {
                    throw new OpenSearchException("Rescore TopHits Failed", e);
                }
            }
            if (topDocs.scoreDocs.length > 0) {
                maxScore = topDocs.scoreDocs[0].score;
            }
        } else if (subSearchContext.trackScores()) {
            TopFieldCollector.populateScores(topDocs.scoreDocs, subSearchContext.searcher(), subSearchContext.query());
            maxScore = collectors.maxScoreCollector.getMaxScore();
        }
        final TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);
        subSearchContext.queryResult()
            .topDocs(topDocsAndMaxScore, subSearchContext.sort() == null ? null : subSearchContext.sort().formats);
        int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
        }
        subSearchContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
        fetchPhase.execute(subSearchContext);
        FetchSearchResult fetchResult = subSearchContext.fetchResult();
        SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
        for (int i = 0; i < internalHits.length; i++) {
            ScoreDoc scoreDoc = topDocs.scoreDocs[i];
            SearchHit searchHitFields = internalHits[i];
            searchHitFields.shard(subSearchContext.shardTarget());
            searchHitFields.score(scoreDoc.score);
            if (scoreDoc instanceof FieldDoc) {
                FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                searchHitFields.sortValues(fieldDoc.fields, subSearchContext.sort().formats);
            }
        }
        return new InternalTopHits(
            name,
            subSearchContext.from(),
            subSearchContext.size(),
            topDocsAndMaxScore,
            fetchResult.hits(),
            metadata()
        );
    }

    @Override
    public InternalTopHits buildEmptyAggregation() {
        TopDocs topDocs;
        if (subSearchContext.sort() != null) {
            topDocs = new TopFieldDocs(
                new TotalHits(0, TotalHits.Relation.EQUAL_TO),
                new FieldDoc[0],
                subSearchContext.sort().sort.getSort()
            );
        } else {
            topDocs = Lucene.EMPTY_TOP_DOCS;
        }
        return new InternalTopHits(
            name,
            subSearchContext.from(),
            subSearchContext.size(),
            new TopDocsAndMaxScore(topDocs, Float.NaN),
            SearchHits.empty(),
            metadata()
        );
    }

    @Override
    protected void doClose() {
        Releasables.close(topDocsCollectors);
    }
}
