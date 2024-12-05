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

package org.opensearch.search.query;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.opensearch.action.search.MaxScoreCollector;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.common.lucene.search.function.ScriptScoreQuery;
import org.opensearch.common.util.CachedSupplier;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.search.profile.query.CollectorResult.REASON_SEARCH_COUNT;
import static org.opensearch.search.profile.query.CollectorResult.REASON_SEARCH_TOP_HITS;

/**
 * A {@link QueryCollectorContext} that creates top docs collector
 *
 * @opensearch.internal
 */
public abstract class TopDocsCollectorContext extends QueryCollectorContext implements RescoringQueryCollectorContext {
    protected final int numHits;

    TopDocsCollectorContext(String profilerName, int numHits) {
        super(profilerName);
        this.numHits = numHits;
    }

    /**
     * Returns the number of top docs to retrieve
     */
    final int numHits() {
        return numHits;
    }

    /**
     * Returns true if the top docs should be re-scored after initial search
     */
    public boolean shouldRescore() {
        return false;
    }

    static class EmptyTopDocsCollectorContext extends TopDocsCollectorContext {
        private final Sort sort;
        private final Collector collector;
        private final Supplier<TotalHits> hitCountSupplier;
        private final int trackTotalHitsUpTo;
        private final int hitCount;

        /**
         * Ctr
         * @param reader The index reader
         * @param query The query to execute
         * @param trackTotalHitsUpTo True if the total number of hits should be tracked
         * @param hasFilterCollector True if the collector chain contains a filter
         */
        private EmptyTopDocsCollectorContext(
            IndexReader reader,
            Query query,
            @Nullable SortAndFormats sortAndFormats,
            int trackTotalHitsUpTo,
            boolean hasFilterCollector
        ) throws IOException {
            super(REASON_SEARCH_COUNT, 0);
            this.sort = sortAndFormats == null ? null : sortAndFormats.sort;
            this.trackTotalHitsUpTo = trackTotalHitsUpTo;
            if (this.trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                this.collector = new EarlyTerminatingCollector(new TotalHitCountCollector(), 0, false);
                // for bwc hit count is set to 0, it will be converted to -1 by the coordinating node
                this.hitCountSupplier = () -> new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                this.hitCount = Integer.MIN_VALUE;
            } else {
                TotalHitCountCollector hitCountCollector = new TotalHitCountCollector();
                // implicit total hit counts are valid only when there is no filter collector in the chain
                this.hitCount = hasFilterCollector ? -1 : shortcutTotalHitCount(reader, query);
                if (this.hitCount == -1) {
                    if (this.trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                        this.collector = hitCountCollector;
                        this.hitCountSupplier = () -> new TotalHits(hitCountCollector.getTotalHits(), TotalHits.Relation.EQUAL_TO);
                    } else {
                        EarlyTerminatingCollector col = new EarlyTerminatingCollector(hitCountCollector, trackTotalHitsUpTo, false);
                        this.collector = col;
                        this.hitCountSupplier = () -> new TotalHits(
                            hitCountCollector.getTotalHits(),
                            col.hasEarlyTerminated() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO
                        );
                    }
                } else {
                    this.collector = new EarlyTerminatingCollector(hitCountCollector, 0, false);
                    this.hitCountSupplier = () -> new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO);
                }
            }
        }

        @Override
        CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
            assert in == null;

            CollectorManager<?, ReduceableSearchResult> manager = null;

            if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                manager = new EarlyTerminatingCollectorManager<>(
                    new TotalHitCountCollectorManager.Empty(new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), sort),
                    0,
                    false
                );
            } else {
                if (hitCount == -1) {
                    if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                        manager = new TotalHitCountCollectorManager(sort);
                    } else {
                        manager = new EarlyTerminatingCollectorManager<>(
                            new TotalHitCountCollectorManager(sort),
                            trackTotalHitsUpTo,
                            false
                        );
                    }
                } else {
                    manager = new EarlyTerminatingCollectorManager<>(
                        new TotalHitCountCollectorManager.Empty(new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO), sort),
                        0,
                        false
                    );
                }
            }

            return manager;
        }

        @Override
        Collector create(Collector in) {
            assert in == null;
            return collector;
        }

        @Override
        void postProcess(QuerySearchResult result) {
            final TotalHits totalHitCount = hitCountSupplier.get();
            final TopDocs topDocs;
            if (sort != null) {
                topDocs = new TopFieldDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS, sort.getSort());
            } else {
                topDocs = new TopDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS);
            }
            result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    static class CollapsingTopDocsCollectorContext extends TopDocsCollectorContext {
        private final DocValueFormat[] sortFmt;
        private final CollapsingTopDocsCollector<?> topDocsCollector;
        private final Collector collector;
        private final Supplier<Float> maxScoreSupplier;
        private final CollapseContext collapseContext;
        private final boolean trackMaxScore;
        private final Sort sort;

        /**
         * Ctr
         * @param collapseContext The collapsing context
         * @param sortAndFormats The query sort
         * @param numHits The number of collapsed top hits to retrieve.
         * @param trackMaxScore True if max score should be tracked
         */
        private CollapsingTopDocsCollectorContext(
            CollapseContext collapseContext,
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            boolean trackMaxScore
        ) {
            super(REASON_SEARCH_TOP_HITS, numHits);
            assert numHits > 0;
            assert collapseContext != null;
            this.sort = sortAndFormats == null ? Sort.RELEVANCE : sortAndFormats.sort;
            this.sortFmt = sortAndFormats == null ? new DocValueFormat[] { DocValueFormat.RAW } : sortAndFormats.formats;
            this.collapseContext = collapseContext;
            this.topDocsCollector = collapseContext.createTopDocs(sort, numHits);
            this.trackMaxScore = trackMaxScore;

            MaxScoreCollector maxScoreCollector = null;
            if (trackMaxScore) {
                maxScoreCollector = new MaxScoreCollector();
                maxScoreSupplier = maxScoreCollector::getMaxScore;
            } else {
                maxScoreCollector = null;
                maxScoreSupplier = () -> Float.NaN;
            }

            this.collector = MultiCollector.wrap(topDocsCollector, maxScoreCollector);
        }

        @Override
        Collector create(Collector in) throws IOException {
            assert in == null;
            return collector;
        }

        @Override
        void postProcess(QuerySearchResult result) throws IOException {
            final CollapseTopFieldDocs topDocs = topDocsCollector.getTopDocs();
            result.topDocs(new TopDocsAndMaxScore(topDocs, maxScoreSupplier.get()), sortFmt);
        }

        @Override
        CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
            return new CollectorManager<Collector, ReduceableSearchResult>() {
                @Override
                public Collector newCollector() throws IOException {
                    MaxScoreCollector maxScoreCollector = null;

                    if (trackMaxScore) {
                        maxScoreCollector = new MaxScoreCollector();
                    }

                    return MultiCollectorWrapper.wrap(collapseContext.createTopDocs(sort, numHits), maxScoreCollector);
                }

                @Override
                public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
                    final Collection<Collector> subs = new ArrayList<>();
                    for (final Collector collector : collectors) {
                        if (collector instanceof MultiCollectorWrapper) {
                            subs.addAll(((MultiCollectorWrapper) collector).getCollectors());
                        } else {
                            subs.add(collector);
                        }
                    }

                    final Collection<CollapseTopFieldDocs> topFieldDocs = new ArrayList<CollapseTopFieldDocs>();
                    float maxScore = Float.NaN;

                    for (final Collector collector : subs) {
                        if (collector instanceof CollapsingTopDocsCollector<?>) {
                            topFieldDocs.add(((CollapsingTopDocsCollector<?>) collector).getTopDocs());
                        } else if (collector instanceof MaxScoreCollector) {
                            float score = ((MaxScoreCollector) collector).getMaxScore();
                            if (Float.isNaN(maxScore)) {
                                maxScore = score;
                            } else {
                                maxScore = Math.max(maxScore, score);
                            }
                        }
                    }

                    return reduceWith(topFieldDocs, maxScore);
                }
            };
        }

        protected ReduceableSearchResult reduceWith(final Collection<CollapseTopFieldDocs> topFieldDocs, float maxScore) {
            return (QuerySearchResult result) -> {
                final CollapseTopFieldDocs topDocs = CollapseTopFieldDocs.merge(
                    sort,
                    0,
                    numHits,
                    topFieldDocs.toArray(new CollapseTopFieldDocs[0]),
                    true
                );
                result.topDocs(new TopDocsAndMaxScore(topDocs, maxScore), sortFmt);
            };
        }
    }

    abstract static class SimpleTopDocsCollectorContext extends TopDocsCollectorContext {

        private static TopDocsCollector<?> createCollector(
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            @Nullable ScoreDoc searchAfter,
            int hitCountThreshold
        ) {
            if (sortAndFormats == null) {
                return TopScoreDocCollector.create(numHits, searchAfter, hitCountThreshold);
            } else {
                return TopFieldCollector.create(sortAndFormats.sort, numHits, (FieldDoc) searchAfter, hitCountThreshold);
            }
        }

        private static CollectorManager<? extends TopDocsCollector<?>, ? extends TopDocs> createCollectorManager(
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            @Nullable ScoreDoc searchAfter,
            int hitCountThreshold
        ) {
            if (sortAndFormats == null) {
                // See please https://github.com/apache/lucene/pull/450, should be fixed in 9.x
                if (searchAfter != null) {
                    return TopScoreDocCollector.createSharedManager(
                        numHits,
                        new FieldDoc(searchAfter.doc, searchAfter.score),
                        hitCountThreshold
                    );
                } else {
                    return TopScoreDocCollector.createSharedManager(numHits, null, hitCountThreshold);
                }
            } else {
                return TopFieldCollector.createSharedManager(sortAndFormats.sort, numHits, (FieldDoc) searchAfter, hitCountThreshold);
            }
        }

        protected final @Nullable SortAndFormats sortAndFormats;
        private final Collector collector;
        private final Supplier<TotalHits> totalHitsSupplier;
        private final Supplier<TopDocs> topDocsSupplier;
        private final Supplier<Float> maxScoreSupplier;
        private final ScoreDoc searchAfter;
        private final int trackTotalHitsUpTo;
        private final boolean trackMaxScore;
        private final boolean hasInfMaxScore;
        private final int hitCount;

        /**
         * Ctr
         * @param reader The index reader
         * @param query The Lucene query
         * @param sortAndFormats The query sort
         * @param numHits The number of top hits to retrieve
         * @param searchAfter The doc this request should "search after"
         * @param trackMaxScore True if max score should be tracked
         * @param trackTotalHitsUpTo True if the total number of hits should be tracked
         * @param hasFilterCollector True if the collector chain contains at least one collector that can filters document
         */
        private SimpleTopDocsCollectorContext(
            IndexReader reader,
            Query query,
            @Nullable SortAndFormats sortAndFormats,
            @Nullable ScoreDoc searchAfter,
            int numHits,
            boolean trackMaxScore,
            int trackTotalHitsUpTo,
            boolean hasFilterCollector
        ) throws IOException {
            super(REASON_SEARCH_TOP_HITS, numHits);
            this.sortAndFormats = sortAndFormats;
            this.searchAfter = searchAfter;
            this.trackTotalHitsUpTo = trackTotalHitsUpTo;
            this.trackMaxScore = trackMaxScore;
            this.hasInfMaxScore = hasInfMaxScore(query);

            final TopDocsCollector<?> topDocsCollector;

            if ((sortAndFormats == null || SortField.FIELD_SCORE.equals(sortAndFormats.sort.getSort()[0])) && hasInfMaxScore) {
                // disable max score optimization since we have a mandatory clause
                // that doesn't track the maximum score
                topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, Integer.MAX_VALUE);
                topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                totalHitsSupplier = () -> topDocsSupplier.get().totalHits;
                hitCount = Integer.MIN_VALUE;
            } else if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                // don't compute hit counts via the collector
                topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, 1);
                topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                totalHitsSupplier = () -> new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                hitCount = -1;
            } else {
                // implicit total hit counts are valid only when there is no filter collector in the chain
                this.hitCount = hasFilterCollector ? -1 : shortcutTotalHitCount(reader, query);
                if (this.hitCount == -1) {
                    topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, trackTotalHitsUpTo);
                    topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                    totalHitsSupplier = () -> topDocsSupplier.get().totalHits;
                } else {
                    // don't compute hit counts via the collector
                    topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, 1);
                    topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                    totalHitsSupplier = () -> new TotalHits(this.hitCount, TotalHits.Relation.EQUAL_TO);
                }
            }
            MaxScoreCollector maxScoreCollector = null;
            if (sortAndFormats == null) {
                maxScoreSupplier = () -> {
                    TopDocs topDocs = topDocsSupplier.get();
                    if (topDocs.scoreDocs.length == 0) {
                        return Float.NaN;
                    } else {
                        return topDocs.scoreDocs[0].score;
                    }
                };
            } else if (trackMaxScore) {
                maxScoreCollector = new MaxScoreCollector();
                maxScoreSupplier = maxScoreCollector::getMaxScore;
            } else {
                maxScoreSupplier = () -> Float.NaN;
            }

            this.collector = MultiCollector.wrap(topDocsCollector, maxScoreCollector);
        }

        private class SimpleTopDocsCollectorManager
            implements
                CollectorManager<Collector, ReduceableSearchResult>,
                EarlyTerminatingListener {
            private Integer terminatedAfter;
            private final CollectorManager<? extends TopDocsCollector<?>, ? extends TopDocs> manager;

            private SimpleTopDocsCollectorManager() {
                if ((sortAndFormats == null || SortField.FIELD_SCORE.equals(sortAndFormats.sort.getSort()[0])) && hasInfMaxScore) {
                    // disable max score optimization since we have a mandatory clause
                    // that doesn't track the maximum score
                    manager = createCollectorManager(sortAndFormats, numHits, searchAfter, Integer.MAX_VALUE);
                } else if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                    // don't compute hit counts via the collector
                    manager = createCollectorManager(sortAndFormats, numHits, searchAfter, 1);
                } else {
                    // implicit total hit counts are valid only when there is no filter collector in the chain
                    if (hitCount == -1) {
                        manager = createCollectorManager(sortAndFormats, numHits, searchAfter, trackTotalHitsUpTo);
                    } else {
                        // don't compute hit counts via the collector
                        manager = createCollectorManager(sortAndFormats, numHits, searchAfter, 1);
                    }
                }
            }

            @Override
            public void onEarlyTermination(int maxCountHits, boolean forcedTermination) {
                terminatedAfter = maxCountHits;
            }

            @Override
            public Collector newCollector() throws IOException {
                MaxScoreCollector maxScoreCollector = null;

                if (sortAndFormats != null && trackMaxScore) {
                    maxScoreCollector = new MaxScoreCollector();
                }

                return MultiCollectorWrapper.wrap(manager.newCollector(), maxScoreCollector);
            }

            @SuppressWarnings("unchecked")
            @Override
            public ReduceableSearchResult reduce(Collection<Collector> collectors) throws IOException {
                final Collection<TopDocsCollector<?>> topDocsCollectors = new ArrayList<>();
                final Collection<MaxScoreCollector> maxScoreCollectors = new ArrayList<>();

                for (final Collector collector : collectors) {
                    if (collector instanceof MultiCollectorWrapper) {
                        for (final Collector sub : (((MultiCollectorWrapper) collector).getCollectors())) {
                            if (sub instanceof TopDocsCollector<?>) {
                                topDocsCollectors.add((TopDocsCollector<?>) sub);
                            } else if (sub instanceof MaxScoreCollector) {
                                maxScoreCollectors.add((MaxScoreCollector) sub);
                            }
                        }
                    } else if (collector instanceof TopDocsCollector<?>) {
                        topDocsCollectors.add((TopDocsCollector<?>) collector);
                    } else if (collector instanceof MaxScoreCollector) {
                        maxScoreCollectors.add((MaxScoreCollector) collector);
                    }
                }

                float maxScore = Float.NaN;
                for (final MaxScoreCollector collector : maxScoreCollectors) {
                    float score = collector.getMaxScore();
                    if (Float.isNaN(maxScore)) {
                        maxScore = score;
                    } else if (!Float.isNaN(score)) {
                        maxScore = Math.max(maxScore, score);
                    }
                }

                final TopDocs topDocs = ((CollectorManager<TopDocsCollector<?>, ? extends TopDocs>) manager).reduce(topDocsCollectors);
                return reduceWith(topDocs, maxScore, terminatedAfter);
            }
        }

        @Override
        CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
            assert in == null;
            return new SimpleTopDocsCollectorManager();
        }

        protected ReduceableSearchResult reduceWith(final TopDocs topDocs, final float maxScore, final Integer terminatedAfter) {
            return (QuerySearchResult result) -> {
                final TopDocsAndMaxScore topDocsAndMaxScore = newTopDocs(topDocs, maxScore, terminatedAfter);
                result.topDocs(topDocsAndMaxScore, sortAndFormats == null ? null : sortAndFormats.formats);
            };
        }

        @Override
        Collector create(Collector in) {
            assert in == null;
            return collector;
        }

        TopDocsAndMaxScore newTopDocs(final TopDocs topDocs, final float maxScore, final Integer terminatedAfter) {
            TotalHits totalHits = null;

            if ((sortAndFormats == null || SortField.FIELD_SCORE.equals(sortAndFormats.sort.getSort()[0])) && hasInfMaxScore) {
                totalHits = topDocs.totalHits;
            } else if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                // don't compute hit counts via the collector
                totalHits = new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            } else {
                if (hitCount == -1) {
                    totalHits = topDocs.totalHits;
                } else {
                    totalHits = new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO);
                }
            }

            // Since we cannot support early forced termination, we have to simulate it by
            // artificially reducing the number of total hits and doc scores.
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            if (terminatedAfter != null) {
                if (totalHits.value > terminatedAfter) {
                    totalHits = new TotalHits(terminatedAfter, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
                }

                if (scoreDocs != null && scoreDocs.length > terminatedAfter) {
                    scoreDocs = Arrays.copyOf(scoreDocs, terminatedAfter);
                }
            }

            final TopDocs newTopDocs;
            if (topDocs instanceof TopFieldDocs) {
                TopFieldDocs fieldDocs = (TopFieldDocs) topDocs;
                newTopDocs = new TopFieldDocs(totalHits, scoreDocs, fieldDocs.fields);
            } else {
                newTopDocs = new TopDocs(totalHits, scoreDocs);
            }

            if (Float.isNaN(maxScore) && newTopDocs.scoreDocs.length > 0 && sortAndFormats == null) {
                return new TopDocsAndMaxScore(newTopDocs, newTopDocs.scoreDocs[0].score);
            } else {
                return new TopDocsAndMaxScore(newTopDocs, maxScore);
            }
        }

        TopDocsAndMaxScore newTopDocs() {
            TopDocs in = topDocsSupplier.get();
            float maxScore = maxScoreSupplier.get();
            final TopDocs newTopDocs;
            if (in instanceof TopFieldDocs) {
                TopFieldDocs fieldDocs = (TopFieldDocs) in;
                newTopDocs = new TopFieldDocs(totalHitsSupplier.get(), fieldDocs.scoreDocs, fieldDocs.fields);
            } else {
                newTopDocs = new TopDocs(totalHitsSupplier.get(), in.scoreDocs);
            }
            return new TopDocsAndMaxScore(newTopDocs, maxScore);
        }

        @Override
        void postProcess(QuerySearchResult result) throws IOException {
            final TopDocsAndMaxScore topDocs = newTopDocs();
            result.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
        }
    }

    static class ScrollingTopDocsCollectorContext extends SimpleTopDocsCollectorContext {
        private final ScrollContext scrollContext;
        private final int numberOfShards;

        private ScrollingTopDocsCollectorContext(
            IndexReader reader,
            Query query,
            ScrollContext scrollContext,
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            boolean trackMaxScore,
            int numberOfShards,
            int trackTotalHitsUpTo,
            boolean hasFilterCollector
        ) throws IOException {
            super(
                reader,
                query,
                sortAndFormats,
                scrollContext.lastEmittedDoc,
                numHits,
                trackMaxScore,
                trackTotalHitsUpTo,
                hasFilterCollector
            );
            this.scrollContext = Objects.requireNonNull(scrollContext);
            this.numberOfShards = numberOfShards;
        }

        @Override
        protected ReduceableSearchResult reduceWith(final TopDocs topDocs, final float maxScore, final Integer terminatedAfter) {
            return (QuerySearchResult result) -> {
                final TopDocsAndMaxScore topDocsAndMaxScore = newTopDocs(topDocs, maxScore, terminatedAfter);

                if (scrollContext.totalHits == null) {
                    // first round
                    scrollContext.totalHits = topDocsAndMaxScore.topDocs.totalHits;
                    scrollContext.maxScore = topDocsAndMaxScore.maxScore;
                } else {
                    // subsequent round: the total number of hits and
                    // the maximum score were computed on the first round
                    topDocsAndMaxScore.topDocs.totalHits = scrollContext.totalHits;
                    topDocsAndMaxScore.maxScore = scrollContext.maxScore;
                }

                if (numberOfShards == 1) {
                    // if we fetch the document in the same roundtrip, we already know the last emitted doc
                    if (topDocsAndMaxScore.topDocs.scoreDocs.length > 0) {
                        // set the last emitted doc
                        scrollContext.lastEmittedDoc = topDocsAndMaxScore.topDocs.scoreDocs[topDocsAndMaxScore.topDocs.scoreDocs.length
                            - 1];
                    }
                }

                result.topDocs(topDocsAndMaxScore, sortAndFormats == null ? null : sortAndFormats.formats);
            };
        }

        @Override
        void postProcess(QuerySearchResult result) throws IOException {
            final TopDocsAndMaxScore topDocs = newTopDocs();
            if (scrollContext.totalHits == null) {
                // first round
                scrollContext.totalHits = topDocs.topDocs.totalHits;
                scrollContext.maxScore = topDocs.maxScore;
            } else {
                // subsequent round: the total number of hits and
                // the maximum score were computed on the first round
                topDocs.topDocs.totalHits = scrollContext.totalHits;
                topDocs.maxScore = scrollContext.maxScore;
            }
            if (numberOfShards == 1) {
                // if we fetch the document in the same roundtrip, we already know the last emitted doc
                if (topDocs.topDocs.scoreDocs.length > 0) {
                    // set the last emitted doc
                    scrollContext.lastEmittedDoc = topDocs.topDocs.scoreDocs[topDocs.topDocs.scoreDocs.length - 1];
                }
            }
            result.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
        }
    }

    /**
     * Returns query total hit count if the <code>query</code> is a {@link MatchAllDocsQuery}
     * or a {@link TermQuery} and the <code>reader</code> has no deletions,
     * -1 otherwise.
     */
    static int shortcutTotalHitCount(IndexReader reader, Query query) throws IOException {
        while (true) {
            // remove wrappers that don't matter for counts
            // this is necessary so that we don't only optimize match_all
            // queries but also match_all queries that are nested in
            // a constant_score query
            if (query instanceof ConstantScoreQuery) {
                query = ((ConstantScoreQuery) query).getQuery();
            } else if (query instanceof BoostQuery) {
                query = ((BoostQuery) query).getQuery();
            } else {
                break;
            }
        }
        if (query.getClass() == MatchAllDocsQuery.class) {
            return reader.numDocs();
        } else if (query.getClass() == TermQuery.class && reader.hasDeletions() == false) {
            final Term term = ((TermQuery) query).getTerm();
            int count = 0;
            for (LeafReaderContext context : reader.leaves()) {
                count += context.reader().docFreq(term);
            }
            return count;
        } else if (query.getClass() == DocValuesFieldExistsQuery.class && reader.hasDeletions() == false) {
            final String field = ((DocValuesFieldExistsQuery) query).getField();
            int count = 0;
            for (LeafReaderContext context : reader.leaves()) {
                FieldInfos fieldInfos = context.reader().getFieldInfos();
                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
                if (fieldInfo != null) {
                    if (fieldInfo.getPointIndexDimensionCount() > 0) {
                        PointValues points = context.reader().getPointValues(field);
                        if (points != null) {
                            count += points.getDocCount();
                        }
                    } else if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                        Terms terms = context.reader().terms(field);
                        if (terms != null) {
                            count += terms.getDocCount();
                        }
                    } else {
                        return -1; // no shortcut possible for fields that are not indexed
                    }
                }
            }
            return count;
        } else {
            return -1;
        }
    }

    /**
     * Creates a {@link TopDocsCollectorContext} from the provided <code>searchContext</code>.
     * @param hasFilterCollector True if the collector chain contains at least one collector that can filters document.
     */
    public static TopDocsCollectorContext createTopDocsCollectorContext(SearchContext searchContext, boolean hasFilterCollector)
        throws IOException {
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final Query query = searchContext.query();
        // top collectors don't like a size of 0
        final int totalNumDocs = Math.max(1, reader.numDocs());
        if (searchContext.size() == 0) {
            // no matter what the value of from is
            return new EmptyTopDocsCollectorContext(
                reader,
                query,
                searchContext.sort(),
                searchContext.trackTotalHitsUpTo(),
                hasFilterCollector
            );
        } else if (searchContext.scrollContext() != null) {
            // we can disable the tracking of total hits after the initial scroll query
            // since the total hits is preserved in the scroll context.
            int trackTotalHitsUpTo = searchContext.scrollContext().totalHits != null
                ? SearchContext.TRACK_TOTAL_HITS_DISABLED
                : SearchContext.TRACK_TOTAL_HITS_ACCURATE;
            // no matter what the value of from is
            int numDocs = Math.min(searchContext.size(), totalNumDocs);
            return new ScrollingTopDocsCollectorContext(
                reader,
                query,
                searchContext.scrollContext(),
                searchContext.sort(),
                numDocs,
                searchContext.trackScores(),
                searchContext.numberOfShards(),
                trackTotalHitsUpTo,
                hasFilterCollector
            );
        } else if (searchContext.collapse() != null) {
            boolean trackScores = searchContext.sort() == null ? true : searchContext.trackScores();
            int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
            return new CollapsingTopDocsCollectorContext(searchContext.collapse(), searchContext.sort(), numDocs, trackScores);
        } else {
            int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
            final boolean rescore = searchContext.rescore().isEmpty() == false;
            if (rescore) {
                assert searchContext.sort() == null;
                for (RescoreContext rescoreContext : searchContext.rescore()) {
                    numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
                }
            }
            return new SimpleTopDocsCollectorContext(
                reader,
                query,
                searchContext.sort(),
                searchContext.searchAfter(),
                numDocs,
                searchContext.trackScores(),
                searchContext.trackTotalHitsUpTo(),
                hasFilterCollector
            ) {
                @Override
                public boolean shouldRescore() {
                    return rescore;
                }
            };
        }
    }

    /**
     * Return true if the provided query contains a mandatory clauses (MUST)
     * that doesn't track the maximum scores per block
     */
    static boolean hasInfMaxScore(Query query) {
        MaxScoreQueryVisitor visitor = new MaxScoreQueryVisitor();
        query.visit(visitor);
        return visitor.hasInfMaxScore;
    }

    private static class MaxScoreQueryVisitor extends QueryVisitor {
        private boolean hasInfMaxScore;

        @Override
        public void visitLeaf(Query query) {
            checkMaxScoreInfo(query);
        }

        @Override
        public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            if (occur != BooleanClause.Occur.MUST) {
                // boolean queries can skip documents even if they have some should
                // clauses that don't track maximum scores
                return QueryVisitor.EMPTY_VISITOR;
            }
            checkMaxScoreInfo(parent);
            return this;
        }

        void checkMaxScoreInfo(Query query) {
            if (query instanceof FunctionScoreQuery || query instanceof ScriptScoreQuery || query instanceof SpanQuery) {
                hasInfMaxScore = true;
            } else if (query instanceof OpenSearchToParentBlockJoinQuery) {
                OpenSearchToParentBlockJoinQuery q = (OpenSearchToParentBlockJoinQuery) query;
                hasInfMaxScore |= (q.getScoreMode() != org.apache.lucene.search.join.ScoreMode.None);
            }
        }
    }
}
