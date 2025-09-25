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

package org.opensearch.search.internal;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.search.RescoreDocIds;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.fetch.subphase.ScriptFieldsContext;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestionSearchContext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class encapsulates the state needed to execute a search. It holds a reference to the
 * shards point in time snapshot (IndexReader / ContextIndexSearcher) and allows passing on
 * state from one query / fetch phase to another.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class SearchContext implements Releasable {

    public static final int DEFAULT_TERMINATE_AFTER = 0;
    public static final int TRACK_TOTAL_HITS_ACCURATE = Integer.MAX_VALUE;
    public static final int TRACK_TOTAL_HITS_DISABLED = -1;
    public static final int DEFAULT_TRACK_TOTAL_HITS_UP_TO = 10000;

    // no-op bucket collector processor
    public static final BucketCollectorProcessor NO_OP_BUCKET_COLLECTOR_PROCESSOR = new BucketCollectorProcessor() {
        @Override
        public void processPostCollection(Collector collectorTree) {
            // do nothing as there is no aggregation collector
        }

        @Override
        public List<Aggregator> toAggregators(Collection<Collector> collectors) {
            // should not be called when there is no aggregation collector
            throw new IllegalStateException("Unexpected toAggregators call on NO_OP_BUCKET_COLLECTOR_PROCESSOR");
        }

        @Override
        public List<InternalAggregation> toInternalAggregations(Collection<Collector> collectors) {
            // should not be called when there is no aggregation collector
            throw new IllegalStateException("Unexpected toInternalAggregations call on NO_OP_BUCKET_COLLECTOR_PROCESSOR");
        }
    };

    private final List<Releasable> releasables = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private InnerHitsContext innerHitsContext;
    private volatile boolean searchTimedOut;

    protected SearchContext() {}

    public abstract void setTask(SearchShardTask task);

    public abstract SearchShardTask getTask();

    public abstract boolean isCancelled();

    public boolean isSearchTimedOut() {
        return this.searchTimedOut;
    }

    public void setSearchTimedOut(boolean searchTimedOut) {
        this.searchTimedOut = searchTimedOut;
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                Releasables.close(releasables);
            } finally {
                doClose();
            }
        }
    }

    protected abstract void doClose();

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     * @param rewrite if the set query should be rewritten against the searcher returned from {@link #searcher()}
     */
    public abstract void preProcess(boolean rewrite);

    /** Automatically apply all required filters to the given query such as
     *  alias filters, types filters, etc. */
    public abstract Query buildFilteredQuery(Query query);

    public abstract ShardSearchContextId id();

    public abstract String source();

    public abstract ShardSearchRequest request();

    public abstract SearchType searchType();

    public abstract SearchShardTarget shardTarget();

    public abstract int numberOfShards();

    public abstract float queryBoost();

    public abstract ScrollContext scrollContext();

    public abstract SearchContextAggregations aggregations();

    public abstract SearchContext aggregations(SearchContextAggregations aggregations);

    public abstract void addSearchExt(SearchExtBuilder searchExtBuilder);

    public abstract SearchExtBuilder getSearchExt(String name);

    public abstract SearchHighlightContext highlight();

    public abstract void highlight(SearchHighlightContext highlight);

    public InnerHitsContext innerHits() {
        if (innerHitsContext == null) {
            innerHitsContext = new InnerHitsContext();
        }
        return innerHitsContext;
    }

    public abstract SuggestionSearchContext suggest();

    public abstract void suggest(SuggestionSearchContext suggest);

    /**
     * @return list of all rescore contexts.  empty if there aren't any.
     */
    public abstract List<RescoreContext> rescore();

    public abstract void addRescore(RescoreContext rescore);

    public final RescoreDocIds rescoreDocIds() {
        final List<RescoreContext> rescore = rescore();
        if (rescore == null) {
            return RescoreDocIds.EMPTY;
        }
        Map<Integer, Set<Integer>> rescoreDocIds = null;
        for (int i = 0; i < rescore.size(); i++) {
            final Set<Integer> docIds = rescore.get(i).getRescoredDocs();
            if (docIds != null && docIds.isEmpty() == false) {
                if (rescoreDocIds == null) {
                    rescoreDocIds = new HashMap<>();
                }
                rescoreDocIds.put(i, docIds);
            }
        }
        return rescoreDocIds == null ? RescoreDocIds.EMPTY : new RescoreDocIds(rescoreDocIds);
    }

    public final void assignRescoreDocIds(RescoreDocIds rescoreDocIds) {
        final List<RescoreContext> rescore = rescore();
        if (rescore != null) {
            for (int i = 0; i < rescore.size(); i++) {
                final Set<Integer> docIds = rescoreDocIds.getId(i);
                if (docIds != null) {
                    rescore.get(i).setRescoredDocs(docIds);
                }
            }
        }
    }

    public abstract boolean hasScriptFields();

    public abstract ScriptFieldsContext scriptFields();

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
    public abstract boolean sourceRequested();

    public abstract boolean hasFetchSourceContext();

    public abstract FetchSourceContext fetchSourceContext();

    public abstract SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext);

    public abstract FetchDocValuesContext docValuesContext();

    public abstract SearchContext docValuesContext(FetchDocValuesContext docValuesContext);

    /**
     * The context related to retrieving fields.
     */
    public abstract FetchFieldsContext fetchFieldsContext();

    /**
     * Sets the context related to retrieving fields.
     */
    public abstract SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext);

    public abstract ContextIndexSearcher searcher();

    public abstract IndexShard indexShard();

    public abstract MapperService mapperService();

    public abstract SimilarityService similarityService();

    public abstract BigArrays bigArrays();

    public abstract BitsetFilterCache bitsetFilterCache();

    public abstract TimeValue timeout();

    public abstract void timeout(TimeValue timeout);

    public abstract int terminateAfter();

    public abstract void terminateAfter(int terminateAfter);

    /**
     * Indicates if the current index should perform frequent low level search cancellation check.
     * <p>
     * Enabling low-level checks will make long running searches to react to the cancellation request faster. However,
     * since it will produce more cancellation checks it might slow the search performance down.
     */
    public abstract boolean lowLevelCancellation();

    public abstract SearchContext minimumScore(float minimumScore);

    public abstract Float minimumScore();

    public abstract SearchContext sort(SortAndFormats sort);

    public abstract SortAndFormats sort();

    public abstract SearchContext trackScores(boolean trackScores);

    public abstract boolean trackScores();

    /**
     * Determines whether named queries' scores should be included in the search results.
     * By default, this is set to return false, indicating that scores from named queries are not included.
     *
     * @param includeNamedQueriesScore true to include scores from named queries, false otherwise.
     */
    public SearchContext includeNamedQueriesScore(boolean includeNamedQueriesScore) {
        // Default implementation does nothing and returns this for chaining.
        // Implementations of SearchContext should override this method to actually store the value.
        return this;
    }

    /**
     * Checks if scores from named queries are included in the search results.
     *
     * @return true if scores from named queries are included, false otherwise.
     */
    public boolean includeNamedQueriesScore() {
        // Default implementation returns false.
        // Implementations of SearchContext should override this method to return the actual value.
        return false;
    }

    public abstract SearchContext trackTotalHitsUpTo(int trackTotalHits);

    /**
     * Indicates the total number of hits to count accurately.
     * Defaults to {@link #DEFAULT_TRACK_TOTAL_HITS_UP_TO}.
     */
    public abstract int trackTotalHitsUpTo();

    public abstract SearchContext searchAfter(FieldDoc searchAfter);

    public abstract FieldDoc searchAfter();

    public abstract SearchContext collapse(CollapseContext collapse);

    public abstract CollapseContext collapse();

    public abstract SearchContext parsedPostFilter(ParsedQuery postFilter);

    public abstract ParsedQuery parsedPostFilter();

    public abstract Query aliasFilter();

    public abstract SearchContext parsedQuery(ParsedQuery query);

    public abstract ParsedQuery parsedQuery();

    /**
     * The query to execute, might be rewritten.
     */
    public abstract Query query();

    public abstract int from();

    public abstract SearchContext from(int from);

    public abstract int size();

    public abstract SearchContext size(int size);

    public abstract boolean hasStoredFields();

    public abstract boolean hasStoredFieldsContext();

    /**
     * A shortcut function to see whether there is a storedFieldsContext and it says the fields are requested.
     */
    public abstract boolean storedFieldsRequested();

    public abstract StoredFieldsContext storedFieldsContext();

    public abstract SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext);

    public abstract boolean explain();

    public abstract void explain(boolean explain);

    @Nullable
    public abstract List<String> groupStats();

    public abstract void groupStats(List<String> groupStats);

    public abstract boolean version();

    public abstract void version(boolean version);

    /** indicates whether the sequence number and primary term of the last modification to each hit should be returned */
    public abstract boolean seqNoAndPrimaryTerm();

    /** controls whether the sequence number and primary term of the last modification to each hit should be returned */
    public abstract void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm);

    public abstract int[] docIdsToLoad();

    public abstract int docIdsToLoadFrom();

    public abstract int docIdsToLoadSize();

    public abstract SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize);

    public abstract DfsSearchResult dfsResult();

    public abstract QuerySearchResult queryResult();

    public abstract FetchPhase fetchPhase();

    public abstract FetchSearchResult fetchResult();

    /**
     * Return a handle over the profilers for the current search request, or {@code null} if profiling is not enabled.
     */
    public abstract Profilers getProfilers();

    /**
     * Returns concurrent segment search status for the search context
     */
    public boolean shouldUseConcurrentSearch() {
        return false;
    }

    /**
     * Returns local bucket count thresholds based on concurrent segment search status
     */
    public LocalBucketCountThresholds asLocalBucketCountThresholds(TermsAggregator.BucketCountThresholds bucketCountThresholds) {
        return new LocalBucketCountThresholds(
            shouldUseConcurrentSearch() ? 0 : bucketCountThresholds.getShardMinDocCount(),
            bucketCountThresholds.getShardSize()
        );
    }

    /**
     * Adds a releasable that will be freed when this context is closed.
     */
    public void addReleasable(Releasable releasable) {
        releasables.add(releasable);
    }

    /**
     * @return true if the request contains only suggest
     */
    public final boolean hasOnlySuggest() {
        return request().source() != null && request().source().isSuggestOnly();
    }

    /**
     * Given the full name of a field, returns its {@link MappedFieldType}.
     */
    public abstract MappedFieldType fieldType(String name);

    public abstract ObjectMapper getObjectMapper(String name);

    /**
     * Returns time in milliseconds that can be used for relative time calculations.
     * WARN: This is not the epoch time and can be a cached time.
     */
    public abstract long getRelativeTimeInMillis();

    /**
     * Returns time in milliseconds that can be used for relative time calculations. this method will fall back to
     * {@link SearchContext#getRelativeTimeInMillis()} (which might be a cached time) if useCache was set to true else it will be just be a
     * wrapper of {@link System#nanoTime()} converted to milliseconds.
     * @param useCache to allow using cached time if true or forcing calling {@link System#nanoTime()} if false
     * @return Returns time in milliseconds that can be used for relative time calculations.
     */
    public long getRelativeTimeInMillis(boolean useCache) {
        if (useCache) {
            return getRelativeTimeInMillis();
        }
        return TimeValue.nsecToMSec(System.nanoTime());
    }

    /** Return a view of the additional query collector managers that should be run for this context. */
    public abstract Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers();

    public abstract QueryShardContext getQueryShardContext();

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder().append(shardTarget());
        if (searchType() != SearchType.DEFAULT) {
            result.append("searchType=[").append(searchType()).append("]");
        }
        if (scrollContext() != null) {
            if (scrollContext().scroll != null) {
                result.append("scroll=[").append(scrollContext().scroll.keepAlive()).append("]");
            } else {
                result.append("scroll=[null]");
            }
        }
        result.append(" query=[").append(query()).append("]");
        return result.toString();
    }

    public abstract ReaderContext readerContext();

    public abstract InternalAggregation.ReduceContext partialOnShard();

    // processor used for bucket collectors
    public abstract void setBucketCollectorProcessor(BucketCollectorProcessor bucketCollectorProcessor);

    public abstract BucketCollectorProcessor bucketCollectorProcessor();

    public abstract int getTargetMaxSliceCount();

    public abstract boolean shouldUseTimeSeriesDescSortOptimization();

    public boolean getStarTreeIndexEnabled() {
        return false;
    }

    public int maxAggRewriteFilters() {
        return 0;
    }

    @ExperimentalApi
    public int filterRewriteSegmentThreshold() {
        return 0;
    }

    public int cardinalityAggregationPruningThreshold() {
        return 0;
    }

    public int bucketSelectionStrategyFactor() {
        return SearchService.DEFAULT_BUCKET_SELECTION_STRATEGY_FACTOR;
    }

    public boolean keywordIndexOrDocValuesEnabled() {
        return false;
    }

    @ExperimentalApi
    public void setStreamChannelListener(StreamSearchChannelListener<SearchPhaseResult, ShardSearchRequest> listener) {
        throw new IllegalStateException("Set search channel listener should be implemented for stream search");
    }

    @ExperimentalApi
    public StreamSearchChannelListener<SearchPhaseResult, ShardSearchRequest> getStreamChannelListener() {
        throw new IllegalStateException("Get search channel listener should be implemented for stream search");
    }

    @ExperimentalApi
    public boolean isStreamSearch() {
        return false;
    }

    public void setDFResults(Map<String, Object[]> dfResults) {}

    public Map<String, Object[]> getDFResults() {
        return Collections.emptyMap();
    }
}
