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

package org.opensearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.deciders.ConcurrentSearchDecision;
import org.opensearch.search.deciders.ConcurrentSearchRequestDecider;
import org.opensearch.search.deciders.ConcurrentSearchVisitor;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.ScriptFieldsContext;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QueryPhaseExecutionException;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.opensearch.search.SearchService.AGGREGATION_REWRITE_FILTER_SEGMENT_THRESHOLD;
import static org.opensearch.search.SearchService.BUCKET_SELECTION_STRATEGY_FACTOR_SETTING;
import static org.opensearch.search.SearchService.CARDINALITY_AGGREGATION_PRUNING_THRESHOLD;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
import static org.opensearch.search.SearchService.KEYWORD_INDEX_OR_DOC_VALUES_ENABLED;
import static org.opensearch.search.SearchService.MAX_AGGREGATION_REWRITE_FILTERS;

/**
 * The main search context used during search phase
 *
 * @opensearch.internal
 */
final class DefaultSearchContext extends SearchContext {

    private static final Logger logger = LogManager.getLogger(DefaultSearchContext.class);

    private final ReaderContext readerContext;
    private final Engine.Searcher engineSearcher;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final LongSupplier relativeTimeSupplier;
    private final Collection<ConcurrentSearchRequestDecider.Factory> concurrentSearchDeciderFactories;
    private SearchType searchType;
    private final BigArrays bigArrays;
    private final IndexShard indexShard;
    private final ClusterService clusterService;
    private final IndexService indexService;
    private final ContextIndexSearcher searcher;
    private final DfsSearchResult dfsResult;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;
    private final float queryBoost;
    private final boolean lowLevelCancellation;
    private TimeValue timeout;
    // terminate after count
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private List<String> groupStats;
    private boolean explain;
    private boolean version = false; // by default, we don't return versions
    private boolean seqAndPrimaryTerm = false;
    private StoredFieldsContext storedFields;
    private ScriptFieldsContext scriptFields;
    private FetchSourceContext fetchSourceContext;
    private FetchDocValuesContext docValuesContext;
    private FetchFieldsContext fetchFieldsContext;
    private int from = -1;
    private int size = -1;
    private SortAndFormats sort;
    private Float minimumScore;
    private boolean trackScores = false; // when sorting, track scores as well...

    private boolean includeNamedQueriesScore = false;
    private int trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;
    private FieldDoc searchAfter;
    private CollapseContext collapse;
    // filter for sliced scroll
    private SliceBuilder sliceBuilder;
    private SearchShardTask task;
    private final Version minNodeVersion;

    /**
     * The original query as sent by the user without the types and aliases
     * applied. Putting things in here leaks them into highlighting so don't add
     * things like the type filter or alias filters.
     */
    private ParsedQuery originalQuery;

    /**
     * The query to actually execute.
     */
    private Query query;
    private ParsedQuery postFilter;
    private Query aliasFilter;
    private int[] docIdsToLoad;
    private int docsIdsToLoadFrom;
    private int docsIdsToLoadSize;
    private SearchContextAggregations aggregations;
    private SearchHighlightContext highlight;
    private SuggestionSearchContext suggest;
    private List<RescoreContext> rescore;
    private Profilers profilers;
    private BucketCollectorProcessor bucketCollectorProcessor = NO_OP_BUCKET_COLLECTOR_PROCESSOR;
    private final Map<String, SearchExtBuilder> searchExtBuilders = new HashMap<>();
    private final Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers = new HashMap<>();
    private final QueryShardContext queryShardContext;
    private final FetchPhase fetchPhase;
    private final Function<SearchSourceBuilder, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder;
    private final String concurrentSearchMode;
    private final SetOnce<Boolean> requestShouldUseConcurrentSearch = new SetOnce<>();
    private final int maxAggRewriteFilters;
    private final int filterRewriteSegmentThreshold;
    private final int cardinalityAggregationPruningThreshold;
    private final int bucketSelectionStrategyFactor;
    private final boolean keywordIndexOrDocValuesEnabled;

    private final boolean isStreamSearch;
    private StreamSearchChannelListener listener;
    private Map<String, Object[]> dfResults;

    DefaultSearchContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        ClusterService clusterService,
        BigArrays bigArrays,
        LongSupplier relativeTimeSupplier,
        TimeValue timeout,
        FetchPhase fetchPhase,
        boolean lowLevelCancellation,
        Version minNodeVersion,
        boolean validate,
        Executor executor,
        Function<SearchSourceBuilder, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder,
        Collection<ConcurrentSearchRequestDecider.Factory> concurrentSearchDeciderFactories,
        boolean isStreamSearch
    ) throws IOException {
        this.readerContext = readerContext;
        this.request = request;
        this.fetchPhase = fetchPhase;
        this.searchType = request.searchType();
        this.shardTarget = shardTarget;
        // SearchContexts use a BigArrays that can circuit break
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.dfsResult = new DfsSearchResult(readerContext.id(), shardTarget, request);
        this.queryResult = new QuerySearchResult(readerContext.id(), shardTarget, request);
        this.fetchResult = new FetchSearchResult(readerContext.id(), shardTarget);
        this.indexService = readerContext.indexService();
        this.indexShard = readerContext.indexShard();
        this.clusterService = clusterService;
        this.engineSearcher = readerContext.acquireSearcher("search");
        this.concurrentSearchMode = evaluateConcurrentSearchMode(executor);
        this.searcher = new ContextIndexSearcher(
            engineSearcher.getIndexReader(),
            engineSearcher.getSimilarity(),
            engineSearcher.getQueryCache(),
            engineSearcher.getQueryCachingPolicy(),
            lowLevelCancellation,
            concurrentSearchMode.equals(CONCURRENT_SEGMENT_SEARCH_MODE_AUTO)
                || concurrentSearchMode.equals(CONCURRENT_SEGMENT_SEARCH_MODE_ALL) ? executor : null,
            this
        );
        this.relativeTimeSupplier = relativeTimeSupplier;
        this.timeout = timeout;
        this.minNodeVersion = minNodeVersion;
        queryShardContext = indexService.newQueryShardContext(
            request.shardId().id(),
            this.searcher,
            request::nowInMillis,
            shardTarget.getClusterAlias(),
            validate,
            evaluateKeywordIndexOrDocValuesEnabled()
        );
        queryBoost = request.indexBoost();
        this.lowLevelCancellation = lowLevelCancellation;
        this.requestToAggReduceContextBuilder = requestToAggReduceContextBuilder;

        this.maxAggRewriteFilters = evaluateFilterRewriteSetting();
        this.filterRewriteSegmentThreshold = evaluateAggRewriteFilterSegThreshold();
        this.cardinalityAggregationPruningThreshold = evaluateCardinalityAggregationPruningThreshold();
        this.bucketSelectionStrategyFactor = evaluateBucketSelectionStrategyFactor();
        this.concurrentSearchDeciderFactories = concurrentSearchDeciderFactories;
        this.keywordIndexOrDocValuesEnabled = evaluateKeywordIndexOrDocValuesEnabled();
        this.isStreamSearch = isStreamSearch;
    }

    DefaultSearchContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        ClusterService clusterService,
        BigArrays bigArrays,
        LongSupplier relativeTimeSupplier,
        TimeValue timeout,
        FetchPhase fetchPhase,
        boolean lowLevelCancellation,
        Version minNodeVersion,
        boolean validate,
        Executor executor,
        Function<SearchSourceBuilder, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder,
        Collection<ConcurrentSearchRequestDecider.Factory> concurrentSearchDeciderFactories
    ) throws IOException {
        this(
            readerContext,
            request,
            shardTarget,
            clusterService,
            bigArrays,
            relativeTimeSupplier,
            timeout,
            fetchPhase,
            lowLevelCancellation,
            minNodeVersion,
            validate,
            executor,
            requestToAggReduceContextBuilder,
            concurrentSearchDeciderFactories,
            false
        );
    }

    @Override
    public void doClose() {
        Releasables.close(engineSearcher, searcher);
    }

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    @Override
    public void preProcess(boolean rewrite) {
        if (hasOnlySuggest()) {
            return;
        }
        long from = from() == -1 ? 0 : from();
        long size = size() == -1 ? 10 : size();
        long resultWindow = from + size;
        int maxResultWindow = indexService.getIndexSettings().getMaxResultWindow();

        if (resultWindow > maxResultWindow) {
            if (scrollContext() == null) {
                throw new IllegalArgumentException(
                    "Result window is too large, from + size must be less than or equal to: ["
                        + maxResultWindow
                        + "] but was ["
                        + resultWindow
                        + "]. See the scroll api for a more efficient way to request large data sets. "
                        + "This limit can be set by changing the ["
                        + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                        + "] index level setting."
                );
            }
            throw new IllegalArgumentException(
                "Batch size is too large, size must be less than or equal to: ["
                    + maxResultWindow
                    + "] but was ["
                    + resultWindow
                    + "]. Scroll batch sizes cost as much memory as result windows so they are controlled by the ["
                    + IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey()
                    + "] index level setting."
            );
        }
        if (rescore != null) {
            if (sort != null) {
                throw new IllegalArgumentException("Cannot use [sort] option in conjunction with [rescore].");
            }
            int maxWindow = indexService.getIndexSettings().getMaxRescoreWindow();
            for (RescoreContext rescoreContext : rescore()) {
                if (rescoreContext.getWindowSize() > maxWindow) {
                    throw new IllegalArgumentException(
                        "Rescore window ["
                            + rescoreContext.getWindowSize()
                            + "] is too large. "
                            + "It must be less than ["
                            + maxWindow
                            + "]. This prevents allocating massive heaps for storing the results "
                            + "to be rescored. This limit can be set by changing the ["
                            + IndexSettings.MAX_RESCORE_WINDOW_SETTING.getKey()
                            + "] index level setting."
                    );
                }
            }
        }

        if (sliceBuilder != null && scrollContext() != null) {
            int sliceLimit = indexService.getIndexSettings().getMaxSlicesPerScroll();
            int numSlices = sliceBuilder.getMax();
            if (numSlices > sliceLimit) {
                throw new IllegalArgumentException(
                    "The number of slices ["
                        + numSlices
                        + "] is too large. It must "
                        + "be less than ["
                        + sliceLimit
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SLICES_PER_SCROLL.getKey()
                        + "] index level setting."
                );
            }
        }

        if (sliceBuilder != null && readerContext != null && readerContext instanceof PitReaderContext) {
            int sliceLimit = indexService.getIndexSettings().getMaxSlicesPerPit();
            int numSlices = sliceBuilder.getMax();
            if (numSlices > sliceLimit) {
                throw new OpenSearchRejectedExecutionException(
                    "The number of slices ["
                        + numSlices
                        + "] is too large. It must "
                        + "be less than ["
                        + sliceLimit
                        + "]. This limit can be set by changing the ["
                        + IndexSettings.MAX_SLICES_PER_PIT.getKey()
                        + "] index level setting."
                );
            }
        }
        // initialize the filtering alias based on the provided filters
        try {
            final QueryBuilder queryBuilder = request.getAliasFilter().getQueryBuilder();
            aliasFilter = queryBuilder == null ? null : queryBuilder.toQuery(queryShardContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (query() == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            parsedQuery(new ParsedQuery(new BoostQuery(query(), queryBoost), parsedQuery()));
        }
        this.query = buildFilteredQuery(query);
        if (rewrite) {
            try {
                this.query = searcher.rewrite(query);
            } catch (IOException e) {
                throw new QueryPhaseExecutionException(shardTarget, "Failed to rewrite main query", e);
            }
        }
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        List<Query> filters = new ArrayList<>();
        if (mapperService().hasNested()
            && new NestedHelper(mapperService()).mightMatchNestedDocs(query)
            && (aliasFilter == null || new NestedHelper(mapperService()).mightMatchNestedDocs(aliasFilter))) {
            filters.add(Queries.newNonNestedFilter());
        }

        if (aliasFilter != null) {
            filters.add(aliasFilter);
        }

        if (sliceBuilder != null) {
            Query slicedQuery = sliceBuilder.toFilter(clusterService, request, queryShardContext, minNodeVersion);
            if (slicedQuery instanceof MatchNoDocsQuery) {
                return slicedQuery;
            } else {
                filters.add(slicedQuery);
            }
        }

        if (filters.isEmpty()) {
            return query;
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(query, Occur.MUST);
            for (Query filter : filters) {
                builder.add(filter, Occur.FILTER);
            }
            return builder.build();
        }
    }

    @Override
    public ShardSearchContextId id() {
        return readerContext.id();
    }

    @Override
    public String source() {
        return "search";
    }

    @Override
    public ShardSearchRequest request() {
        return this.request;
    }

    @Override
    public SearchType searchType() {
        return this.searchType;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return this.shardTarget;
    }

    @Override
    public int numberOfShards() {
        return request.numberOfShards();
    }

    @Override
    public float queryBoost() {
        return queryBoost;
    }

    @Override
    public ScrollContext scrollContext() {
        return readerContext.scrollContext();
    }

    @Override
    public SearchContextAggregations aggregations() {
        return aggregations;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        this.aggregations = aggregations;
        return this;
    }

    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        // it's ok to use the writeable name here given that we enforce it to be the same as the name of the element that gets
        // parsed by the corresponding parser. There is one single name and one single way to retrieve the parsed object from the context.
        searchExtBuilders.put(searchExtBuilder.getWriteableName(), searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return searchExtBuilders.get(name);
    }

    @Override
    public SearchHighlightContext highlight() {
        return highlight;
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {
        this.highlight = highlight;
    }

    @Override
    public SuggestionSearchContext suggest() {
        return suggest;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        this.suggest = suggest;
    }

    @Override
    public List<RescoreContext> rescore() {
        if (rescore == null) {
            return Collections.emptyList();
        }
        return rescore;
    }

    @Override
    public void addRescore(RescoreContext rescore) {
        if (this.rescore == null) {
            this.rescore = new ArrayList<>();
        }
        this.rescore.add(rescore);
    }

    @Override
    public boolean hasScriptFields() {
        return scriptFields != null;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        if (scriptFields == null) {
            scriptFields = new ScriptFieldsContext();
        }
        return this.scriptFields;
    }

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
    @Override
    public boolean sourceRequested() {
        return fetchSourceContext != null && fetchSourceContext.fetchSource();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return fetchSourceContext != null;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return this.fetchSourceContext;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        return docValuesContext;
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        this.docValuesContext = docValuesContext;
        return this;
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        return fetchFieldsContext;
    }

    @Override
    public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        this.fetchFieldsContext = fetchFieldsContext;
        return this;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    @Override
    public IndexShard indexShard() {
        return this.indexShard;
    }

    @Override
    public MapperService mapperService() {
        return indexService.mapperService();
    }

    @Override
    public SimilarityService similarityService() {
        return indexService.similarityService();
    }

    @Override
    public BigArrays bigArrays() {
        return bigArrays;
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return indexService.cache().bitsetFilterCache();
    }

    @Override
    public TimeValue timeout() {
        return timeout;
    }

    @Override
    public void timeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    @Override
    public int terminateAfter() {
        return terminateAfter;
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        this.terminateAfter = terminateAfter;
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        this.minimumScore = minimumScore;
        return this;
    }

    @Override
    public Float minimumScore() {
        return this.minimumScore;
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public SortAndFormats sort() {
        return this.sort;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    @Override
    public boolean trackScores() {
        return this.trackScores;
    }

    @Override
    public SearchContext includeNamedQueriesScore(boolean includeNamedQueriesScore) {
        this.includeNamedQueriesScore = includeNamedQueriesScore;
        return this;
    }

    @Override
    public boolean includeNamedQueriesScore() {
        return includeNamedQueriesScore;
    }

    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHitsUpTo) {
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        return this;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return trackTotalHitsUpTo;
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        this.searchAfter = searchAfter;
        return this;
    }

    @Override
    public boolean lowLevelCancellation() {
        return lowLevelCancellation;
    }

    @Override
    public FieldDoc searchAfter() {
        return searchAfter;
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        this.collapse = collapse;
        return this;
    }

    @Override
    public CollapseContext collapse() {
        return collapse;
    }

    public SearchContext sliceBuilder(SliceBuilder sliceBuilder) {
        this.sliceBuilder = sliceBuilder;
        return this;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return this.postFilter;
    }

    @Override
    public Query aliasFilter() {
        return aliasFilter;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

    /**
     * The query to execute, in its rewritten form.
     */
    @Override
    public Query query() {
        return this.query;
    }

    @Override
    public int from() {
        return from;
    }

    @Override
    public SearchContext from(int from) {
        this.from = from;
        return this;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public SearchContext size(int size) {
        this.size = size;
        return this;
    }

    @Override
    public boolean hasStoredFields() {
        return storedFields != null && storedFields.fieldNames() != null;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return storedFields != null;
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return storedFields;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        this.storedFields = storedFieldsContext;
        return this;
    }

    @Override
    public boolean storedFieldsRequested() {
        return storedFields == null || storedFields.fetchFields();
    }

    @Override
    public boolean explain() {
        return explain;
    }

    @Override
    public void explain(boolean explain) {
        this.explain = explain;
    }

    @Override
    @Nullable
    public List<String> groupStats() {
        return this.groupStats;
    }

    @Override
    public void groupStats(List<String> groupStats) {
        this.groupStats = groupStats;
    }

    @Override
    public boolean version() {
        return version;
    }

    @Override
    public void version(boolean version) {
        this.version = version;
    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return seqAndPrimaryTerm;
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        this.seqAndPrimaryTerm = seqNoAndPrimaryTerm;
    }

    @Override
    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    @Override
    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    @Override
    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    @Override
    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchPhase fetchPhase() {
        return fetchPhase;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    @Override
    public MappedFieldType fieldType(String name) {
        return mapperService().fieldType(name);
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        return mapperService().getObjectMapper(name);
    }

    @Override
    public long getRelativeTimeInMillis() {
        return relativeTimeSupplier.getAsLong();
    }

    @Override
    public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers() {
        return queryCollectorManagers;
    }

    @Override
    public QueryShardContext getQueryShardContext() {
        return queryShardContext;
    }

    @Override
    public Profilers getProfilers() {
        return profilers;
    }

    /**
     * Returns concurrent segment search status for the search context. This should only be used after request parsing, during which requestShouldUseConcurrentSearch will be set.
     */
    @Override
    public boolean shouldUseConcurrentSearch() {
        assert requestShouldUseConcurrentSearch.get() != null : "requestShouldUseConcurrentSearch must be set";
        assert concurrentSearchMode != null : "concurrentSearchMode must be set";
        return (concurrentSearchMode.equals(CONCURRENT_SEGMENT_SEARCH_MODE_AUTO)
            || concurrentSearchMode.equals(CONCURRENT_SEGMENT_SEARCH_MODE_ALL))
            && Boolean.TRUE.equals(requestShouldUseConcurrentSearch.get());
    }

    private boolean evaluateAutoMode() {

        final Set<ConcurrentSearchRequestDecider> concurrentSearchRequestDeciders = new HashSet<>();

        // create the ConcurrentSearchRequestDeciders using registered factories
        for (ConcurrentSearchRequestDecider.Factory deciderFactory : concurrentSearchDeciderFactories) {
            final Optional<ConcurrentSearchRequestDecider> concurrentSearchRequestDecider = deciderFactory.create(
                indexService.getIndexSettings()
            );
            concurrentSearchRequestDecider.ifPresent(concurrentSearchRequestDeciders::add);

        }

        // evaluate based on concurrent search query visitor
        if (concurrentSearchRequestDeciders.size() > 0) {
            ConcurrentSearchVisitor concurrentSearchVisitor = new ConcurrentSearchVisitor(
                concurrentSearchRequestDeciders,
                indexService.getIndexSettings()
            );
            if (request().source() != null && request().source().query() != null) {
                QueryBuilder queryBuilder = request().source().query();
                queryBuilder.visit(concurrentSearchVisitor);
            }
        }

        final List<ConcurrentSearchDecision> decisions = new ArrayList<>();
        for (ConcurrentSearchRequestDecider decider : concurrentSearchRequestDeciders) {
            ConcurrentSearchDecision decision = decider.getConcurrentSearchDecision();
            if (decision != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("concurrent search decision from plugin decider [{}]", decision.toString());
                }
                decisions.add(decision);
            }
        }

        final ConcurrentSearchDecision pluginDecision = ConcurrentSearchDecision.getCompositeDecision(decisions);
        if (pluginDecision.getDecisionStatus().equals(ConcurrentSearchDecision.DecisionStatus.NO_OP)) {
            // plugins don't have preference, decide based on whether request has aggregations or not.
            if (aggregations() != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("request has supported aggregations, using concurrent search");
                }
                return true;

            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("request does not have aggregations, not using concurrent search");
                }
                return false;
            }

        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("concurrent search decision from plugins [{}]", pluginDecision.toString());
            }
            return pluginDecision.getDecisionStatus() == ConcurrentSearchDecision.DecisionStatus.YES;
        }

    }

    /**
     * Evaluate if request should use concurrent search based on request and concurrent search deciders
     */
    public void evaluateRequestShouldUseConcurrentSearch() {
        if (sort != null && sort.isSortOnTimeSeriesField()) {
            requestShouldUseConcurrentSearch.set(false);
        } else if (aggregations() != null
            && aggregations().factories() != null
            && !aggregations().factories().allFactoriesSupportConcurrentSearch()) {
                requestShouldUseConcurrentSearch.set(false);
            } else if (terminateAfter != DEFAULT_TERMINATE_AFTER) {
                requestShouldUseConcurrentSearch.set(false);
            } else if (concurrentSearchMode.equals(CONCURRENT_SEGMENT_SEARCH_MODE_AUTO)) {
                requestShouldUseConcurrentSearch.set(evaluateAutoMode());
            } else {
                requestShouldUseConcurrentSearch.set(true);
            }
    }

    public void setProfilers(Profilers profilers) {
        this.profilers = profilers;
    }

    @Override
    public void setTask(SearchShardTask task) {
        this.task = task;
    }

    @Override
    public SearchShardTask getTask() {
        return task;
    }

    @Override
    public boolean isCancelled() {
        return task.isCancelled();
    }

    @Override
    public ReaderContext readerContext() {
        return readerContext;
    }

    @Override
    public InternalAggregation.ReduceContext partialOnShard() {
        InternalAggregation.ReduceContext rc = requestToAggReduceContextBuilder.apply(request.source()).forPartialReduction();
        rc.setSliceLevel(shouldUseConcurrentSearch());
        return rc;
    }

    @Override
    public void setBucketCollectorProcessor(BucketCollectorProcessor bucketCollectorProcessor) {
        this.bucketCollectorProcessor = bucketCollectorProcessor;
    }

    @Override
    public BucketCollectorProcessor bucketCollectorProcessor() {
        return bucketCollectorProcessor;
    }

    /**
     * Determines the appropriate concurrent segment search mode for the current search request.
     * <p>
     * This method evaluates both index-level and cluster-level settings to decide whether
     * concurrent segment search should be enabled. The resolution logic is as follows:
     * <ol>
     *     <li>If the request targets a system index or uses search throttling, concurrent segment search is disabled.</li>
     *     <li>If a legacy boolean setting is present (cluster or index level), it is honored:
     *         <ul>
     *             <li><code>true</code> → enables concurrent segment search ("all")</li>
     *             <li><code>false</code> → disables it ("none")</li>
     *         </ul>
     *     </li>
     *     <li>Otherwise, the modern string-based setting is used. Allowed values are: "none", "auto", or "all".</li>
     * </ol>
     *
     * @param concurrentSearchExecutor the executor used for concurrent segment search; if null, disables the feature
     * @return the resolved concurrent segment search mode: "none", "auto", or "all"
     */
    private String evaluateConcurrentSearchMode(Executor concurrentSearchExecutor) {
        // Skip concurrent search for system indices, throttled requests, or if dependencies are missing
        if (indexShard.isSystem()
            || indexShard.indexSettings().isSearchThrottled()
            || clusterService == null
            || concurrentSearchExecutor == null) {
            return CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
        }

        Settings indexSettings = indexService.getIndexSettings().getSettings();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        final String newConcurrentMode = indexSettings.get(
            IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(),
            clusterSettings.getOrNull(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE)
        );

        // Step 1: New concurrent mode is explicitly set → use it
        if (newConcurrentMode != null) return newConcurrentMode;

        final Boolean legacySetting = indexSettings.getAsBoolean(
            IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(),
            clusterSettings.getOrNull(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );

        // Step 2: If new concurrent setting is not explicitly set
        // then check if Legacy boolean setting is explicitly set → use it
        if (legacySetting != null) {
            return Boolean.TRUE.equals(legacySetting) ? CONCURRENT_SEGMENT_SEARCH_MODE_ALL : CONCURRENT_SEGMENT_SEARCH_MODE_NONE;
        }

        // Step 3: Neither explicitly set → use default concurrent mode
        return indexSettings.get(
            IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(),
            clusterSettings.get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE)
        );
    }

    /**
     * Returns the target maximum slice count to use for concurrent segment search.
     *
     * If concurrent segment search is disabled (either due to system index, throttled search,
     * missing executor, or explicitly disabled settings), then we return a slice count of 1.
     * This effectively disables concurrent slicing and ensures that the search is performed
     * in a single-threaded manner.
     *
     * Otherwise, fetch the configured slice count from index or cluster-level settings.
     *
     * @return number of slices to use for concurrent segment search; returns 1 if concurrent search is disabled.
     */
    @Override
    public int getTargetMaxSliceCount() {
        if (shouldUseConcurrentSearch() == false) {
            return 1; // Disable slicing: run search in a single thread when concurrent search is off
        }

        return indexService.getIndexSettings()
            .getSettings()
            .getAsInt(
                IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT.getKey(),
                clusterService.getClusterSettings().get(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING)
            );

    }

    @Override
    public boolean shouldUseTimeSeriesDescSortOptimization() {
        return indexShard.isTimeSeriesDescSortOptimizationEnabled()
            && sort != null
            && sort.isSortOnTimeSeriesField()
            && sort.sort.getSort()[0].getReverse() == false;
    }

    @Override
    public boolean getStarTreeIndexEnabled() {
        return indexService.getIndexSettings()
            .getSettings()
            .getAsBoolean(
                StarTreeIndexSettings.STAR_TREE_SEARCH_ENABLED_SETTING.getKey(),
                clusterService.getClusterSettings().get(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING)
            );
    }

    @Override
    public int maxAggRewriteFilters() {
        return maxAggRewriteFilters;
    }

    private int evaluateFilterRewriteSetting() {
        if (clusterService != null) {
            return clusterService.getClusterSettings().get(MAX_AGGREGATION_REWRITE_FILTERS);
        }
        return 0;
    }

    @Override
    public int filterRewriteSegmentThreshold() {
        return filterRewriteSegmentThreshold;
    }

    private int evaluateAggRewriteFilterSegThreshold() {
        if (clusterService != null) {
            return clusterService.getClusterSettings().get(AGGREGATION_REWRITE_FILTER_SEGMENT_THRESHOLD);
        }
        return 0;
    }

    @Override
    public int cardinalityAggregationPruningThreshold() {
        return cardinalityAggregationPruningThreshold;
    }

    @Override
    public int bucketSelectionStrategyFactor() {
        return bucketSelectionStrategyFactor;
    }

    @Override
    public boolean keywordIndexOrDocValuesEnabled() {
        return keywordIndexOrDocValuesEnabled;
    }

    private int evaluateCardinalityAggregationPruningThreshold() {
        if (clusterService != null) {
            return clusterService.getClusterSettings().get(CARDINALITY_AGGREGATION_PRUNING_THRESHOLD);
        }
        return 0;
    }

    private int evaluateBucketSelectionStrategyFactor() {
        if (clusterService != null) {
            return clusterService.getClusterSettings().get(BUCKET_SELECTION_STRATEGY_FACTOR_SETTING);
        }
        return SearchService.DEFAULT_BUCKET_SELECTION_STRATEGY_FACTOR;
    }

    public boolean evaluateKeywordIndexOrDocValuesEnabled() {
        if (clusterService != null) {
            return clusterService.getClusterSettings().get(KEYWORD_INDEX_OR_DOC_VALUES_ENABLED);
        }
        return false;
    }

    public void setStreamChannelListener(StreamSearchChannelListener listener) {
        assert isStreamSearch() : "Stream search not enabled";
        this.listener = listener;
    }

    public StreamSearchChannelListener getStreamChannelListener() {
        assert isStreamSearch() : "Stream search not enabled";
        return listener;
    }

    public boolean isStreamSearch() {
        return isStreamSearch;
    }

    public void setDFResults(Map<String, Object[]> dfResults) {
        this.dfResults = dfResults;
    }

    public Map<String, Object[]> getDFResults() {
        return dfResults;
    }
}
