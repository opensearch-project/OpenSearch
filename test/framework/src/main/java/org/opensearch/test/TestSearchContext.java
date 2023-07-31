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

package org.opensearch.test;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.IndexService;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.collapse.CollapseContext;
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
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestionSearchContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSearchContext extends SearchContext {
    public static final SearchShardTarget SHARD_TARGET = new SearchShardTarget(
        "test",
        new ShardId("test", "test", 0),
        null,
        OriginalIndices.NONE
    );

    final BigArrays bigArrays;
    final IndexService indexService;
    final BitsetFilterCache fixedBitSetFilterCache;
    final Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers = new HashMap<>();
    final IndexShard indexShard;
    final QuerySearchResult queryResult = new QuerySearchResult();
    final QueryShardContext queryShardContext;
    ParsedQuery originalQuery;
    ParsedQuery postFilter;
    Query query;
    Float minScore;
    SearchShardTask task;
    SortAndFormats sort;
    boolean trackScores = false;
    int trackTotalHitsUpTo = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO;

    ContextIndexSearcher searcher;
    int from;
    int size;
    private int terminateAfter = DEFAULT_TERMINATE_AFTER;
    private SearchContextAggregations aggregations;
    private ScrollContext scrollContext;
    private FieldDoc searchAfter;
    private Profilers profilers;
    private CollapseContext collapse;
    protected boolean concurrentSegmentSearchEnabled;
    private BucketCollectorProcessor bucketCollectorProcessor = NO_OP_BUCKET_COLLECTOR_PROCESSOR;

    /**
     * Sets the concurrent segment search enabled field
     */
    public void setConcurrentSegmentSearchEnabled(boolean concurrentSegmentSearchEnabled) {
        this.concurrentSegmentSearchEnabled = concurrentSegmentSearchEnabled;
    }

    private final Map<String, SearchExtBuilder> searchExtBuilders = new HashMap<>();

    public TestSearchContext(BigArrays bigArrays, IndexService indexService) {
        this.bigArrays = bigArrays.withCircuitBreaking();
        this.indexService = indexService;
        this.fixedBitSetFilterCache = indexService.cache().bitsetFilterCache();
        this.indexShard = indexService.getShardOrNull(0);
        queryShardContext = indexService.newQueryShardContext(0, null, () -> 0L, null);
    }

    public TestSearchContext(QueryShardContext queryShardContext) {
        this(queryShardContext, null);
    }

    public TestSearchContext(QueryShardContext queryShardContext, IndexShard indexShard) {
        this(queryShardContext, indexShard, null);
    }

    public TestSearchContext(QueryShardContext queryShardContext, IndexShard indexShard, ContextIndexSearcher searcher) {
        this(queryShardContext, indexShard, searcher, null);
    }

    public TestSearchContext(
        QueryShardContext queryShardContext,
        IndexShard indexShard,
        ContextIndexSearcher searcher,
        ScrollContext scrollContext
    ) {
        this.bigArrays = null;
        this.indexService = null;
        this.fixedBitSetFilterCache = null;
        this.indexShard = indexShard;
        this.queryShardContext = queryShardContext;
        this.searcher = searcher;
        this.concurrentSegmentSearchEnabled = searcher != null && (searcher.getExecutor() != null);
        this.scrollContext = scrollContext;
    }

    public void setSearcher(ContextIndexSearcher searcher) {
        this.searcher = searcher;
    }

    @Override
    public void preProcess(boolean rewrite) {}

    @Override
    public Query buildFilteredQuery(Query query) {
        return null;
    }

    @Override
    public ShardSearchContextId id() {
        return new ShardSearchContextId("", 0);
    }

    @Override
    public String source() {
        return null;
    }

    @Override
    public ShardSearchRequest request() {
        return null;
    }

    @Override
    public SearchType searchType() {
        return null;
    }

    @Override
    public SearchShardTarget shardTarget() {
        return null;
    }

    @Override
    public int numberOfShards() {
        return 1;
    }

    @Override
    public float queryBoost() {
        return 0;
    }

    @Override
    public ScrollContext scrollContext() {
        return scrollContext;
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
        searchExtBuilders.put(searchExtBuilder.getWriteableName(), searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return searchExtBuilders.get(name);
    }

    @Override
    public SearchHighlightContext highlight() {
        return null;
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {}

    @Override
    public SuggestionSearchContext suggest() {
        return null;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {}

    @Override
    public List<RescoreContext> rescore() {
        return Collections.emptyList();
    }

    @Override
    public boolean hasScriptFields() {
        return false;
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        return null;
    }

    @Override
    public boolean sourceRequested() {
        return false;
    }

    @Override
    public boolean hasFetchSourceContext() {
        return false;
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return null;
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        return null;
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        return null;
    }

    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        return null;
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        return null;
    }

    @Override
    public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
        return null;
    }

    @Override
    public ContextIndexSearcher searcher() {
        return searcher;
    }

    @Override
    public IndexShard indexShard() {
        return indexShard;
    }

    @Override
    public MapperService mapperService() {
        if (indexService != null) {
            return indexService.mapperService();
        }
        return null;
    }

    @Override
    public SimilarityService similarityService() {
        return null;
    }

    @Override
    public BigArrays bigArrays() {
        return bigArrays;
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return fixedBitSetFilterCache;
    }

    @Override
    public TimeValue timeout() {
        return TimeValue.ZERO;
    }

    @Override
    public void timeout(TimeValue timeout) {}

    @Override
    public int terminateAfter() {
        return terminateAfter;
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        this.terminateAfter = terminateAfter;
    }

    @Override
    public boolean lowLevelCancellation() {
        return false;
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        this.minScore = minimumScore;
        return this;
    }

    @Override
    public Float minimumScore() {
        return minScore;
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public SortAndFormats sort() {
        return sort;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    @Override
    public boolean trackScores() {
        return trackScores;
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
    public SearchContext searchAfter(FieldDoc searchAfterDoc) {
        this.searchAfter = searchAfterDoc;
        return this;
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

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return postFilter;
    }

    @Override
    public Query aliasFilter() {
        return null;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return originalQuery;
    }

    @Override
    public Query query() {
        return query;
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

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public SearchContext size(int size) {
        return null;
    }

    @Override
    public boolean hasStoredFields() {
        return false;
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return false;
    }

    @Override
    public boolean storedFieldsRequested() {
        return false;
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return null;
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        return null;
    }

    @Override
    public boolean explain() {
        return false;
    }

    @Override
    public void explain(boolean explain) {}

    @Override
    public List<String> groupStats() {
        return null;
    }

    @Override
    public void groupStats(List<String> groupStats) {}

    @Override
    public boolean version() {
        return false;
    }

    @Override
    public void version(boolean version) {}

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return false;
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {

    }

    @Override
    public int[] docIdsToLoad() {
        return new int[0];
    }

    @Override
    public int docIdsToLoadFrom() {
        return 0;
    }

    @Override
    public int docIdsToLoadSize() {
        return 0;
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        return null;
    }

    @Override
    public DfsSearchResult dfsResult() {
        return null;
    }

    @Override
    public QuerySearchResult queryResult() {
        return queryResult;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return null;
    }

    @Override
    public FetchPhase fetchPhase() {
        return null;
    }

    @Override
    public MappedFieldType fieldType(String name) {
        if (mapperService() != null) {
            return mapperService().fieldType(name);
        }
        return null;
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        if (mapperService() != null) {
            return mapperService().getObjectMapper(name);
        }
        return null;
    }

    @Override
    public void doClose() {}

    @Override
    public long getRelativeTimeInMillis() {
        return 0L;
    }

    @Override
    public Profilers getProfilers() {
        return profilers;
    }

    /**
     * Returns concurrent segment search status for the search context
     */
    @Override
    public boolean isConcurrentSegmentSearchEnabled() {
        return concurrentSegmentSearchEnabled;
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
    public void addRescore(RescoreContext rescore) {

    }

    @Override
    public ReaderContext readerContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalAggregation.ReduceContext partialOnShard() {
        return InternalAggregationTestCase.emptyReduceContextBuilder().forPartialReduction();
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
     * Clean the query results by consuming all of it
     */
    public TestSearchContext withCleanQueryResult() {
        queryResult.consumeAll();
        profilers = null;
        return this;
    }

    /**
     * Add profilers to the query
     */
    public TestSearchContext withProfilers() {
        this.profilers = new Profilers(searcher, concurrentSegmentSearchEnabled);
        return this;
    }
}
