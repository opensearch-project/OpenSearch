/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
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
import org.opensearch.datafusion.DatafusionEngine;
import org.opensearch.search.ContextEngineSearcher;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestionSearchContext;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

import java.util.List;
import java.util.Map;

/**
 * Search context for Datafusion engine
 */
public class DatafusionContext extends SearchContext {
    private final ReaderContext readerContext;
    private final ShardSearchRequest request;
    private final SearchShardTask task;
    private final DatafusionEngine readEngine;
    private final DatafusionSearcher engineSearcher;
    private DatafusionQuery datafusionQuery;

    public DatafusionContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTask task,
        DatafusionEngine engine) {
        this.readerContext = readerContext;
        this.request = request;
        this.task = task;
        this.readEngine = engine;
        this.engineSearcher = engine.acquireSearcher("search");//null;//TODO readerContext.contextEngineSearcher();
    }

    public DatafusionEngine readEngine() {
        return readEngine;
    }

    public DatafusionContext datafusionQuery(DatafusionQuery datafusionQuery) {
        this.datafusionQuery = datafusionQuery;
        return this;
    }
    public DatafusionQuery getDatafusionQuery() {
        return datafusionQuery;
    }

    public DatafusionSearcher getEngineSearcher() {
        return engineSearcher;
    }

    @Override
    public void setTask(SearchShardTask task) {

    }

    @Override
    public SearchShardTask getTask() {
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    protected void doClose() {

    }

    @Override
    public void preProcess(boolean rewrite) {

    }

    @Override
    public Query buildFilteredQuery(Query query) {
        return null;
    }

    @Override
    public ShardSearchContextId id() {
        return null;
    }

    @Override
    public String source() {
        return "";
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
        return 0;
    }

    @Override
    public float queryBoost() {
        return 0;
    }

    @Override
    public ScrollContext scrollContext() {
        return null;
    }

    @Override
    public SearchContextAggregations aggregations() {
        return null;
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        return null;
    }

    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {

    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return null;
    }

    @Override
    public SearchHighlightContext highlight() {
        return null;
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {

    }

    @Override
    public SuggestionSearchContext suggest() {
        return null;
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {

    }

    @Override
    public List<RescoreContext> rescore() {
        return List.of();
    }

    @Override
    public void addRescore(RescoreContext rescore) {

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
        return null;
    }

    @Override
    public IndexShard indexShard() {
        return null;
    }

    @Override
    public MapperService mapperService() {
        return null;
    }

    @Override
    public SimilarityService similarityService() {
        return null;
    }

    @Override
    public BigArrays bigArrays() {
        return null;
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return null;
    }

    @Override
    public TimeValue timeout() {
        return null;
    }

    @Override
    public void timeout(TimeValue timeout) {

    }

    @Override
    public int terminateAfter() {
        return 0;
    }

    @Override
    public void terminateAfter(int terminateAfter) {

    }

    @Override
    public boolean lowLevelCancellation() {
        return false;
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        return null;
    }

    @Override
    public Float minimumScore() {
        return 0f;
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        return null;
    }

    @Override
    public SortAndFormats sort() {
        return null;
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        return null;
    }

    @Override
    public boolean trackScores() {
        return false;
    }

    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
        return null;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return 0;
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        return null;
    }

    @Override
    public FieldDoc searchAfter() {
        return null;
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        return null;
    }

    @Override
    public CollapseContext collapse() {
        return null;
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        return null;
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return null;
    }

    @Override
    public Query aliasFilter() {
        return null;
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        return null;
    }

    @Override
    public ParsedQuery parsedQuery() {
        return null;
    }

    // TODO : fix this
    public Query query() {
        // Extract query from request
        return null;
    }

    @Override
    public int from() {
        return 0;
    }

    @Override
    public SearchContext from(int from) {
        return null;
    }

    @Override
    public int size() {
        return 0;
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
    public void explain(boolean explain) {

    }

    @Override
    public List<String> groupStats() {
        return List.of();
    }

    @Override
    public void groupStats(List<String> groupStats) {

    }

    @Override
    public boolean version() {
        return false;
    }

    @Override
    public void version(boolean version) {

    }

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
        return null;
    }

    @Override
    public FetchPhase fetchPhase() {
        return null;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return null;
    }

    @Override
    public Profilers getProfilers() {
        return null;
    }

    @Override
    public MappedFieldType fieldType(String name) {
        return null;
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        return null;
    }

    @Override
    public long getRelativeTimeInMillis() {
        return 0;
    }

    @Override
    public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers() {
        return Map.of();
    }

    @Override
    public QueryShardContext getQueryShardContext() {
        return null;
    }

    @Override
    public ReaderContext readerContext() {
        return null;
    }

    @Override
    public InternalAggregation.ReduceContext partialOnShard() {
        return null;
    }

    @Override
    public void setBucketCollectorProcessor(BucketCollectorProcessor bucketCollectorProcessor) {

    }

    @Override
    public BucketCollectorProcessor bucketCollectorProcessor() {
        return null;
    }

    @Override
    public int getTargetMaxSliceCount() {
        return 0;
    }

    @Override
    public boolean shouldUseTimeSeriesDescSortOptimization() {
        return false;
    }

    public ContextEngineSearcher<DatafusionQuery, RecordBatchStream> contextEngineSearcher() {
        return new ContextEngineSearcher<>(this.engineSearcher, this);
    }
}
