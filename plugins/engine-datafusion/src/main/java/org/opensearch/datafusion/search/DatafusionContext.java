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
import org.opensearch.index.IndexService;
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
    private final IndexShard indexShard;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;
    private final IndexService indexService;
    private final QueryShardContext queryShardContext;
    private DatafusionQuery datafusionQuery;
    private Map<String, Object[]> dfResults;
    /**
     * Constructor
     * @param readerContext The reader context
     * @param request The shard search request
     * @param task The search shard task
     * @param engine The datafusion engine
     */
    public DatafusionContext(
        ReaderContext readerContext,
        ShardSearchRequest request,
        SearchShardTarget searchShardTarget,
        SearchShardTask task,
        DatafusionEngine engine) {
        this.readerContext = readerContext;
        this.indexShard = readerContext.indexShard();
        this.request = request;
        this.task = task;
        this.readEngine = engine;
        this.engineSearcher = engine.acquireSearcher("search");//null;//TODO readerContext.contextEngineSearcher();
        this.queryResult = new QuerySearchResult(readerContext.id(), searchShardTarget, request);
        this.fetchResult = new FetchSearchResult(readerContext.id(), searchShardTarget);
        this.indexService = readerContext.indexService();
        this.queryShardContext = indexService.newQueryShardContext(
            request.shardId().id(),
            null, // TOOD : index searcher is null
            request::nowInMillis,
            searchShardTarget.getClusterAlias(),
            false, // reevaluate the usage
            false // specific to lucene
        );
    }

    /**
     * Gets the read engine
     * @return The datafusion engine
     */
    public DatafusionEngine readEngine() {
        return readEngine;
    }

    /**
     * Sets datafusion query
     * @param datafusionQuery The datafusion query
     */
    public DatafusionContext datafusionQuery(DatafusionQuery datafusionQuery) {
        this.datafusionQuery = datafusionQuery;
        return this;
    }
    /**
     * Gets the datafusion query
     * @return The datafusion query
     */
    public DatafusionQuery getDatafusionQuery() {
        return datafusionQuery;
    }

    /**
     * Gets the engine searcher
     * @return The datafusion searcher
     */
    public DatafusionSearcher getEngineSearcher() {
        return engineSearcher;
    }

    /**
     * {@inheritDoc}
     * @param task The search shard task
     */
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

    /**
     * {@inheritDoc}
     * @param rewrite Whether to rewrite
     */
    @Override
    public void preProcess(boolean rewrite) {

    }

    /**
     * {@inheritDoc}
     * @param query The query
     */
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
        return request;
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

    /**
     * {@inheritDoc}
     * @param aggregations The search context aggregations
     */
    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param searchExtBuilder The search extension builder
     */
    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {

    }

    /**
     * {@inheritDoc}
     * @param name The name
     */
    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return null;
    }

    @Override
    public SearchHighlightContext highlight() {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param highlight The search highlight context
     */
    @Override
    public void highlight(SearchHighlightContext highlight) {

    }

    @Override
    public SuggestionSearchContext suggest() {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param suggest The suggestion search context
     */
    @Override
    public void suggest(SuggestionSearchContext suggest) {

    }

    @Override
    public List<RescoreContext> rescore() {
        return List.of();
    }

    /**
     * {@inheritDoc}
     * @param rescore The rescore context
     */
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

    /**
     * {@inheritDoc}
     * @param fetchSourceContext The fetch source context
     */
    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        return null;
    }

    @Override
    public FetchDocValuesContext docValuesContext() {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param docValuesContext The fetch doc values context
     */
    @Override
    public SearchContext docValuesContext(FetchDocValuesContext docValuesContext) {
        return null;
    }

    @Override
    public FetchFieldsContext fetchFieldsContext() {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param fetchFieldsContext The fetch fields context
     */
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
        return this.indexShard;
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

    /**
     * {@inheritDoc}
     * @param timeout The timeout value
     */
    @Override
    public void timeout(TimeValue timeout) {

    }

    @Override
    public int terminateAfter() {
        return 0;
    }

    /**
     * {@inheritDoc}
     * @param terminateAfter The terminate after value
     */
    @Override
    public void terminateAfter(int terminateAfter) {

    }

    @Override
    public boolean lowLevelCancellation() {
        return false;
    }

    /**
     * {@inheritDoc}
     * @param minimumScore The minimum score
     */
    @Override
    public SearchContext minimumScore(float minimumScore) {
        return null;
    }

    @Override
    public Float minimumScore() {
        return 0f;
    }

    /**
     * {@inheritDoc}
     * @param sort The sort and formats
     */
    @Override
    public SearchContext sort(SortAndFormats sort) {
        return null;
    }

    @Override
    public SortAndFormats sort() {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param trackScores Whether to track scores
     */
    @Override
    public SearchContext trackScores(boolean trackScores) {
        return null;
    }

    @Override
    public boolean trackScores() {
        return false;
    }

    /**
     * {@inheritDoc}
     * @param trackTotalHits The track total hits value
     */
    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHits) {
        return null;
    }

    @Override
    public int trackTotalHitsUpTo() {
        return 0;
    }

    @Override
    /**
     * {@inheritDoc}
     * @param searchAfter The field doc for search after
     */
    public SearchContext searchAfter(FieldDoc searchAfter) {
        return null;
    }

    @Override
    public FieldDoc searchAfter() {
        return null;
    }

    @Override
    /**
     * {@inheritDoc}
     * @param collapse The collapse context
     */
    public SearchContext collapse(CollapseContext collapse) {
        return null;
    }

    @Override
    public CollapseContext collapse() {
        return null;
    }

    @Override
    /**
     * {@inheritDoc}
     * @param postFilter The parsed post filter query
     */
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
    /**
     * {@inheritDoc}
     * @param query The parsed query
     */
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

    /**
     * {@inheritDoc}
     * @param from The from value
     */
    @Override
    public SearchContext from(int from) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    /**
     * {@inheritDoc}
     * @param size The size value
     */
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

    /**
     * {@inheritDoc}
     * @param storedFieldsContext The stored fields context
     */
    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        return null;
    }

    @Override
    public boolean explain() {
        return false;
    }

    /**
     * {@inheritDoc}
     * @param explain Whether to explain
     */
    @Override
    public void explain(boolean explain) {

    }

    @Override
    public List<String> groupStats() {
        return List.of();
    }

    /**
     * {@inheritDoc}
     * @param groupStats The group stats
     */
    @Override
    public void groupStats(List<String> groupStats) {

    }

    @Override
    public boolean version() {
        return false;
    }

    /**
     * {@inheritDoc}
     * @param version Whether to include version
     */
    @Override
    public void version(boolean version) {

    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return false;
    }

    /**
     * {@inheritDoc}
     * @param seqNoAndPrimaryTerm Whether to include sequence number and primary term
     */
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

    /**
     * {@inheritDoc}
     * @param docIdsToLoad The document IDs to load
     * @param docsIdsToLoadFrom The starting index for document IDs to load
     * @param docsIdsToLoadSize The size of document IDs to load
     */
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
        return this.queryResult;
    }

    @Override
    public FetchPhase fetchPhase() {
        return null;
    }

    @Override
    public FetchSearchResult fetchResult() {
        return this.fetchResult;
    }

    @Override
    public Profilers getProfilers() {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param name The field name
     */
    @Override
    public MappedFieldType fieldType(String name) {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param name The object mapper name
     */
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
        return queryShardContext;
    }

    @Override
    public ReaderContext readerContext() {
        return null;
    }

    @Override
    public InternalAggregation.ReduceContext partialOnShard() {
        return null;
    }

    /**
     * {@inheritDoc}
     * @param bucketCollectorProcessor The bucket collector processor
     */
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

    /**
     * Gets the context engine searcher
     * @return The context engine searcher
     */
    public ContextEngineSearcher<DatafusionQuery, RecordBatchStream> contextEngineSearcher() {
        return new ContextEngineSearcher<>(this.engineSearcher, this);
    }

    public void setDFResults(Map<String, Object[]> dfResults) {
        this.dfResults = dfResults;
    }

    public Map<String, Object[]> getDFResults() {
        return dfResults;
    }
}
