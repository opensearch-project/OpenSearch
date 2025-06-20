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

import java.util.List;
import java.util.Map;

/**
 * Context used during a filtered search
 *
 * @opensearch.internal
 */
public abstract class FilteredSearchContext extends SearchContext {

    private final SearchContext in;

    public FilteredSearchContext(SearchContext in) {
        this.in = in;
    }

    @Override
    public boolean hasStoredFields() {
        return in.hasStoredFields();
    }

    @Override
    public boolean hasStoredFieldsContext() {
        return in.hasStoredFieldsContext();
    }

    @Override
    public boolean storedFieldsRequested() {
        return in.storedFieldsRequested();
    }

    @Override
    public StoredFieldsContext storedFieldsContext() {
        return in.storedFieldsContext();
    }

    @Override
    public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
        return in.storedFieldsContext(storedFieldsContext);
    }

    @Override
    protected void doClose() {
        in.doClose();
    }

    @Override
    public void preProcess(boolean rewrite) {
        in.preProcess(rewrite);
    }

    @Override
    public Query buildFilteredQuery(Query query) {
        return in.buildFilteredQuery(query);
    }

    @Override
    public ShardSearchContextId id() {
        return in.id();
    }

    @Override
    public String source() {
        return in.source();
    }

    @Override
    public ShardSearchRequest request() {
        return in.request();
    }

    @Override
    public SearchType searchType() {
        return in.searchType();
    }

    @Override
    public SearchShardTarget shardTarget() {
        return in.shardTarget();
    }

    @Override
    public int numberOfShards() {
        return in.numberOfShards();
    }

    @Override
    public float queryBoost() {
        return in.queryBoost();
    }

    @Override
    public ScrollContext scrollContext() {
        return in.scrollContext();
    }

    @Override
    public SearchContextAggregations aggregations() {
        return in.aggregations();
    }

    @Override
    public SearchContext aggregations(SearchContextAggregations aggregations) {
        return in.aggregations(aggregations);
    }

    @Override
    public SearchHighlightContext highlight() {
        return in.highlight();
    }

    @Override
    public void highlight(SearchHighlightContext highlight) {
        in.highlight(highlight);
    }

    @Override
    public InnerHitsContext innerHits() {
        return in.innerHits();
    }

    @Override
    public SuggestionSearchContext suggest() {
        return in.suggest();
    }

    @Override
    public void suggest(SuggestionSearchContext suggest) {
        in.suggest(suggest);
    }

    @Override
    public List<RescoreContext> rescore() {
        return in.rescore();
    }

    @Override
    public boolean hasScriptFields() {
        return in.hasScriptFields();
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        return in.scriptFields();
    }

    @Override
    public boolean sourceRequested() {
        return in.sourceRequested();
    }

    @Override
    public boolean hasFetchSourceContext() {
        return in.hasFetchSourceContext();
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return in.fetchSourceContext();
    }

    @Override
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        return in.fetchSourceContext(fetchSourceContext);
    }

    @Override
    public ContextIndexSearcher searcher() {
        return in.searcher();
    }

    @Override
    public IndexShard indexShard() {
        return in.indexShard();
    }

    @Override
    public MapperService mapperService() {
        return in.mapperService();
    }

    @Override
    public SimilarityService similarityService() {
        return in.similarityService();
    }

    @Override
    public BigArrays bigArrays() {
        return in.bigArrays();
    }

    @Override
    public BitsetFilterCache bitsetFilterCache() {
        return in.bitsetFilterCache();
    }

    @Override
    public TimeValue timeout() {
        return in.timeout();
    }

    @Override
    public void timeout(TimeValue timeout) {
        in.timeout(timeout);
    }

    @Override
    public int terminateAfter() {
        return in.terminateAfter();
    }

    @Override
    public void terminateAfter(int terminateAfter) {
        in.terminateAfter(terminateAfter);
    }

    @Override
    public boolean lowLevelCancellation() {
        return in.lowLevelCancellation();
    }

    @Override
    public SearchContext minimumScore(float minimumScore) {
        return in.minimumScore(minimumScore);
    }

    @Override
    public Float minimumScore() {
        return in.minimumScore();
    }

    @Override
    public SearchContext sort(SortAndFormats sort) {
        return in.sort(sort);
    }

    @Override
    public SortAndFormats sort() {
        return in.sort();
    }

    @Override
    public SearchContext trackScores(boolean trackScores) {
        return in.trackScores(trackScores);
    }

    @Override
    public boolean trackScores() {
        return in.trackScores();
    }

    @Override
    public SearchContext trackTotalHitsUpTo(int trackTotalHitsUpTo) {
        return in.trackTotalHitsUpTo(trackTotalHitsUpTo);
    }

    @Override
    public int trackTotalHitsUpTo() {
        return in.trackTotalHitsUpTo();
    }

    @Override
    public SearchContext searchAfter(FieldDoc searchAfter) {
        return in.searchAfter(searchAfter);
    }

    @Override
    public FieldDoc searchAfter() {
        return in.searchAfter();
    }

    public SearchContext includeNamedQueriesScore(boolean includeNamedQueriesScore) {
        return in.includeNamedQueriesScore(includeNamedQueriesScore);
    }

    public boolean includeNamedQueriesScore() {
        return in.includeNamedQueriesScore();
    }

    @Override
    public SearchContext parsedPostFilter(ParsedQuery postFilter) {
        return in.parsedPostFilter(postFilter);
    }

    @Override
    public ParsedQuery parsedPostFilter() {
        return in.parsedPostFilter();
    }

    @Override
    public Query aliasFilter() {
        return in.aliasFilter();
    }

    @Override
    public SearchContext parsedQuery(ParsedQuery query) {
        return in.parsedQuery(query);
    }

    @Override
    public ParsedQuery parsedQuery() {
        return in.parsedQuery();
    }

    @Override
    public Query query() {
        return in.query();
    }

    @Override
    public int from() {
        return in.from();
    }

    @Override
    public SearchContext from(int from) {
        return in.from(from);
    }

    @Override
    public int size() {
        return in.size();
    }

    @Override
    public SearchContext size(int size) {
        return in.size(size);
    }

    @Override
    public boolean explain() {
        return in.explain();
    }

    @Override
    public void explain(boolean explain) {
        in.explain(explain);
    }

    @Override
    public List<String> groupStats() {
        return in.groupStats();
    }

    @Override
    public void groupStats(List<String> groupStats) {
        in.groupStats(groupStats);
    }

    @Override
    public boolean version() {
        return in.version();
    }

    @Override
    public void version(boolean version) {
        in.version(version);
    }

    @Override
    public boolean seqNoAndPrimaryTerm() {
        return in.seqNoAndPrimaryTerm();
    }

    @Override
    public void seqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        in.seqNoAndPrimaryTerm(seqNoAndPrimaryTerm);
    }

    @Override
    public int[] docIdsToLoad() {
        return in.docIdsToLoad();
    }

    @Override
    public int docIdsToLoadFrom() {
        return in.docIdsToLoadFrom();
    }

    @Override
    public int docIdsToLoadSize() {
        return in.docIdsToLoadSize();
    }

    @Override
    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        return in.docIdsToLoad(docIdsToLoad, docsIdsToLoadFrom, docsIdsToLoadSize);
    }

    @Override
    public DfsSearchResult dfsResult() {
        return in.dfsResult();
    }

    @Override
    public QuerySearchResult queryResult() {
        return in.queryResult();
    }

    @Override
    public FetchSearchResult fetchResult() {
        return in.fetchResult();
    }

    @Override
    public FetchPhase fetchPhase() {
        return in.fetchPhase();
    }

    @Override
    public MappedFieldType fieldType(String name) {
        return in.fieldType(name);
    }

    @Override
    public ObjectMapper getObjectMapper(String name) {
        return in.getObjectMapper(name);
    }

    @Override
    public long getRelativeTimeInMillis() {
        return in.getRelativeTimeInMillis();
    }

    @Override
    public void addSearchExt(SearchExtBuilder searchExtBuilder) {
        in.addSearchExt(searchExtBuilder);
    }

    @Override
    public SearchExtBuilder getSearchExt(String name) {
        return in.getSearchExt(name);
    }

    @Override
    public Profilers getProfilers() {
        return in.getProfilers();
    }

    @Override
    public Map<Class<?>, CollectorManager<? extends Collector, ReduceableSearchResult>> queryCollectorManagers() {
        return in.queryCollectorManagers();
    }

    @Override
    public QueryShardContext getQueryShardContext() {
        return in.getQueryShardContext();
    }

    @Override
    public void setTask(SearchShardTask task) {
        in.setTask(task);
    }

    @Override
    public SearchShardTask getTask() {
        return in.getTask();
    }

    @Override
    public boolean isCancelled() {
        return in.isCancelled();
    }

    @Override
    public SearchContext collapse(CollapseContext collapse) {
        return in.collapse(collapse);
    }

    @Override
    public CollapseContext collapse() {
        return in.collapse();
    }

    @Override
    public void addRescore(RescoreContext rescore) {
        in.addRescore(rescore);
    }

    @Override
    public ReaderContext readerContext() {
        return in.readerContext();
    }

    @Override
    public InternalAggregation.ReduceContext partialOnShard() {
        return in.partialOnShard();
    }

    @Override
    public void setBucketCollectorProcessor(BucketCollectorProcessor bucketCollectorProcessor) {
        in.setBucketCollectorProcessor(bucketCollectorProcessor);
    }

    @Override
    public BucketCollectorProcessor bucketCollectorProcessor() {
        return in.bucketCollectorProcessor();
    }

    @Override
    public boolean shouldUseConcurrentSearch() {
        return in.shouldUseConcurrentSearch();
    }

    @Override
    public int getTargetMaxSliceCount() {
        return in.getTargetMaxSliceCount();
    }

    @Override
    public boolean shouldUseTimeSeriesDescSortOptimization() {
        return in.shouldUseTimeSeriesDescSortOptimization();
    }

    @Override
    public boolean getStarTreeIndexEnabled() {
        return in.getStarTreeIndexEnabled();
    }
}
