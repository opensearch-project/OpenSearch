package org.opensearch.search;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;

import java.util.function.LongSupplier;

/**
 * Lightweight engine-agnostic reader context for query execution
 */
public class EngineReaderContext {
    private final EngineSearcherSupplier<?> engineSearcherSupplier;
    private final ShardSearchRequest request;
    private final SearchShardTarget shardTarget;
    private final FetchPhase fetchPhase;
    private final QuerySearchResult queryResult;
    private final FetchSearchResult fetchResult;
    private final IndexService indexService;
    private final IndexShard indexShard;
    private final ClusterService clusterService;
    private final ContextEngineSearcher contextEngineSearcher;
    private final LongSupplier relativeTimeSupplier;
    private final TimeValue timeout;
    private final boolean lowLevelCancellation;

    public EngineReaderContext(
        EngineSearcherSupplier<?> engineSearcherSupplier,
        ShardSearchRequest request,
        SearchShardTarget shardTarget,
        FetchPhase fetchPhase,
        QuerySearchResult queryResult,
        FetchSearchResult fetchResult,
        IndexService indexService,
        IndexShard indexShard,
        ClusterService clusterService,
        ContextEngineSearcher<?> contextEngineSearcher,
        LongSupplier relativeTimeSupplier,
        TimeValue timeout,
        boolean lowLevelCancellation
    ) {
        this.engineSearcherSupplier = engineSearcherSupplier;
        this.request = request;
        this.shardTarget = shardTarget;
        this.fetchPhase = fetchPhase;
        this.queryResult = queryResult;
        this.fetchResult = fetchResult;
        this.indexService = indexService;
        this.indexShard = indexShard;
        this.clusterService = clusterService;
        this.contextEngineSearcher = contextEngineSearcher;
        this.relativeTimeSupplier = relativeTimeSupplier;
        this.timeout = timeout;
        this.lowLevelCancellation = lowLevelCancellation;
    }

    public ContextEngineSearcher<?> contextEngineSearcher() { return contextEngineSearcher; }
    public ShardSearchRequest request() { return request; }
    public QuerySearchResult queryResult() { return queryResult; }
    public FetchSearchResult fetchResult() { return fetchResult; }
    public IndexShard indexShard() { return indexShard; }
    public TimeValue timeout() { return timeout; }
    public IndexService indexService() { return indexService; }
    public ClusterService clusterService() { return clusterService; }
}
