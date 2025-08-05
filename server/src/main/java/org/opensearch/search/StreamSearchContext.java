/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.Version;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.deciders.ConcurrentSearchRequestDecider;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO;

/**
 * Search context for stream search
 */
public class StreamSearchContext extends DefaultSearchContext {
    StreamSearchChannelListener listener;

    StreamSearchContext(
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
        super(
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
            concurrentSearchDeciderFactories
        );
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
    }

    public void setListener(StreamSearchChannelListener listener) {
        this.listener = listener;
    }

    public StreamSearchChannelListener getListener() {
        return listener;
    }

    public boolean isStreamSearch() {
        return listener != null;
    }
}
