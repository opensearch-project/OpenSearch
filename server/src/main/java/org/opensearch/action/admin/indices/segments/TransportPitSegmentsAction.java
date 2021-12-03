/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.search.SearchContextId;
import org.opensearch.action.search.SearchContextIdForNode;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.index.Index;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TransportPitSegmentsAction extends TransportBroadcastByNodeAction<PitSegmentsRequest, IndicesSegmentResponse, ShardSegments> {

    private final IndicesService indicesService;
    private final SearchService searchService;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportPitSegmentsAction(ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      SearchService searchService,
                                      NamedWriteableRegistry namedWriteableRegistry) {
        super(PitSegmentsAction.NAME, clusterService, transportService, actionFilters, indexNameExpressionResolver,
            PitSegmentsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    /**
     * Segments goes across *all* active shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, PitSegmentsRequest request, String[] concreteIndices) {
        SearchContextId searchContext = SearchContextId.decode(namedWriteableRegistry, request.getPitIds().get(0));
        return getShardsIterator(searchContext);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PitSegmentsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PitSegmentsRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardSegments readShardResult(StreamInput in) throws IOException {
        return new ShardSegments(in);
    }

    @Override
    protected IndicesSegmentResponse newResponse(PitSegmentsRequest request, int totalShards, int successfulShards, int failedShards,
                                                 List<ShardSegments> results, List<DefaultShardOperationFailedException> shardFailures,
                                                 ClusterState clusterState) {
        return new IndicesSegmentResponse(results.toArray(new ShardSegments[results.size()]), totalShards, successfulShards, failedShards,
            shardFailures);
    }

    @Override
    protected PitSegmentsRequest readRequestFrom(StreamInput in) throws IOException {
        return new PitSegmentsRequest(in);
    }

    @Override
    protected ShardSegments shardOperation(PitSegmentsRequest request, ShardRouting shardRouting) {
        SearchContextId searchContext = SearchContextId.decode(namedWriteableRegistry, request.getPitIds().get(0));
        SearchContextIdForNode searchContextIdForNode = searchContext.shards().get(shardRouting.shardId());
        PitReaderContext pitReaderContext = searchService.getPitReaderContext(searchContextIdForNode.getSearchContextId());
        return new ShardSegments(pitReaderContext.getShardRouting(), pitReaderContext.getSegments());
    }

    public static ShardsIterator getShardsIterator(SearchContextId searchContext) {
        final ArrayList<ShardRouting> iterators = new ArrayList<>();
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
            final SearchContextIdForNode perNode = entry.getValue();
            if (Strings.isEmpty(perNode.getClusterAlias())) {
                final ShardId shardId = entry.getKey();

                final List<String> targetNodes = Collections.singletonList(perNode.getNode());
                iterators.add(new ShardRouting(shardId, perNode.getNode(), null, true, ShardRoutingState.STARTED,
                    null, null, null, -1L));

            }
        }
        return new PlainShardsIterator(iterators);
    }
}
