/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.admin.indices.segments.TransportPitSegmentsAction.PitAwareShardRouting;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.OptionallyResolvedIndices;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchContextMissingException;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.action.search.SearchContextId.decode;

/**
 * Transport action for retrieving usage information of PIT reader contexts
 */
public class TransportPitUsageAction extends TransportBroadcastByNodeAction<PitUsageRequest, PitUsageResponse, PitUsageInfo>
    implements
        TransportIndicesResolvingAction<PitUsageRequest> {
    private final SearchService searchService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final PitService pitService;

    @Inject
    public TransportPitUsageAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchService searchService,
        NamedWriteableRegistry namedWriteableRegistry,
        PitService pitService
    ) {
        super(
            PitUsageAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            PitUsageRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.searchService = searchService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.pitService = pitService;
    }

    @Override
    protected void doExecute(Task task, PitUsageRequest request, ActionListener<PitUsageResponse> listener) {
        if (isAllPitsRequest(request)) {
            pitService.getAllPits(ActionListener.wrap(response -> {
                request.clearAndSetPitIds(response.getPitInfos().stream().map(ListPitInfo::getPitId).collect(Collectors.toList()));
                super.doExecute(task, request, listener);
            }, listener::onFailure));
        } else {
            super.doExecute(task, request, listener);
        }
    }

    @Override
    public OptionallyResolvedIndices resolveIndices(PitUsageRequest request) {
        if (isAllPitsRequest(request)) {
            return ResolvedIndices.unknown();
        } else {
            return ResolvedIndices.of(this.pitService.getIndicesForPitsFlat(request.getPitIds()));
        }
    }

    private boolean isAllPitsRequest(PitUsageRequest request) {
        return request.getPitIds().size() == 1 && "_all".equals(request.getPitIds().get(0));
    }

    @Override
    protected ResolvedIndices.Local.Concrete resolveConcreteIndices(ClusterState clusterState, PitUsageRequest request) {
        Set<Index> indices = new HashSet<>();
        for (String pitId : request.getPitIds()) {
            for (ShardId shardId : decode(namedWriteableRegistry, pitId).shards().keySet()) {
                indices.add(shardId.getIndex());
            }
        }
        return ResolvedIndices.Local.Concrete.of(indices.toArray(new Index[0]));
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, PitUsageRequest request, String[] concreteIndices) {
        final ArrayList<ShardRouting> iterators = new ArrayList<>();
        // remove duplicates from the request
        Set<String> uniquePitIds = new LinkedHashSet<>(request.getPitIds());
        for (String pitId : uniquePitIds) {
            SearchContextId searchContext = decode(namedWriteableRegistry, pitId);
            for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
                final SearchContextIdForNode perNode = entry.getValue();
                // check if node is part of local cluster
                if (Strings.isEmpty(perNode.getClusterAlias())) {
                    iterators.add(
                        new PitAwareShardRouting(
                            pitId,
                            entry.getKey(),
                            perNode.getNode(),
                            null,
                            true,
                            ShardRoutingState.STARTED,
                            null,
                            null,
                            null,
                            -1L
                        )
                    );
                }
            }
        }
        return new PlainShardsIterator(iterators);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PitUsageRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PitUsageRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected PitUsageInfo readShardResult(StreamInput in) throws IOException {
        return new PitUsageInfo(in);
    }

    /**
     * A search request runs the query phase on every shard of the PIT that can match, so the request count of a PIT
     * is the maximum over its shards. The fetch phase only loads the hits that are returned, so the hit count is the sum.
     */
    @Override
    protected PitUsageResponse newResponse(
        PitUsageRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<PitUsageInfo> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        Map<String, PitUsageInfo> pitUsages = new LinkedHashMap<>();
        for (PitUsageInfo result : results) {
            pitUsages.merge(
                result.getPitId(),
                result,
                (a, b) -> new PitUsageInfo(
                    a.getPitId(),
                    Math.max(a.getSearchRequests(), b.getSearchRequests()),
                    a.getHitsReturned() + b.getHitsReturned()
                )
            );
        }
        return new PitUsageResponse(new ArrayList<>(pitUsages.values()), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected PitUsageRequest readRequestFrom(StreamInput in) throws IOException {
        return new PitUsageRequest(in);
    }

    @Override
    public List<ShardRouting> getShardRoutingsFromInputStream(StreamInput in) throws IOException {
        return in.readList(PitAwareShardRouting::new);
    }

    @Override
    protected PitUsageInfo shardOperation(PitUsageRequest request, ShardRouting shardRouting) {
        assert shardRouting instanceof PitAwareShardRouting;
        String pitId = ((PitAwareShardRouting) shardRouting).getPitId();
        SearchContextIdForNode searchContextIdForNode = decode(namedWriteableRegistry, pitId).shards().get(shardRouting.shardId());
        PitReaderContext pitReaderContext = searchService.getPitReaderContext(searchContextIdForNode.getSearchContextId());
        if (pitReaderContext == null) {
            throw new SearchContextMissingException(searchContextIdForNode.getSearchContextId());
        }
        return new PitUsageInfo(pitId, pitReaderContext.getSearchRequests(), pitReaderContext.getHitsReturned());
    }
}
