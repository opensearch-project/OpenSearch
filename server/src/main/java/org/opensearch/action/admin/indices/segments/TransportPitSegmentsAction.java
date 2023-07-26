/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.ListPitInfo;
import org.opensearch.action.search.PitService;
import org.opensearch.action.search.SearchContextId;
import org.opensearch.action.search.SearchContextIdForNode;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.PlainShardsIterator;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.action.search.SearchContextId.decode;

/**
 * Transport action for retrieving segment information of PITs
 */
public class TransportPitSegmentsAction extends TransportBroadcastByNodeAction<PitSegmentsRequest, IndicesSegmentResponse, ShardSegments> {
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final SearchService searchService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportService transportService;
    private final PitService pitService;

    @Inject
    public TransportPitSegmentsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchService searchService,
        NamedWriteableRegistry namedWriteableRegistry,
        PitService pitService
    ) {
        super(
            PitSegmentsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            PitSegmentsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.transportService = transportService;
        this.pitService = pitService;
    }

    /**
     * Execute PIT segments flow for all PITs or request PIT IDs
     */
    @Override
    protected void doExecute(Task task, PitSegmentsRequest request, ActionListener<IndicesSegmentResponse> listener) {
        if (request.getPitIds().size() == 1 && "_all".equals(request.getPitIds().get(0))) {
            pitService.getAllPits(ActionListener.wrap(response -> {
                request.clearAndSetPitIds(response.getPitInfos().stream().map(ListPitInfo::getPitId).collect(Collectors.toList()));
                super.doExecute(task, request, listener);
            }, listener::onFailure));
        } else {
            super.doExecute(task, request, listener);
        }
    }

    /**
     * This adds list of shards on which we need to retrieve pit segments details
     * @param clusterState    the cluster state
     * @param request         the underlying request
     * @param concreteIndices the concrete indices on which to execute the operation
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, PitSegmentsRequest request, String[] concreteIndices) {
        final ArrayList<ShardRouting> iterators = new ArrayList<>();
        // remove duplicates from the request
        Set<String> uniquePitIds = new LinkedHashSet<>(request.getPitIds());
        for (String pitId : uniquePitIds) {
            SearchContextId searchContext = decode(namedWriteableRegistry, pitId);
            for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
                final SearchContextIdForNode perNode = entry.getValue();
                // check if node is part of local cluster
                if (Strings.isEmpty(perNode.getClusterAlias())) {
                    final ShardId shardId = entry.getKey();
                    iterators.add(
                        new PitAwareShardRouting(
                            pitId,
                            shardId,
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
    protected IndicesSegmentResponse newResponse(
        PitSegmentsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardSegments> results,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new IndicesSegmentResponse(
            results.toArray(new ShardSegments[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected PitSegmentsRequest readRequestFrom(StreamInput in) throws IOException {
        return new PitSegmentsRequest(in);
    }

    @Override
    public List<ShardRouting> getShardRoutingsFromInputStream(StreamInput in) throws IOException {
        return in.readList(PitAwareShardRouting::new);
    }

    /**
     * This retrieves segment details of PIT context
     * @param request      the node-level request
     * @param shardRouting the shard on which to execute the operation
     */
    @Override
    protected ShardSegments shardOperation(PitSegmentsRequest request, ShardRouting shardRouting) {
        assert shardRouting instanceof PitAwareShardRouting;
        PitAwareShardRouting pitAwareShardRouting = (PitAwareShardRouting) shardRouting;
        SearchContextIdForNode searchContextIdForNode = decode(namedWriteableRegistry, pitAwareShardRouting.getPitId()).shards()
            .get(shardRouting.shardId());
        PitReaderContext pitReaderContext = searchService.getPitReaderContext(searchContextIdForNode.getSearchContextId());
        if (pitReaderContext == null) {
            return new ShardSegments(shardRouting, Collections.emptyList());
        }
        return new ShardSegments(pitReaderContext.getShardRouting(), pitReaderContext.getSegments());
    }

    /**
     * This holds PIT id which is used to perform broadcast operation in PIT shards to retrieve segments information
     */
    public class PitAwareShardRouting extends ShardRouting {

        private final String pitId;

        public PitAwareShardRouting(StreamInput in) throws IOException {
            super(in);
            this.pitId = in.readString();
        }

        public PitAwareShardRouting(
            String pitId,
            ShardId shardId,
            String currentNodeId,
            String relocatingNodeId,
            boolean primary,
            ShardRoutingState state,
            RecoverySource recoverySource,
            UnassignedInfo unassignedInfo,
            AllocationId allocationId,
            long expectedShardSize
        ) {
            super(
                shardId,
                currentNodeId,
                relocatingNodeId,
                primary,
                state,
                recoverySource,
                unassignedInfo,
                allocationId,
                expectedShardSize
            );
            this.pitId = pitId;
        }

        public String getPitId() {
            return pitId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(pitId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            super.toXContent(builder, params);
            builder.field("pit_id", pitId);
            return builder.endObject();
        }
    }
}
