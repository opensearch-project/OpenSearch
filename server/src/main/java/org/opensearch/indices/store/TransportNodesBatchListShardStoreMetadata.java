/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.store;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.*;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.AsyncShardsFetchPerNode;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Transport action for fetching the batch of shard stores Metadata from a list of transport nodes
 *
 * @opensearch.internal
 */
public class TransportNodesBatchListShardStoreMetadata extends TransportNodesAction<
    TransportNodesBatchListShardStoreMetadata.Request,
    TransportNodesBatchListShardStoreMetadata.NodesStoreFilesMetadata,
    TransportNodesBatchListShardStoreMetadata.NodeRequest,
    TransportNodesBatchListShardStoreMetadata.NodeStoreFilesMetadataBatch> implements
    AsyncShardsFetchPerNode.Lister<
        TransportNodesBatchListShardStoreMetadata.NodesStoreFilesMetadata,
        TransportNodesBatchListShardStoreMetadata.NodeStoreFilesMetadataBatch> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store/batch";
    public static final ActionType<TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata> TYPE = new ActionType<>(ACTION_NAME, TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata::new);

    private final Settings settings;
    private final IndicesService indicesService;
    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesBatchListShardStoreMetadata(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        NodeEnvironment nodeEnv,
        ActionFilters actionFilters
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STORE,
            NodeStoreFilesMetadataBatch.class
        );
        this.settings = settings;
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
    }

    @Override
    public void list(DiscoveryNode[] nodes,  Map<ShardId,String> shardIdsWithCustomDataPath, ActionListener<NodesStoreFilesMetadata> listener) {
        execute(new TransportNodesBatchListShardStoreMetadata.Request(shardIdsWithCustomDataPath, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeStoreFilesMetadataBatch newNodeResponse(StreamInput in) throws IOException {
        return new NodeStoreFilesMetadataBatch(in);
    }

    @Override
    protected NodesStoreFilesMetadata newResponse(
        Request request,
        List<NodeStoreFilesMetadataBatch> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesStoreFilesMetadata(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeStoreFilesMetadataBatch nodeOperation(NodeRequest request) {
        try {
            return new NodeStoreFilesMetadataBatch(clusterService.localNode(), listStoreMetadata(request));
        } catch (IOException e) {
            throw new OpenSearchException("Failed to list store metadata for shards [" + request.getShardIdsWithCustomDataPath() + "]", e);
        }
    }

    /**
     * This method is similar to listStoreMetadata method of {@link TransportNodesListShardStoreMetadata}
     * In this case we fetch the shard store files for batch of shards instead of one shard.
     */
    private Map<ShardId, NodeStoreFilesMetadata> listStoreMetadata(NodeRequest request) throws IOException {
        Map<ShardId, NodeStoreFilesMetadata> shardStoreMetadataMap = new HashMap<ShardId, NodeStoreFilesMetadata>();
        for(Map.Entry<ShardId, String> shardToCustomDataPathEntry: request.getShardIdsWithCustomDataPath().entrySet()) {
            final ShardId shardId = shardToCustomDataPathEntry.getKey();
            try {
                logger.debug("Listing store meta data for {}", shardId);
                TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata nodeStoreFilesMetadata = TransportNodesListShardStoreMetadataHelper.getStoreFilesMetadata(
                    logger,
                    indicesService,
                    clusterService,
                    shardId,
                    shardToCustomDataPathEntry.getValue(),
                    settings,
                    nodeEnv
                );
                shardStoreMetadataMap.put(
                    shardId,
                    new NodeStoreFilesMetadata(
                        nodeStoreFilesMetadata,
                        null
                    )
                );
            } catch (Exception storeFileFetchException) {
                logger.trace(new ParameterizedMessage("Unable to fetch store files for shard [{}]", shardId), storeFileFetchException);
                shardStoreMetadataMap.put(
                    shardId,
                    new NodeStoreFilesMetadata(
                        null,
                        storeFileFetchException
                    )
                );
            }
        }
        logger.debug("Loaded store meta data for {} shards", shardStoreMetadataMap);
        return shardStoreMetadataMap;
    }


    /**
     * The request
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final Map<ShardId, String> shardIdsWithCustomDataPath;


        public Request(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath = in.readMap(ShardId::new, StreamInput::readString);
        }

        public Request(Map<ShardId, String> shardIdsWithCustomDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(shardIdsWithCustomDataPath);
        }

        public Map<ShardId, String> getShardIdsWithCustomDataPath() {
            return shardIdsWithCustomDataPath;
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }
    }

    /**
     * Metadata for the nodes store files
     *
     * @opensearch.internal
     */
    public static class NodesStoreFilesMetadata extends BaseNodesResponse<NodeStoreFilesMetadataBatch> {

        public NodesStoreFilesMetadata(StreamInput in) throws IOException {
            super(in);
        }

        public NodesStoreFilesMetadata(ClusterName clusterName, List<NodeStoreFilesMetadataBatch> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeStoreFilesMetadataBatch> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeStoreFilesMetadataBatch::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeStoreFilesMetadataBatch> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The metadata for the node store files
     *
     * @opensearch.internal
     */
    public static class NodeStoreFilesMetadata {

        private TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata storeFilesMetadata;
        private Exception storeFileFetchException;

        public NodeStoreFilesMetadata(TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata storeFilesMetadata) {
            this.storeFilesMetadata = storeFilesMetadata;
            this.storeFileFetchException = null;
        }

        public NodeStoreFilesMetadata(StreamInput in) throws IOException {
            storeFilesMetadata = new TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata(in);
            this.storeFileFetchException = null;
        }

        public NodeStoreFilesMetadata(TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata storeFilesMetadata, Exception storeFileFetchException) {
            this.storeFilesMetadata = storeFilesMetadata;
            this.storeFileFetchException = storeFileFetchException;
        }

        public TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata storeFilesMetadata() {
            return storeFilesMetadata;
        }

        public static NodeStoreFilesMetadata readListShardStoreNodeOperationResponse(StreamInput in) throws IOException {
            return new NodeStoreFilesMetadata(in);
        }

        public void writeTo(StreamOutput out) throws IOException {
            storeFilesMetadata.writeTo(out);
        }

        public Exception getStoreFileFetchException() {
            return storeFileFetchException;
        }

        @Override
        public String toString() {
            return "[[" + storeFilesMetadata + "]]";
        }
    }

    /**
     * The node request
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {

        private final Map<ShardId, String> shardIdsWithCustomDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath = in.readMap(ShardId::new, StreamInput::readString);
        }

        public NodeRequest(Request request) {
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(request.getShardIdsWithCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }

        public Map<ShardId, String> getShardIdsWithCustomDataPath() {
            return shardIdsWithCustomDataPath;
        }
    }


    public static class NodeStoreFilesMetadataBatch extends BaseNodeResponse {
        private final Map<ShardId, NodeStoreFilesMetadata> nodeStoreFilesMetadataBatch;
        protected NodeStoreFilesMetadataBatch(StreamInput in) throws IOException {
            super(in);
            this.nodeStoreFilesMetadataBatch =  in.readMap(ShardId::new, NodeStoreFilesMetadata::new);
        }


        public NodeStoreFilesMetadataBatch(DiscoveryNode node, Map<ShardId, NodeStoreFilesMetadata> nodeStoreFilesMetadataBatch) {
            super(node);
            this.nodeStoreFilesMetadataBatch = nodeStoreFilesMetadataBatch;
        }

        public Map<ShardId, NodeStoreFilesMetadata> getNodeStoreFilesMetadataBatch() {
            return this.nodeStoreFilesMetadataBatch;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(nodeStoreFilesMetadataBatch, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
        }
    }

}
