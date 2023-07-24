/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This transport action is used to fetch  all unassigned shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 *
 * @opensearch.internal
 */
public class TransportNodesListGatewayStartedShardsBatch extends TransportNodesAction<
    TransportNodesListGatewayStartedShardsBatch.Request,
    TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch,
    TransportNodesListGatewayStartedShardsBatch.NodeRequest,
    TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch>
    implements
        AsyncBatchShardFetch.Lister<
            TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch,
            TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards_batch";
    public static final ActionType<NodesGatewayStartedShardsBatch> TYPE = new ActionType<>(
        ACTION_NAME,
        NodesGatewayStartedShardsBatch::new
    );

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesListGatewayStartedShardsBatch(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeEnvironment env,
        IndicesService indicesService,
        NamedXContentRegistry namedXContentRegistry
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            NodeRequest::new,
            ThreadPool.Names.FETCH_SHARD_STARTED,
            NodeGatewayStartedShardsBatch.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public void list(
        DiscoveryNode[] nodes,
        Map<ShardId, String> shardIdsWithCustomDataPath,
        ActionListener<NodesGatewayStartedShardsBatch> listener
    ) {
        execute(new Request(nodes, shardIdsWithCustomDataPath), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeGatewayStartedShardsBatch newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayStartedShardsBatch(in);
    }

    @Override
    protected NodesGatewayStartedShardsBatch newResponse(
        Request request,
        List<NodeGatewayStartedShardsBatch> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShardsBatch(clusterService.getClusterName(), responses, failures);
    }

    /**
     * This function is similar to nodeoperation method of {@link TransportNodesListGatewayStartedShards} we loop over
     * the shards here to fetch the shard result in bulk.
     *
     * @param request
     * @return NodeGatewayStartedShardsBatch
     */
    @Override
    protected NodeGatewayStartedShardsBatch nodeOperation(NodeRequest request) {
        Map<ShardId, NodeGatewayStartedShards> shardsOnNode = new HashMap<>();
        for (Map.Entry<ShardId, String> shardToCustomDataPathEntry : request.shardIdsWithCustomDataPath.entrySet()) {
            final ShardId shardId = shardToCustomDataPathEntry.getKey();
            try {
                logger.trace("{} loading local shard state info", shardId);
                ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
                    logger,
                    namedXContentRegistry,
                    nodeEnv.availableShardPaths(shardId)
                );
                if (shardStateMetadata != null) {
                    if (indicesService.getShardOrNull(shardId) == null
                        && shardStateMetadata.indexDataLocation == ShardStateMetadata.IndexDataLocation.LOCAL) {
                        final String customDataPath;
                        if (shardToCustomDataPathEntry.getValue() != null) {
                            customDataPath = shardToCustomDataPathEntry.getValue();
                        } else {
                            // TODO: Fallback for BWC with older OpenSearch versions.
                            // Remove once request.getCustomDataPath() always returns non-null
                            final IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
                            if (metadata != null) {
                                customDataPath = new IndexSettings(metadata, settings).customDataPath();
                            } else {
                                logger.trace("{} node doesn't have meta data for the requests index", shardId);
                                throw new OpenSearchException("node doesn't have meta data for index " + shardId.getIndex());
                            }
                        }
                        // we don't have an open shard on the store, validate the files on disk are openable
                        ShardPath shardPath = null;
                        try {
                            shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, customDataPath);
                            if (shardPath == null) {
                                throw new IllegalStateException(shardId + " no shard path found");
                            }
                            Store.tryOpenIndex(shardPath.resolveIndex(), shardId, nodeEnv::shardLock, logger);
                        } catch (Exception exception) {
                            final ShardPath finalShardPath = shardPath;
                            logger.trace(
                                () -> new ParameterizedMessage(
                                    "{} can't open index for shard [{}] in path [{}]",
                                    shardId,
                                    shardStateMetadata,
                                    (finalShardPath != null) ? finalShardPath.resolveIndex() : ""
                                ),
                                exception
                            );
                            String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                            shardsOnNode.put(
                                shardId,
                                new NodeGatewayStartedShards(allocationId, shardStateMetadata.primary, null, exception)
                            );
                            continue;
                        }
                    }

                    logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
                    String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                    final IndexShard shard = indicesService.getShardOrNull(shardId);
                    shardsOnNode.put(
                        shardId,
                        new NodeGatewayStartedShards(
                            allocationId,
                            shardStateMetadata.primary,
                            shard != null ? shard.getLatestReplicationCheckpoint() : null
                        )
                    );
                    continue;
                }
                logger.trace("{} no local shard info found", shardId);
                shardsOnNode.put(shardId, new NodeGatewayStartedShards(null, false, null));
            } catch (Exception e) {
                shardsOnNode.put(
                    shardId,
                    new NodeGatewayStartedShards(null, false, null, new OpenSearchException("failed to load started shards", e))
                );
            }
        }
        return new NodeGatewayStartedShardsBatch(clusterService.localNode(), shardsOnNode);
    }

    /**
     * This is used in constructing the request for making the transport request to set of other node.
     * Refer {@link TransportNodesAction} class start method.
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {
        private final Map<ShardId, String> shardIdsWithCustomDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath = in.readMap(ShardId::new, StreamInput::readString);
        }

        public Request(DiscoveryNode[] nodes, Map<ShardId, String> shardIdStringMap) {
            super(nodes);
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(shardIdStringMap);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }

        public Map<ShardId, String> getShardIdsMap() {
            return shardIdsWithCustomDataPath;
        }
    }

    /**
     * Responses received from set of other nodes is clubbed into this class and sent back to the caller
     * of this transport request. Refer {@link TransportNodesAction}
     *
     * @opensearch.internal
     */
    public static class NodesGatewayStartedShardsBatch extends BaseNodesResponse<NodeGatewayStartedShardsBatch> {

        public NodesGatewayStartedShardsBatch(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShardsBatch(
            ClusterName clusterName,
            List<NodeGatewayStartedShardsBatch> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShardsBatch> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayStartedShardsBatch::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShardsBatch> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * NodeRequest class is for deserializing the  request received by this node from other node for this transport action.
     * This is used in {@link TransportNodesAction}
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
            this.shardIdsWithCustomDataPath = Objects.requireNonNull(request.getShardIdsMap());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdsWithCustomDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }

    }

    /**
     * Class for storing the information about the shards fetched on the node.
     *
     * @opensearch.internal
     */
    public static class NodeGatewayStartedShards {
        private final String allocationId;
        private final boolean primary;
        private final Exception storeException;
        private final ReplicationCheckpoint replicationCheckpoint;

        public NodeGatewayStartedShards(StreamInput in) throws IOException {
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readException();
            } else {
                storeException = null;
            }
            if (in.getVersion().onOrAfter(Version.V_2_3_0) && in.readBoolean()) {
                replicationCheckpoint = new ReplicationCheckpoint(in);
            } else {
                replicationCheckpoint = null;
            }
        }

        public NodeGatewayStartedShards(String allocationId, boolean primary, ReplicationCheckpoint replicationCheckpoint) {
            this(allocationId, primary, replicationCheckpoint, null);
        }

        public NodeGatewayStartedShards(
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            Exception storeException
        ) {
            this.allocationId = allocationId;
            this.primary = primary;
            this.replicationCheckpoint = replicationCheckpoint;
            this.storeException = storeException;
        }

        public String allocationId() {
            return this.allocationId;
        }

        public boolean primary() {
            return this.primary;
        }

        public ReplicationCheckpoint replicationCheckpoint() {
            return this.replicationCheckpoint;
        }

        public Exception storeException() {
            return this.storeException;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(allocationId);
            out.writeBoolean(primary);
            if (storeException != null) {
                out.writeBoolean(true);
                out.writeException(storeException);
            } else {
                out.writeBoolean(false);
            }
            if (out.getVersion().onOrAfter(Version.V_2_3_0)) {
                if (replicationCheckpoint != null) {
                    out.writeBoolean(true);
                    replicationCheckpoint.writeTo(out);
                } else {
                    out.writeBoolean(false);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NodeGatewayStartedShards that = (NodeGatewayStartedShards) o;

            return primary == that.primary
                && Objects.equals(allocationId, that.allocationId)
                && Objects.equals(storeException, that.storeException)
                && Objects.equals(replicationCheckpoint, that.replicationCheckpoint);
        }

        @Override
        public int hashCode() {
            int result = (allocationId != null ? allocationId.hashCode() : 0);
            result = 31 * result + (primary ? 1 : 0);
            result = 31 * result + (storeException != null ? storeException.hashCode() : 0);
            result = 31 * result + (replicationCheckpoint != null ? replicationCheckpoint.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("NodeGatewayStartedShards[").append("allocationId=").append(allocationId).append(",primary=").append(primary);
            if (storeException != null) {
                buf.append(",storeException=").append(storeException);
            }
            if (replicationCheckpoint != null) {
                buf.append(",ReplicationCheckpoint=").append(replicationCheckpoint.toString());
            }
            buf.append("]");
            return buf.toString();
        }
    }

    /**
     * This is the response from a single node, this is used in {@link NodesGatewayStartedShardsBatch} for creating
     * node to its response mapping for this transport request.
     * Refer {@link TransportNodesAction} start method
     *
     * @opensearch.internal
     */
    public static class NodeGatewayStartedShardsBatch extends BaseNodeResponse {
        private final Map<ShardId, NodeGatewayStartedShards> nodeGatewayStartedShardsBatch;

        public Map<ShardId, NodeGatewayStartedShards> getNodeGatewayStartedShardsBatch() {
            return nodeGatewayStartedShardsBatch;
        }

        public NodeGatewayStartedShardsBatch(StreamInput in) throws IOException {
            super(in);
            this.nodeGatewayStartedShardsBatch = in.readMap(ShardId::new, NodeGatewayStartedShards::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(nodeGatewayStartedShardsBatch, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
        }

        public NodeGatewayStartedShardsBatch(DiscoveryNode node, Map<ShardId, NodeGatewayStartedShards> nodeGatewayStartedShardsBatch) {
            super(node);
            this.nodeGatewayStartedShardsBatch = nodeGatewayStartedShardsBatch;
        }
    }
}
