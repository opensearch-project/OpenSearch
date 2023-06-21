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

package org.opensearch.gateway;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.transport.TransportRequest;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
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
public class TransportNodesBulkListGatewayStartedShards extends TransportNodesAction<
    TransportNodesBulkListGatewayStartedShards.Request,
    TransportNodesBulkListGatewayStartedShards.NodesGatewayStartedShards,
    TransportNodesBulkListGatewayStartedShards.NodeRequest,
    TransportNodesBulkListGatewayStartedShards.BulkOfNodeGatewayStartedShards>
    implements
        AsyncShardsFetchPerNode.Lister<
            TransportNodesBulkListGatewayStartedShards.NodesGatewayStartedShards,
            TransportNodesBulkListGatewayStartedShards.BulkOfNodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/bulk_started_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShards::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesBulkListGatewayStartedShards(
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
            BulkOfNodeGatewayStartedShards.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public void list(DiscoveryNode[] nodes, Map<ShardId, String> shardsIdMap, ActionListener<NodesGatewayStartedShards> listener) {
        execute(new Request(nodes, shardsIdMap), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected BulkOfNodeGatewayStartedShards newNodeResponse(StreamInput in) throws IOException {
        return new BulkOfNodeGatewayStartedShards(in);
    }

    @Override
    protected NodesGatewayStartedShards newResponse(
        Request request,
        List<BulkOfNodeGatewayStartedShards> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    /**
     * This function is similar to nodeoperation method of {@link TransportNodesListGatewayStartedShards} we loop over
     * the shards here to fetch the shard result in bulk.
     *
     * @param request
     * @return BulkOfNodeGatewayStartedShards
     */
    @Override
    protected BulkOfNodeGatewayStartedShards nodeOperation(NodeRequest request) {
        Map<ShardId, NodeGatewayStartedShards> shardsOnNode = new HashMap<>();
        for (Map.Entry<ShardId, String> shardToCustomDataPathEntry : request.shardIdsWithCustomDataPath.entrySet()) {
            try {
                final ShardId shardId = shardToCustomDataPathEntry.getKey();
                logger.trace("{} loading local shard state info", shardId);
                ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
                    logger,
                    namedXContentRegistry,
                    nodeEnv.availableShardPaths(shardId)
                );
                if (shardStateMetadata != null) {
                    if (indicesService.getShardOrNull(shardId) == null) {
                        final String customDataPath = TransportNodesGatewayStartedShardHelper.getCustomDataPathForShard(
                            logger,
                            shardId,
                            shardToCustomDataPathEntry.getValue(),
                            settings,
                            clusterService
                        );
                        // we don't have an open shard on the store, validate the files on disk are openable
                        Exception shardCorruptionException = TransportNodesGatewayStartedShardHelper.getShardCorruption(
                            logger,
                            nodeEnv,
                            shardId,
                            shardStateMetadata,
                            customDataPath
                        );
                        if (shardCorruptionException != null) {
                            String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                            shardsOnNode.put(
                                shardId,
                                new NodeGatewayStartedShards(allocationId, shardStateMetadata.primary, null, shardCorruptionException)
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
                } else {
                    logger.trace("{} no local shard info found", shardId);
                    shardsOnNode.put(shardId, new NodeGatewayStartedShards(null, false, null));
                }
            } catch (Exception e) {
                throw new OpenSearchException("failed to load started shards", e);
            }
        }
        return new BulkOfNodeGatewayStartedShards(clusterService.localNode(), shardsOnNode);
    }

    /**
     * The nodes request.
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
     * The nodes response.
     *
     * @opensearch.internal
     */
    public static class NodesGatewayStartedShards extends BaseNodesResponse<BulkOfNodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(
            ClusterName clusterName,
            List<BulkOfNodeGatewayStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<BulkOfNodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(BulkOfNodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<BulkOfNodeGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The request.
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
     * The response as stored by TransportNodesListGatewayStartedShards(to maintain backward compatibility).
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
            TransportNodesGatewayStartedShardHelper.NodeGatewayStartedShardsWriteTo(
                out,
                allocationId,
                primary,
                storeException,
                replicationCheckpoint
            );
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
            return TransportNodesGatewayStartedShardHelper.NodeGatewayStartedShardsHashCode(
                allocationId,
                primary,
                storeException,
                replicationCheckpoint
            );
        }

        @Override
        public String toString() {
            return TransportNodesGatewayStartedShardHelper.NodeGatewayStartedShardsToString(
                allocationId,
                primary,
                storeException,
                replicationCheckpoint
            );
        }
    }

    public static class BulkOfNodeGatewayStartedShards extends BaseNodeResponse {
        public Map<ShardId, NodeGatewayStartedShards> getBulkOfNodeGatewayStartedShards() {
            return bulkOfNodeGatewayStartedShards;
        }

        private final Map<ShardId, NodeGatewayStartedShards> bulkOfNodeGatewayStartedShards;

        public BulkOfNodeGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
            this.bulkOfNodeGatewayStartedShards = in.readMap(ShardId::new, NodeGatewayStartedShards::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(bulkOfNodeGatewayStartedShards, (o, k) -> k.writeTo(o), (o, v) -> v.writeTo(o));
        }

        public BulkOfNodeGatewayStartedShards(DiscoveryNode node, Map<ShardId, NodeGatewayStartedShards> bulkOfNodeGatewayStartedShards) {
            super(node);
            this.bulkOfNodeGatewayStartedShards = bulkOfNodeGatewayStartedShards;
        }

    }
}
