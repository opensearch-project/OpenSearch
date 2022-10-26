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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.LegacyESVersion;
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
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This transport action is used to fetch the shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 *
 * @opensearch.internal
 */
public class TransportNodesListGatewayStartedShards extends TransportNodesAction<
    TransportNodesListGatewayStartedShards.Request,
    TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
    TransportNodesListGatewayStartedShards.NodeRequest,
    TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>
    implements
        AsyncShardFetch.Lister<
            TransportNodesListGatewayStartedShards.NodesGatewayStartedShards,
            TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/started_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShards::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesListGatewayStartedShards(
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
            NodeGatewayStartedShards.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public void list(ShardId shardId, String customDataPath, DiscoveryNode[] nodes, ActionListener<NodesGatewayStartedShards> listener) {
        execute(new Request(shardId, customDataPath, nodes), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeGatewayStartedShards newNodeResponse(StreamInput in) throws IOException {
        return new NodeGatewayStartedShards(in);
    }

    @Override
    protected NodesGatewayStartedShards newResponse(
        Request request,
        List<NodeGatewayStartedShards> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeGatewayStartedShards nodeOperation(NodeRequest request) {
        try {
            final ShardId shardId = request.getShardId();
            logger.trace("{} loading local shard state info", shardId);
            ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
                logger,
                namedXContentRegistry,
                nodeEnv.availableShardPaths(request.shardId)
            );
            if (shardStateMetadata != null) {
                if (indicesService.getShardOrNull(shardId) == null
                    && shardStateMetadata.indexDataLocation == ShardStateMetadata.IndexDataLocation.LOCAL) {
                    final String customDataPath;
                    if (request.getCustomDataPath() != null) {
                        customDataPath = request.getCustomDataPath();
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
                        return new NodeGatewayStartedShards(
                            clusterService.localNode(),
                            allocationId,
                            shardStateMetadata.primary,
                            null,
                            exception
                        );
                    }
                }

                logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
                String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                final IndexShard shard = indicesService.getShardOrNull(shardId);
                return new NodeGatewayStartedShards(
                    clusterService.localNode(),
                    allocationId,
                    shardStateMetadata.primary,
                    shard != null ? shard.getLatestReplicationCheckpoint() : null
                );
            }
            logger.trace("{} no local shard info found", shardId);
            return new NodeGatewayStartedShards(clusterService.localNode(), null, false, null);
        } catch (Exception e) {
            throw new OpenSearchException("failed to load started shards", e);
        }
    }

    /**
     * The nodes request.
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                customDataPath = in.readString();
            } else {
                customDataPath = null;
            }
        }

        public Request(ShardId shardId, String customDataPath, DiscoveryNode[] nodes) {
            super(nodes);
            this.shardId = Objects.requireNonNull(shardId);
            this.customDataPath = Objects.requireNonNull(customDataPath);
        }

        public ShardId shardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                out.writeString(customDataPath);
            }
        }
    }

    /**
     *  The nodes response.
     *
     * @opensearch.internal
     */
    public static class NodesGatewayStartedShards extends BaseNodesResponse<NodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(
            ClusterName clusterName,
            List<NodeGatewayStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The request.
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends TransportRequest {

        private final ShardId shardId;
        @Nullable
        private final String customDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardId = new ShardId(in);
            if (in.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                customDataPath = in.readString();
            } else {
                customDataPath = null;
            }
        }

        public NodeRequest(Request request) {
            this.shardId = Objects.requireNonNull(request.shardId());
            this.customDataPath = Objects.requireNonNull(request.getCustomDataPath());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            if (out.getVersion().onOrAfter(LegacyESVersion.V_7_6_0)) {
                assert customDataPath != null;
                out.writeString(customDataPath);
            }
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * Returns the custom data path that is used to look up information for this shard.
         * Returns an empty string if no custom data path is used for this index.
         * Returns null if custom data path information is not available (due to BWC).
         */
        @Nullable
        public String getCustomDataPath() {
            return customDataPath;
        }
    }

    /**
     * The response.
     *
     * @opensearch.internal
     */
    public static class NodeGatewayStartedShards extends BaseNodeResponse {
        private final String allocationId;
        private final boolean primary;
        private final Exception storeException;
        private final ReplicationCheckpoint replicationCheckpoint;

        public NodeGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
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

        public NodeGatewayStartedShards(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint
        ) {
            this(node, allocationId, primary, replicationCheckpoint, null);
        }

        public NodeGatewayStartedShards(
            DiscoveryNode node,
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            Exception storeException
        ) {
            super(node);
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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
}
