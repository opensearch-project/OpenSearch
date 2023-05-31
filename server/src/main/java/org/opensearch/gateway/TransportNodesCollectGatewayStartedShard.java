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
import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.HppcMaps;
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
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * This transport action is used to fetch the all unassigned shard version from each node during primary allocation in {@link GatewayAllocator}.
 * We use this to find out which node holds the latest shard version and which of them used to be a primary in order to allocate
 * shards after node or cluster restarts.
 *
 * @opensearch.internal
 */
public class TransportNodesCollectGatewayStartedShard extends TransportNodesAction<
    TransportNodesCollectGatewayStartedShard.Request,
    TransportNodesCollectGatewayStartedShard.NodesGatewayStartedShards,
    TransportNodesCollectGatewayStartedShard.NodeRequest,
    TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards>
    implements
    AsyncShardsFetchPerNode.Lister<
        TransportNodesCollectGatewayStartedShard.NodesGatewayStartedShards,
        TransportNodesCollectGatewayStartedShard.ListOfNodeGatewayStartedShards> {

    public static final String ACTION_NAME = "internal:gateway/local/collect_shards";
    public static final ActionType<NodesGatewayStartedShards> TYPE = new ActionType<>(ACTION_NAME, NodesGatewayStartedShards::new);

    private final Settings settings;
    private final NodeEnvironment nodeEnv;
    private final IndicesService indicesService;
    private final NamedXContentRegistry namedXContentRegistry;

    @Inject
    public TransportNodesCollectGatewayStartedShard(
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
            ListOfNodeGatewayStartedShards.class
        );
        this.settings = settings;
        this.nodeEnv = env;
        this.indicesService = indicesService;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    @Override
    public void list(DiscoveryNode[] nodes, Map<ShardId,String>shardsIdMap,ActionListener<NodesGatewayStartedShards> listener) {
        execute(new Request(nodes, shardsIdMap), listener);
    }

    @Override
    protected NodeRequest newNodeRequest(Request request) {
        return new NodeRequest(request);
    }

    @Override
    protected ListOfNodeGatewayStartedShards newNodeResponse(StreamInput in) throws IOException {
        return new ListOfNodeGatewayStartedShards(in);
    }

    @Override
    protected NodesGatewayStartedShards newResponse(
        Request request,
        List<ListOfNodeGatewayStartedShards> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesGatewayStartedShards(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ListOfNodeGatewayStartedShards nodeOperation(NodeRequest request) {
        logger.info("TEST->Transport call- +TC");
        Map<ShardId, NodeGatewayStartedShards> shardsOnNode = new HashMap<>();

        /* This is node Operation is same as nodeOperation on TransportNodesListGatewayStartedShards, but it  does over a loop
         for all Unassigned shards
         */
        for (Map.Entry<ShardId, String> unsassignedShardsMap : request.shardIdsWithCustomDataPath.entrySet()) {
            try {
                final ShardId shardId = unsassignedShardsMap.getKey();
                logger.trace("{} loading local shard state info", shardId);
                ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
                    logger,
                    namedXContentRegistry,
                    nodeEnv.availableShardPaths(shardId)
                );
                if (shardStateMetadata != null) {
                    if (indicesService.getShardOrNull(shardId) == null) {
                        final String customDataPath;
                        if (unsassignedShardsMap.getValue() != null) {
                            customDataPath = unsassignedShardsMap.getValue();
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
                            shardsOnNode.put(shardId, new NodeGatewayStartedShards(
                                allocationId,
                                shardStateMetadata.primary,
                                null,
                                exception
                            ));
                        }
                    }

                    logger.info("TEST---> {} shard state info found: [{}]", shardId, shardStateMetadata);
                    String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
                    final IndexShard shard = indicesService.getShardOrNull(shardId);
                    shardsOnNode.put(shardId, new NodeGatewayStartedShards(
                        allocationId,
                        shardStateMetadata.primary,
                        shard != null ? shard.getLatestReplicationCheckpoint() : null
                    ));
                }
                else {
                    logger.info("TEST--> {} no local shard info found", shardId);
                    shardsOnNode.put(shardId, new NodeGatewayStartedShards(null, false, null));
                }
            } catch (Exception e) {
                throw new OpenSearchException("failed to load started shards", e);
            }
        }
        return new ListOfNodeGatewayStartedShards(clusterService.localNode(), shardsOnNode);
    }

    /**
     * The nodes request.
     *
     * @opensearch.internal
     */
    public static class Request extends BaseNodesRequest<Request> {


        private final Map<ShardId, String> shardIdStringMap;

        public Request(StreamInput in) throws IOException {
            super(in);
            shardIdStringMap=in.readMap(ShardId::new, StreamInput::readString);
        }

        public Request(DiscoveryNode[] nodes, Map<ShardId, String> shardIdStringMap) {
            super(nodes);
            this.shardIdStringMap= Objects.requireNonNull(shardIdStringMap);
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(shardIdStringMap, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        }

        public Map<ShardId, String> getShardIdsMap() {
            return shardIdStringMap;
        }
    }

    /**
     *  The nodes response.
     *
     * @opensearch.internal
     */
    public static class NodesGatewayStartedShards extends BaseNodesResponse<ListOfNodeGatewayStartedShards> {

        public NodesGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
        }

        public NodesGatewayStartedShards(
            ClusterName clusterName,
            List<ListOfNodeGatewayStartedShards> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<ListOfNodeGatewayStartedShards> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(ListOfNodeGatewayStartedShards::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<ListOfNodeGatewayStartedShards> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    /**
     * The request.
     *
     * @opensearch.internal
     */
    public static class NodeRequest extends BaseNodeRequest {


        private final Map<ShardId, String> shardIdsWithCustomDataPath;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            shardIdsWithCustomDataPath=in.readMap(ShardId::new, StreamInput::readString);
        }

        public NodeRequest(Request request) {

            this.shardIdsWithCustomDataPath=Objects.requireNonNull(request.getShardIdsMap());
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
    public static class NodeGatewayStartedShards  {
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

        public NodeGatewayStartedShards(
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint
        ) {
            this( allocationId, primary, replicationCheckpoint, null);
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

    public static class ListOfNodeGatewayStartedShards extends BaseNodeResponse {
        public Map<ShardId, NodeGatewayStartedShards> getListOfNodeGatewayStartedShards() {
            return listOfNodeGatewayStartedShards;
        }

        private final Map<ShardId, NodeGatewayStartedShards> listOfNodeGatewayStartedShards;
        public ListOfNodeGatewayStartedShards(StreamInput in) throws IOException {
            super(in);
            this.listOfNodeGatewayStartedShards = in.readMap(ShardId::new, NodeGatewayStartedShards::new);
        }
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(listOfNodeGatewayStartedShards, (o, k) -> k.writeTo(o),(o,v)->v.writeTo(o));
        }

        public ListOfNodeGatewayStartedShards(DiscoveryNode node, Map<ShardId, NodeGatewayStartedShards> listOfNodeGatewayStartedShards) {
            super(node);
            this.listOfNodeGatewayStartedShards=listOfNodeGatewayStartedShards;
        }

    }
}
