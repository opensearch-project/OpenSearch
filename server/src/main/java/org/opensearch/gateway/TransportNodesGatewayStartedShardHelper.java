/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Objects;

/**
 * This class has the common code used in {@link TransportNodesListGatewayStartedShards} and
 * {@link TransportNodesListGatewayStartedShardsBatch} to get the shard info on the local node.
 * <p>
 * This class should not be used to add more functions and will be removed when the
 * {@link TransportNodesListGatewayStartedShards} will be deprecated and all the code will be moved to
 * {@link TransportNodesListGatewayStartedShardsBatch}
 *
 * @opensearch.internal
 */
public class TransportNodesGatewayStartedShardHelper {
    public static final String INDEX_NOT_FOUND = "node doesn't have meta data for index";

    public static GatewayStartedShard getShardInfoOnLocalNode(
        Logger logger,
        final ShardId shardId,
        NamedXContentRegistry namedXContentRegistry,
        NodeEnvironment nodeEnv,
        IndicesService indicesService,
        String shardDataPathInRequest,
        Settings settings,
        ClusterService clusterService
    ) throws IOException {
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
                if (shardDataPathInRequest != null) {
                    customDataPath = shardDataPathInRequest;
                } else {
                    // TODO: Fallback for BWC with older OpenSearch versions.
                    // Remove once request.getCustomDataPath() always returns non-null
                    final IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
                    if (metadata != null) {
                        customDataPath = new IndexSettings(metadata, settings).customDataPath();
                    } else {
                        logger.trace("{} node doesn't have meta data for the requests index", shardId);
                        throw new OpenSearchException(INDEX_NOT_FOUND + " " + shardId.getIndex());
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
                    return new GatewayStartedShard(allocationId, shardStateMetadata.primary, null, exception);
                }
            }

            logger.debug("{} shard state info found: [{}]", shardId, shardStateMetadata);
            String allocationId = shardStateMetadata.allocationId != null ? shardStateMetadata.allocationId.getId() : null;
            final IndexShard shard = indicesService.getShardOrNull(shardId);
            return new GatewayStartedShard(
                allocationId,
                shardStateMetadata.primary,
                shard != null ? shard.getLatestReplicationCheckpoint() : null
            );
        }
        logger.trace("{} no local shard info found", shardId);
        return new GatewayStartedShard(null, false, null);
    }

    /**
     * This class encapsulates the metadata about a started shard that needs to be persisted or sent between nodes.
     * This is used in {@link TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShardsBatch} to construct the response for each node, instead of
     * {@link TransportNodesListGatewayStartedShards.NodeGatewayStartedShards} because we don't need to save an extra
     * {@link DiscoveryNode} object like in {@link TransportNodesListGatewayStartedShards.NodeGatewayStartedShards}
     * which reduces memory footprint of its objects.
     *
     * @opensearch.internal
     */
    public static class GatewayStartedShard {
        private final String allocationId;
        private final boolean primary;
        private final Exception storeException;
        private final ReplicationCheckpoint replicationCheckpoint;

        public GatewayStartedShard(StreamInput in) throws IOException {
            allocationId = in.readOptionalString();
            primary = in.readBoolean();
            if (in.readBoolean()) {
                storeException = in.readException();
            } else {
                storeException = null;
            }
            if (in.readBoolean()) {
                replicationCheckpoint = new ReplicationCheckpoint(in);
            } else {
                replicationCheckpoint = null;
            }
        }

        public GatewayStartedShard(String allocationId, boolean primary, ReplicationCheckpoint replicationCheckpoint) {
            this(allocationId, primary, replicationCheckpoint, null);
        }

        public GatewayStartedShard(
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
            if (replicationCheckpoint != null) {
                out.writeBoolean(true);
                replicationCheckpoint.writeTo(out);
            } else {
                out.writeBoolean(false);
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

            GatewayStartedShard that = (GatewayStartedShard) o;

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

        public static boolean isEmpty(GatewayStartedShard gatewayStartedShard) {
            return gatewayStartedShard.allocationId() == null
                && gatewayStartedShard.primary() == false
                && gatewayStartedShard.storeException() == null
                && gatewayStartedShard.replicationCheckpoint() == null;
        }
    }

    /**
     * This class extends the {@link GatewayStartedShard} which contains all necessary shard metadata like
     * allocationId and replication checkpoint. It also has DiscoveryNode which is needed by
     * {@link PrimaryShardAllocator} and {@link PrimaryShardBatchAllocator} to make allocation decision.
     * This class removes the dependency of
     * {@link TransportNodesListGatewayStartedShards.NodeGatewayStartedShards} to make allocation decisions by
     * {@link PrimaryShardAllocator} or {@link PrimaryShardBatchAllocator}.
     */
    public static class NodeGatewayStartedShard extends GatewayStartedShard {

        private final DiscoveryNode node;

        public NodeGatewayStartedShard(
            String allocationId,
            boolean primary,
            ReplicationCheckpoint replicationCheckpoint,
            Exception storeException,
            DiscoveryNode node
        ) {
            super(allocationId, primary, replicationCheckpoint, storeException);
            this.node = node;
        }

        public DiscoveryNode getNode() {
            return node;
        }
    }
}
