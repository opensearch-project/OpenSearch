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
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;

/**
 * This class has the common code used in TransportNodesBulkListGatewayStartedShards and TransportNodesListGatewayStartedShards
 */
public class TransportNodesGatewayStartedShardHelper {

    /**
     * Helper function for getting the data path of the shard that is used to look up information for this shard.
     * If the dataPathInRequest passed to the method is not empty then same is returned. Else the custom data path is returned
     * from the indexSettings fetched from the cluster state metadata for the specified shard.
     *
     * @param logger
     * @param shardId
     * @param dataPathInRequest
     * @param settings
     * @param clusterService
     * @return String
     */
    public static String getCustomDataPathForShard(
        Logger logger,
        ShardId shardId,
        String dataPathInRequest,
        Settings settings,
        ClusterService clusterService
    ) {
        if (dataPathInRequest != null) return dataPathInRequest;
        // TODO: Fallback for BWC with older OpenSearch versions.
        // Remove once request.getCustomDataPath() always returns non-null
        final IndexMetadata metadata = clusterService.state().metadata().index(shardId.getIndex());
        if (metadata != null) {
            return new IndexSettings(metadata, settings).customDataPath();
        } else {
            logger.trace("{} node doesn't have meta data for the requests index", shardId);
            throw new OpenSearchException("node doesn't have meta data for index " + shardId.getIndex());
        }
    }

    /**
     * Helper function for checking if the shard file exists and is not corrupted. We return the specific exception if
     * the shard file is corrupted. else null value is returned.
     *
     * @param logger
     * @param nodeEnv
     * @param shardId
     * @param shardStateMetadata
     * @param customDataPath
     * @return Exception
     */
    public static Exception getShardCorruption(
        Logger logger,
        NodeEnvironment nodeEnv,
        ShardId shardId,
        ShardStateMetadata shardStateMetadata,
        String customDataPath
    ) {
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
            return exception;
        }
        return null;
    }

    /**
     * Helper function for getting the string representation of the NodeGatewayStartedShards object
     *
     * @param allocationId
     * @param primary
     * @param storeException
     * @param replicationCheckpoint
     * @return String
     */
    public static String NodeGatewayStartedShardsToString(
        String allocationId,
        boolean primary,
        Exception storeException,
        ReplicationCheckpoint replicationCheckpoint
    ) {
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

    /**
     * Helper function for computing the hashcode of the NodeGatewayStartedShards object
     *
     * @param allocationId
     * @param primary
     * @param storeException
     * @param replicationCheckpoint
     * @return int
     */
    public static int NodeGatewayStartedShardsHashCode(
        String allocationId,
        boolean primary,
        Exception storeException,
        ReplicationCheckpoint replicationCheckpoint
    ) {
        int result = (allocationId != null ? allocationId.hashCode() : 0);
        result = 31 * result + (primary ? 1 : 0);
        result = 31 * result + (storeException != null ? storeException.hashCode() : 0);
        result = 31 * result + (replicationCheckpoint != null ? replicationCheckpoint.hashCode() : 0);
        return result;
    }

    /**
     * Helper function for NodeGatewayStartedShardsWriteTo method
     *
     * @param out
     * @param allocationId
     * @param primary
     * @param storeException
     * @param replicationCheckpoint
     * @throws IOException
     */
    public static void NodeGatewayStartedShardsWriteTo(
        StreamOutput out,
        String allocationId,
        boolean primary,
        Exception storeException,
        ReplicationCheckpoint replicationCheckpoint
    ) throws IOException {
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
}
