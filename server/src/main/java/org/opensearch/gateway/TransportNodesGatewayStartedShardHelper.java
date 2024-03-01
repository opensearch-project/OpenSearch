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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.shard.ShardStateMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;

import java.io.IOException;

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
    public static TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard getShardInfoOnLocalNode(
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
                    return new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard(
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
            return new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard(
                allocationId,
                shardStateMetadata.primary,
                shard != null ? shard.getLatestReplicationCheckpoint() : null
            );
        }
        logger.trace("{} no local shard info found", shardId);
        return new TransportNodesListGatewayStartedShardsBatch.NodeGatewayStartedShard(null, false, null);
    }
}
