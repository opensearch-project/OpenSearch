/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.shard.IndexShard;

/**
 * Factory that supplies {@link RecoverySourceHandler}.
 *
 * @opensearch.internal
 */
public class RecoverySourceHandlerFactory {

    private static final Logger logger = LogManager.getLogger(RecoverySourceHandlerFactory.class);


    public static RecoverySourceHandler create(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        StartRecoveryRequest request,
        RecoverySettings recoverySettings
    ) {
        logger.info("RecoverySourceHandlerFactory.create called for shard [{}], isPrimaryRelocation [{}], targetNode [{}]",
            shard.shardId(), request.isPrimaryRelocation(), request.targetNode());

        boolean isReplicaRecoveryWithRemoteTranslog = request.isPrimaryRelocation() == false && request.targetNode().isRemoteStoreNode();
        if (isReplicaRecoveryWithRemoteTranslog) {
            return new RemoteStorePeerRecoverySourceHandler(
                shard,
                recoveryTarget,
                shard.getThreadPool(),
                request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(),
                recoverySettings.getMaxConcurrentOperations()
            );
        } else {
            return new LocalStorePeerRecoverySourceHandler(
                shard,
                recoveryTarget,
                shard.getThreadPool(),
                request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(),
                recoverySettings.getMaxConcurrentOperations()
            );
        }
    }
}
