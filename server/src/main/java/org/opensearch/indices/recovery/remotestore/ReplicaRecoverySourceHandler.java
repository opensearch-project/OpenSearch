/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery.remotestore;

import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySourceHandler;
import org.opensearch.indices.recovery.RecoveryTargetHandler;
import org.opensearch.indices.recovery.StartRecoveryRequest;
import org.opensearch.threadpool.ThreadPool;

/**
 * This handler is used when peer recovery target is a remote store enabled replica.
 */
public class ReplicaRecoverySourceHandler extends RecoverySourceHandler {
    public ReplicaRecoverySourceHandler(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        ThreadPool threadPool,
        StartRecoveryRequest request,
        int fileChunkSizeInBytes,
        int maxConcurrentFileChunks,
        int maxConcurrentOperations
    ) {
        super(shard, recoveryTarget, threadPool, request, fileChunkSizeInBytes, maxConcurrentFileChunks, maxConcurrentOperations);
    }
}
