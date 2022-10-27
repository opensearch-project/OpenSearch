/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;

/**
 * This handler is used for the peer recovery when there is no remote store available for segments/translogs. TODO -
 * Add more details as this is refactored further.
 */
public class DefaultRecoverySourceHandler extends RecoverySourceHandler {

    public DefaultRecoverySourceHandler(
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
