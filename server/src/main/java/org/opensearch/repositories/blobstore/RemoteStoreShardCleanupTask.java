/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;

import java.util.Set;

import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

/**
 A Runnable wrapper to make sure that for a given shard only one cleanup task runs at a time.
 */
public class RemoteStoreShardCleanupTask implements Runnable {
    private final Runnable task;
    private final String shardIdentifier;
    final static Set<String> ongoingRemoteDirectoryCleanups = newConcurrentSet();
    private static final Logger staticLogger = LogManager.getLogger(RemoteStoreShardCleanupTask.class);

    public RemoteStoreShardCleanupTask(Runnable task, String indexUUID, ShardId shardId) {
        this.task = task;
        this.shardIdentifier = indexShardIdentifier(indexUUID, shardId);
    }

    private static String indexShardIdentifier(String indexUUID, ShardId shardId) {
        return String.join("/", indexUUID, String.valueOf(shardId.id()));
    }

    @Override
    public void run() {
        // If there is already a same task ongoing for a shard, we need to skip the new task to avoid multiple
        // concurrent cleanup of same shard.
        if (ongoingRemoteDirectoryCleanups.add(shardIdentifier)) {
            try {
                task.run();
            } finally {
                ongoingRemoteDirectoryCleanups.remove(shardIdentifier);
            }
        } else {
            staticLogger.warn("one cleanup task for shard {} is already ongoing, need to skip this task", shardIdentifier);
        }
    }
}
