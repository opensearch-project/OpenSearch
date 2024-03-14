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

import java.util.Map;
import java.util.Set;

import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

/**
 A Runnable wrapper to make sure that for a given shard only one cleanup task runs at a time.
 */
public class RemoteStoreShardCleanupTask implements Runnable {
    private final Runnable task;
    private final String shardIdentifier;
    final static Set<String> ongoingRemoteDirectoryCleanups = newConcurrentSet();
    final static Map<String, Runnable> pendingRemoteDirectoryCleanups = newConcurrentMap();
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
        // TODO: this is the best effort at the moment since there is still a known race condition scenario in this
        // method which needs to be handled where one of the thread just came out of while loop and removed the
        // entry from ongoingRemoteDirectoryCleanup, and another thread added new pending task in the map.
        // we need to introduce semaphores/locks to avoid that situation which introduces the overhead of lock object
        // cleanups. however, there will be no scenario where two threads run cleanup for same shard at same time.
        // <issue-link>
        if (pendingRemoteDirectoryCleanups.put(shardIdentifier, task) == null) {
            if (ongoingRemoteDirectoryCleanups.add(shardIdentifier)) {
                while (pendingRemoteDirectoryCleanups.containsKey(shardIdentifier)) {
                    Runnable newTask = pendingRemoteDirectoryCleanups.get(shardIdentifier);
                    pendingRemoteDirectoryCleanups.remove(shardIdentifier);
                    newTask.run();
                }
                ongoingRemoteDirectoryCleanups.remove(shardIdentifier);
            } else {
                staticLogger.debug("one task is already ongoing for shard {}, we can leave entry in pending", shardIdentifier);
            }
        } else {
            staticLogger.debug("one cleanup task for shard {} is already in pending, we can skip this task", shardIdentifier);
        }
    }
}
