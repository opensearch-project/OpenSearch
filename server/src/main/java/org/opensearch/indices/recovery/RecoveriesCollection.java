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

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;

/**
 * This class holds a collection of all on going recoveries on the current node (i.e., the node is the target node
 * of those recoveries). The class is used to guarantee concurrent semantics such that once a recoveries was done/cancelled/failed
 * no other thread will be able to find it. Last, the {@link org.opensearch.indices.recovery.ReplicationCollection.ReplicationRef} inner class verifies that recovery temporary files
 * and store will only be cleared once on going usage is finished.
 */
public class RecoveriesCollection extends ReplicationCollection<RecoveryTarget> {

    public RecoveriesCollection(Logger logger, ThreadPool threadPool) {
        super(logger, threadPool);
    }

    public long startRecovery(IndexShard indexShard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener, TimeValue activityTimeout) {
        return startReplication(new RecoveryTarget(indexShard, sourceNode, listener), activityTimeout);
    }

    /**
     * Resets the recovery and performs a recovery restart on the currently recovering index shard
     *
     * @return newly created RecoveryTarget
     * @see IndexShard#performRecoveryRestart()
     */
    public RecoveryTarget resetRecovery(final long recoveryId, final TimeValue activityTimeout) {
        RecoveryTarget oldRecoveryTarget = null;
        final RecoveryTarget newRecoveryTarget;

        try {
            synchronized (getOngoingReplications()) {
                // swap recovery targets in a synchronized block to ensure that the newly added recovery target is picked up by
                // cancelRecoveriesForShard whenever the old recovery target is picked up
                oldRecoveryTarget = getOngoingReplications().remove(recoveryId);
                if (oldRecoveryTarget == null) {
                    return null;
                }

                // Copy the RecoveryTarget to retry recovery from the same source node onto the same shard and using the same listener.
                newRecoveryTarget = new RecoveryTarget(
                    oldRecoveryTarget.indexShard(),
                    oldRecoveryTarget.sourceNode(),
                    oldRecoveryTarget.getListener()
                );
                startRecoveryInternal(newRecoveryTarget, activityTimeout);
            }

            // Closes the current recovery target
            boolean successfulReset = oldRecoveryTarget.resetRecovery(newRecoveryTarget.cancellableThreads());
            if (successfulReset) {
                logger.trace(
                    "{} restarted recovery from {}, id [{}], previous id [{}]",
                    newRecoveryTarget.indexShard().shardId(),
                    newRecoveryTarget.sourceNode(),
                    newRecoveryTarget.getId(),
                    oldRecoveryTarget.getId()
                );
                return newRecoveryTarget;
            } else {
                logger.trace(
                    "{} recovery could not be reset as it is already cancelled, recovery from {}, id [{}], previous id [{}]",
                    newRecoveryTarget.indexShard().shardId(),
                    newRecoveryTarget.sourceNode(),
                    newRecoveryTarget.getId(),
                    oldRecoveryTarget.getId()
                );
                cancelRecovery(newRecoveryTarget.getId(), "recovery cancelled during reset");
                return null;
            }
        } catch (Exception e) {
            // fail shard to be safe
            oldRecoveryTarget.notifyListener(new RecoveryFailedException(oldRecoveryTarget.state(), "failed to retry recovery", e), true);
            return null;
        }
    }

    public ReplicationRef<RecoveryTarget> getRecovery(long recoveryId) {
        return getReplication(recoveryId);
    }

    public ReplicationRef<RecoveryTarget> getRecoverySafe(long recoveryId, ShardId shardId) {
        return getReplicationSafe(recoveryId, shardId);
    }
}
