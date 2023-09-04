/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.indices.replication.common.SegmentReplicationLagTimer;

import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * Class responsible for capturing Segment Replication stats for a replic shard.
 *
 * @opensearch.internal
 */
public class SegmentReplicationReplicaStatsTracker {

    public static final Logger logger = LogManager.getLogger(SegmentReplicationReplicaStatsTracker.class);

    private final Deque<TimedReplicationCheckpoint> checkpointTimers;

    public SegmentReplicationReplicaStatsTracker(ReplicationCheckpoint currentCheckpoint) {
        this.checkpointTimers = ConcurrentCollections.newDeque();
    }

    @Nullable
    public ReplicationCheckpoint getLatestReceivedCheckpoint() {
        return Optional.ofNullable(checkpointTimers.peekLast()).map(TimedReplicationCheckpoint::getCheckpoint).orElse(null);
    }

    /**
     * Add a new checkpoint received from the primary Shard.
     * @param checkpoint - {@link ReplicationCheckpoint}
     */
    public synchronized void addCheckpoint(ReplicationCheckpoint checkpoint) {
        final TimedReplicationCheckpoint peek = checkpointTimers.peekLast();
        if (peek == null || checkpoint.isAheadOf(peek.checkpoint)) {
            final TimedReplicationCheckpoint timedCheckpoint = new TimedReplicationCheckpoint(checkpoint);
            timedCheckpoint.timer.start();
            checkpointTimers.add(timedCheckpoint);
        }
    }

    /**
     * Clear timers up to the given checkpoint
     * @param checkpoint {@link ReplicationCheckpoint}
     */
    public synchronized void clearUpToCheckpoint(ReplicationCheckpoint checkpoint) {
        while (checkpointTimers.peekFirst() != null && checkpointTimers.peekFirst().checkpoint.isAheadOf(checkpoint) == false) {
            checkpointTimers.removeFirst();
        }
    }

    /**
     * Compute the amount of bytes the replica is to its latest received checkpoint.
     * @param latest {@link ReplicationCheckpoint} latest local checkpoint.
     * @return bytes behind
     */
    public long getBytesBehind(ReplicationCheckpoint latest) {
        return getMissingFiles(latest).stream().mapToLong(StoreFileMetadata::length).sum();
    }

    public long getReplicationLag() {
        return checkpointTimers.stream().map(TimedReplicationCheckpoint::getTimer).mapToLong(ReplicationTimer::time).max().orElse(0);
    }

    // for tests
    Queue<TimedReplicationCheckpoint> getActiveTimers() {
        return checkpointTimers;
    }

    private List<StoreFileMetadata> getMissingFiles(ReplicationCheckpoint latestReplicationCheckpoint) {
        final ReplicationCheckpoint latestReceivedCheckpoint = getLatestReceivedCheckpoint();
        if (latestReplicationCheckpoint != null && latestReceivedCheckpoint != null) {
            return Store.segmentReplicationDiff(
                latestReplicationCheckpoint.getMetadataMap(),
                latestReceivedCheckpoint.getMetadataMap()
            ).missing;
        }
        return Collections.emptyList();
    }

    private static class TimedReplicationCheckpoint {

        final ReplicationCheckpoint checkpoint;
        final ReplicationTimer timer;

        TimedReplicationCheckpoint(ReplicationCheckpoint checkpoint) {
            this.checkpoint = checkpoint;
            timer = new SegmentReplicationLagTimer();
        }

        ReplicationTimer getTimer() {
            return timer;
        }

        ReplicationCheckpoint getCheckpoint() {
            return checkpoint;
        }

        @Override
        public String toString() {
            return "TimedReplicationCheckpoint{" + "checkpoint=" + checkpoint + '}';
        }
    }
}
