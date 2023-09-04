/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.Version;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;

public class SegmentReplicationReplicaStatsTrackerTest extends OpenSearchTestCase {

    private final ShardId shardId = new ShardId("index", "_na_", 1);
    long initialInfosVersion = 1;
    private final ReplicationCheckpoint initialCheckpoint = new ReplicationCheckpoint(
        shardId,
        1L,
        1L,
        1L,
        1L,
        "codec",
        Collections.emptyMap()
    );

    public void testStatsComputedAsCheckpointsAreAdded() {
        SegmentReplicationReplicaStatsTracker tracker = new SegmentReplicationReplicaStatsTracker(initialCheckpoint);
        final StoreFileMetadata segment_0 = new StoreFileMetadata("_0.si", 20, "test", Version.CURRENT.luceneVersion);
        final StoreFileMetadata segment_1 = new StoreFileMetadata("_1.si", 20, "test", Version.CURRENT.luceneVersion);
        Map<String, StoreFileMetadata> metadataMap = Map.of("_0.si", segment_0);
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            shardId,
            1L,
            1L,
            initialInfosVersion++,
            1L,
            "codec",
            metadataMap
        );
        tracker.addCheckpoint(checkpoint);
        assertEquals(checkpoint, tracker.getLatestReceivedCheckpoint());
        assertEquals(1, tracker.getActiveTimers().size());
        assertEquals(20L, tracker.getBytesBehind(initialCheckpoint));

        metadataMap = Map.of("_0.si", segment_0, "_1.si", segment_1);
        final ReplicationCheckpoint checkpoint_2 = new ReplicationCheckpoint(
            shardId,
            1L,
            1L,
            initialInfosVersion++,
            1L,
            "codec",
            metadataMap
        );
        tracker.addCheckpoint(checkpoint_2);
        assertEquals(checkpoint_2, tracker.getLatestReceivedCheckpoint());
        assertEquals(2, tracker.getActiveTimers().size());
        assertEquals(40L, tracker.getBytesBehind(initialCheckpoint));

        if (randomBoolean()) {
            tracker.clearUpToCheckpoint(checkpoint);
            assertEquals(1, tracker.getActiveTimers().size());
            assertEquals(20L, tracker.getBytesBehind(checkpoint));
        }

        tracker.clearUpToCheckpoint(checkpoint_2);
        assertEquals(0, tracker.getActiveTimers().size());
        assertEquals(0L, tracker.getBytesBehind(checkpoint_2));
    }

    public void testAddSameCheckpointTwice() {
        SegmentReplicationReplicaStatsTracker tracker = new SegmentReplicationReplicaStatsTracker(initialCheckpoint);
        final StoreFileMetadata segment_0 = new StoreFileMetadata("_0.si", 20, "test", Version.CURRENT.luceneVersion);
        Map<String, StoreFileMetadata> metadataMap = Map.of("_0.si", segment_0);
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            shardId,
            1L,
            1L,
            initialInfosVersion++,
            1L,
            "codec",
            metadataMap
        );
        tracker.addCheckpoint(checkpoint);
        tracker.addCheckpoint(checkpoint);
        assertEquals(checkpoint, tracker.getLatestReceivedCheckpoint());
        assertEquals(1, tracker.getActiveTimers().size());
        assertEquals(20L, tracker.getBytesBehind(initialCheckpoint));
    }
}
