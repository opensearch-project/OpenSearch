/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.PeerRecoveryStats;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.RecoveryTarget;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class StatsAwareReplicationCollectionTests extends OpenSearchIndexLevelReplicationTestCase {
    static final ReplicationListener listener = new ReplicationListener() {
        @Override
        public void onDone(ReplicationState response) {}

        @Override
        public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {

        }
    };

    public void testStatsInResetRecoveries() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startAll();
            int numDocs = randomIntBetween(1, 15);
            shards.indexDocs(numDocs);
            final StatsAwareReplicationCollection<RecoveryTarget> collection = new StatsAwareReplicationCollection<>(logger, threadPool);
            IndexShard shard = shards.addReplica();
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shard);
            final long resetRecoveryId = collection.reset(recoveryId, TimeValue.timeValueMinutes(60)).getId();
            try (ReplicationCollection.ReplicationRef<RecoveryTarget> newRecoveryRef = collection.get(resetRecoveryId)) {
                shards.recoverReplica(shard, (s, n) -> {
                    assertSame(s, newRecoveryRef.get().indexShard());
                    return newRecoveryRef.get();
                }, false);
            }
            shards.assertAllEqual(numDocs);
            assertNull("recovery is done", collection.get(recoveryId));
            collection.markAsDone(resetRecoveryId);
            PeerRecoveryStats peerRecoveryStats = new PeerRecoveryStats(1, 0, 1, 1, 0);
            assertEquals(peerRecoveryStats.getTotalStartedRecoveries(), collection.stats().getTotalStartedRecoveries());
            assertEquals(peerRecoveryStats.getTotalFailedRecoveries(), collection.stats().getTotalFailedRecoveries());
            assertEquals(peerRecoveryStats.getTotalRetriedRecoveries(), collection.stats().getTotalRetriedRecoveries());
            assertEquals(peerRecoveryStats.getTotalCompletedRecoveries(), collection.stats().getTotalCompletedRecoveries());
            assertEquals(peerRecoveryStats.getTotalCancelledRecoveries(), collection.stats().getTotalCancelledRecoveries());
        }
    }

    public void testStatsInRecoveryCancellation() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final StatsAwareReplicationCollection<RecoveryTarget> collection = new StatsAwareReplicationCollection<>(logger, threadPool);
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            final long recoveryId2 = startRecovery(collection, shards.getPrimaryNode(), shards.addReplica());
            try (ReplicationCollection.ReplicationRef<RecoveryTarget> recoveryRef = collection.get(recoveryId)) {
                ShardId shardId = recoveryRef.get().indexShard().shardId();
                assertTrue("failed to cancel recoveries", collection.cancelForShard(shardId, "test"));
                assertThat("all recoveries should be cancelled", collection.size(), equalTo(0));
            } finally {
                collection.cancel(recoveryId, "meh");
                collection.cancel(recoveryId2, "meh");
            }

            PeerRecoveryStats peerRecoveryStats = new PeerRecoveryStats(2, 0, 0, 0, 1);
            assertEquals(peerRecoveryStats.getTotalStartedRecoveries(), collection.stats().getTotalStartedRecoveries());
            assertEquals(peerRecoveryStats.getTotalFailedRecoveries(), collection.stats().getTotalFailedRecoveries());
            assertEquals(peerRecoveryStats.getTotalRetriedRecoveries(), collection.stats().getTotalRetriedRecoveries());
            assertEquals(peerRecoveryStats.getTotalCompletedRecoveries(), collection.stats().getTotalCompletedRecoveries());
            assertEquals(peerRecoveryStats.getTotalCancelledRecoveries(), collection.stats().getTotalCancelledRecoveries());

        }
    }

    public void testStatsInRecoveryTimeout() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            final StatsAwareReplicationCollection<RecoveryTarget> collection = new StatsAwareReplicationCollection<>(logger, threadPool);
            final AtomicBoolean failed = new AtomicBoolean();
            final CountDownLatch latch = new CountDownLatch(1);
            final long recoveryId = startRecovery(collection, shards.getPrimaryNode(), shards.addReplica(), new ReplicationListener() {
                @Override
                public void onDone(ReplicationState state) {
                    latch.countDown();
                }

                @Override
                public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                    failed.set(true);
                    latch.countDown();
                }
            }, TimeValue.timeValueMillis(100));
            try {
                latch.await(30, TimeUnit.SECONDS);
                assertTrue("recovery failed to timeout", failed.get());
            } finally {
                collection.cancel(recoveryId, "meh");
            }

            PeerRecoveryStats peerRecoveryStats = new PeerRecoveryStats(1, 1, 0, 0, 0);
            assertEquals(peerRecoveryStats.getTotalStartedRecoveries(), collection.stats().getTotalStartedRecoveries());
            assertEquals(peerRecoveryStats.getTotalFailedRecoveries(), collection.stats().getTotalFailedRecoveries());
            assertEquals(peerRecoveryStats.getTotalRetriedRecoveries(), collection.stats().getTotalRetriedRecoveries());
            assertEquals(peerRecoveryStats.getTotalCompletedRecoveries(), collection.stats().getTotalCompletedRecoveries());
            assertEquals(peerRecoveryStats.getTotalCancelledRecoveries(), collection.stats().getTotalCancelledRecoveries());

        }
    }

    long startRecovery(StatsAwareReplicationCollection<RecoveryTarget> collection, DiscoveryNode sourceNode, IndexShard shard) {
        return startRecovery(collection, sourceNode, shard, listener, TimeValue.timeValueMinutes(60));
    }

    long startRecovery(
        StatsAwareReplicationCollection<RecoveryTarget> collection,
        DiscoveryNode sourceNode,
        IndexShard indexShard,
        ReplicationListener listener,
        TimeValue timeValue
    ) {
        final DiscoveryNode rNode = getDiscoveryNode(indexShard.routingEntry().currentNodeId());
        indexShard.markAsRecovering("remote", new RecoveryState(indexShard.routingEntry(), sourceNode, rNode));
        indexShard.prepareForIndexRecovery();
        return collection.start(new RecoveryTarget(indexShard, sourceNode, listener, threadPool), timeValue);
    }
}
