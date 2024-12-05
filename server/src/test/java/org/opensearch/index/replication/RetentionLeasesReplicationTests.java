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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.replication;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.seqno.RetentionLeaseSyncAction;
import org.opensearch.index.seqno.RetentionLeaseUtils;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RetentionLeasesReplicationTests extends OpenSearchIndexLevelReplicationTestCase {

    public void testSimpleSyncRetentionLeases() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        try (ReplicationGroup group = createGroup(between(0, 2), settings)) {
            group.startAll();
            List<RetentionLease> leases = new ArrayList<>();
            int iterations = between(1, 100);
            CountDownLatch latch = new CountDownLatch(iterations);
            for (int i = 0; i < iterations; i++) {
                if (leases.isEmpty() == false && rarely()) {
                    RetentionLease leaseToRemove = randomFrom(leases);
                    leases.remove(leaseToRemove);
                    group.removeRetentionLease(leaseToRemove.id(), ActionListener.wrap(latch::countDown));
                } else {
                    RetentionLease newLease = group.addRetentionLease(
                        Integer.toString(i),
                        randomNonNegativeLong(),
                        "test-" + i,
                        ActionListener.wrap(latch::countDown)
                    );
                    leases.add(newLease);
                }
            }
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.version(), equalTo(iterations + group.getReplicas().size() + 1L));
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(
                RetentionLeaseUtils.toMapExcludingPeerRecoveryRetentionLeases(leasesOnPrimary).values(),
                containsInAnyOrder(leases.toArray(new RetentionLease[0]))
            );
            latch.await();
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testOutOfOrderRetentionLeasesRequests() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        int numberOfReplicas = between(1, 2);
        IndexMetadata indexMetadata = buildIndexMetadata(numberOfReplicas, settings, indexMapping);
        try (ReplicationGroup group = new ReplicationGroup(indexMetadata) {
            @Override
            protected void syncRetentionLeases(ShardId shardId, RetentionLeases leases, ActionListener<ReplicationResponse> listener) {
                listener.onResponse(new SyncRetentionLeasesResponse(new RetentionLeaseSyncAction.Request(shardId, leases)));
            }
        }) {
            group.startAll();
            int numLeases = between(1, 10);
            List<RetentionLeaseSyncAction.Request> requests = new ArrayList<>();
            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<ReplicationResponse> future = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, future);
                requests.add(((SyncRetentionLeasesResponse) future.actionGet()).syncRequest);
            }
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            for (IndexShard replica : group.getReplicas()) {
                Randomness.shuffle(requests);
                requests.forEach(request -> group.executeRetentionLeasesSyncRequestOnReplica(request, replica));
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testSyncRetentionLeasesWithPrimaryPromotion() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        int numberOfReplicas = between(2, 4);
        IndexMetadata indexMetadata = buildIndexMetadata(numberOfReplicas, settings, indexMapping);
        try (ReplicationGroup group = new ReplicationGroup(indexMetadata) {
            @Override
            protected void syncRetentionLeases(ShardId shardId, RetentionLeases leases, ActionListener<ReplicationResponse> listener) {
                listener.onResponse(new SyncRetentionLeasesResponse(new RetentionLeaseSyncAction.Request(shardId, leases)));
            }
        }) {
            group.startAll();
            for (IndexShard replica : group.getReplicas()) {
                replica.updateRetentionLeasesOnReplica(group.getPrimary().getRetentionLeases());
            }
            int numLeases = between(1, 100);
            IndexShard newPrimary = randomFrom(group.getReplicas());
            RetentionLeases latestRetentionLeasesOnNewPrimary = newPrimary.getRetentionLeases();
            for (int i = 0; i < numLeases; i++) {
                PlainActionFuture<ReplicationResponse> addLeaseFuture = new PlainActionFuture<>();
                group.addRetentionLease(Integer.toString(i), randomNonNegativeLong(), "test-" + i, addLeaseFuture);
                RetentionLeaseSyncAction.Request request = ((SyncRetentionLeasesResponse) addLeaseFuture.actionGet()).syncRequest;
                for (IndexShard replica : randomSubsetOf(group.getReplicas())) {
                    group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
                    if (newPrimary == replica) {
                        latestRetentionLeasesOnNewPrimary = request.getRetentionLeases();
                    }
                }
            }
            group.promoteReplicaToPrimary(newPrimary).get();
            // we need to make changes to retention leases to sync it to replicas
            // since we don't sync retention leases when promoting a new primary.
            PlainActionFuture<ReplicationResponse> newLeaseFuture = new PlainActionFuture<>();
            group.addRetentionLease("new-lease-after-promotion", randomNonNegativeLong(), "test", newLeaseFuture);
            RetentionLeases leasesOnPrimary = group.getPrimary().getRetentionLeases();
            assertThat(leasesOnPrimary.primaryTerm(), equalTo(group.getPrimary().getOperationPrimaryTerm()));
            assertThat(leasesOnPrimary.version(), equalTo(latestRetentionLeasesOnNewPrimary.version() + 1));
            assertThat(leasesOnPrimary.leases(), hasSize(latestRetentionLeasesOnNewPrimary.leases().size() + 1));
            RetentionLeaseSyncAction.Request request = ((SyncRetentionLeasesResponse) newLeaseFuture.actionGet()).syncRequest;
            for (IndexShard replica : group.getReplicas()) {
                group.executeRetentionLeasesSyncRequestOnReplica(request, replica);
            }
            for (IndexShard replica : group.getReplicas()) {
                assertThat(replica.getRetentionLeases(), equalTo(leasesOnPrimary));
            }
        }
    }

    public void testTurnOffTranslogRetentionAfterAllShardStarted() throws Exception {
        final Settings.Builder settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true);
        if (randomBoolean()) {
            settings.put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomIndexCompatibleVersion(random()));
        }
        try (ReplicationGroup group = createGroup(between(1, 2), settings.build())) {
            group.startAll();
            group.indexDocs(randomIntBetween(1, 10));
            for (IndexShard shard : group) {
                shard.updateShardState(
                    shard.routingEntry(),
                    shard.getOperationPrimaryTerm(),
                    null,
                    1L,
                    group.getPrimary().getReplicationGroup().getInSyncAllocationIds(),
                    group.getPrimary().getReplicationGroup().getRoutingTable(),
                    IndexShardTestUtils.getFakeDiscoveryNodes(shard.routingEntry())
                );
            }
            group.syncGlobalCheckpoint();
            group.flush();
            assertBusy(() -> {
                // we turn off the translog retention policy using the generic threadPool
                for (IndexShard shard : group) {
                    assertThat(shard.translogStats().estimatedNumberOfOperations(), equalTo(0));
                }
            });
        }
    }

    static final class SyncRetentionLeasesResponse extends ReplicationResponse {
        final RetentionLeaseSyncAction.Request syncRequest;

        SyncRetentionLeasesResponse(RetentionLeaseSyncAction.Request syncRequest) {
            this.syncRequest = syncRequest;
        }
    }
}
