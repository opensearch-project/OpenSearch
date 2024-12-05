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

package org.opensearch.index.seqno;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.WriteStateException;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

public class ReplicationTrackerRetentionLeaseTests extends ReplicationTrackerTestCase {

    public void testAddOrRenewRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final int length = randomIntBetween(0, 8);
        final long[] minimumRetainingSequenceNumbers = new long[length];
        for (int i = 0; i < length; i++) {
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            replicationTracker.addRetentionLease(
                Integer.toString(i),
                minimumRetainingSequenceNumbers[i],
                "test-" + i,
                ActionListener.wrap(() -> {})
            );
            assertRetentionLeases(replicationTracker, i + 1, minimumRetainingSequenceNumbers, primaryTerm, 2 + i, true, false);
        }

        for (int i = 0; i < length; i++) {
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            minimumRetainingSequenceNumbers[i] = randomLongBetween(minimumRetainingSequenceNumbers[i], Long.MAX_VALUE);
            replicationTracker.renewRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
            assertRetentionLeases(replicationTracker, length, minimumRetainingSequenceNumbers, primaryTerm, 2 + length + i, true, false);
        }
    }

    public void testAddDuplicateRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);
        replicationTracker.addRetentionLease(id, retainingSequenceNumber, source, ActionListener.wrap(() -> {}));
        final long nextRetaininSequenceNumber = randomLongBetween(retainingSequenceNumber, Long.MAX_VALUE);
        final RetentionLeaseAlreadyExistsException e = expectThrows(
            RetentionLeaseAlreadyExistsException.class,
            () -> replicationTracker.addRetentionLease(id, nextRetaininSequenceNumber, source, ActionListener.wrap(() -> {}))
        );
        assertThat(e, hasToString(containsString("retention lease with ID [" + id + "] already exists")));
    }

    public void testRenewNotFoundRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final String id = randomAlphaOfLength(8);
        final RetentionLeaseNotFoundException e = expectThrows(
            RetentionLeaseNotFoundException.class,
            () -> replicationTracker.renewRetentionLease(id, randomNonNegativeLong(), randomAlphaOfLength(8))
        );
        assertThat(e, hasToString(containsString("retention lease with ID [" + id + "] not found")));
    }

    public void testAddRetentionLeaseCausesRetentionLeaseSync() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final Map<String, Long> retainingSequenceNumbers = new HashMap<>();
        final AtomicBoolean invoked = new AtomicBoolean();
        final AtomicReference<ReplicationTracker> reference = new AtomicReference<>();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {
                // we do not want to hold a lock on the replication tracker in the callback!
                assertFalse(Thread.holdsLock(reference.get()));
                invoked.set(true);
                assertThat(
                    leases.leases().stream().collect(Collectors.toMap(RetentionLease::id, RetentionLease::retainingSequenceNumber)),
                    equalTo(retainingSequenceNumbers)
                );
            },
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        reference.set(replicationTracker);
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        retainingSequenceNumbers.put(ReplicationTracker.getPeerRecoveryRetentionLeaseId(nodeIdFromAllocationId(allocationId)), 0L);

        final int length = randomIntBetween(0, 8);
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            retainingSequenceNumbers.put(id, retainingSequenceNumber);
            replicationTracker.addRetentionLease(id, retainingSequenceNumber, "test", ActionListener.wrap(() -> {}));
            // assert that the new retention lease callback was invoked
            assertTrue(invoked.get());

            // reset the invocation marker so that we can assert the callback was not invoked when renewing the lease
            invoked.set(false);
            replicationTracker.renewRetentionLease(id, retainingSequenceNumber, "test");
            assertFalse(invoked.get());
        }
    }

    public void testRemoveRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final int length = randomIntBetween(0, 8);
        final long[] minimumRetainingSequenceNumbers = new long[length];
        for (int i = 0; i < length; i++) {
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            replicationTracker.addRetentionLease(
                Integer.toString(i),
                minimumRetainingSequenceNumbers[i],
                "test-" + i,
                ActionListener.wrap(() -> {})
            );
        }

        for (int i = 0; i < length; i++) {
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            /*
             * Remove from the end since it will make the following assertion easier; we want to ensure that only the intended lease was
             * removed.
             */
            replicationTracker.removeRetentionLease(Integer.toString(length - i - 1), ActionListener.wrap(() -> {}));
            assertRetentionLeases(
                replicationTracker,
                length - i - 1,
                minimumRetainingSequenceNumbers,
                primaryTerm,
                2 + length + i,
                true,
                false
            );
        }
    }

    public void testCloneRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final AtomicReference<ReplicationTracker> replicationTrackerRef = new AtomicReference<>();
        final AtomicLong timeReference = new AtomicLong();
        final AtomicBoolean synced = new AtomicBoolean();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomLongBetween(1, Long.MAX_VALUE),
            UNASSIGNED_SEQ_NO,
            value -> {},
            timeReference::get,
            (leases, listener) -> {
                assertFalse(Thread.holdsLock(replicationTrackerRef.get()));
                assertTrue(synced.compareAndSet(false, true));
                listener.onResponse(new ReplicationResponse());
            },
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTrackerRef.set(replicationTracker);
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);

        final long addTime = randomLongBetween(timeReference.get(), Long.MAX_VALUE);
        timeReference.set(addTime);
        final long minimumRetainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        final PlainActionFuture<ReplicationResponse> addFuture = new PlainActionFuture<>();
        replicationTracker.addRetentionLease("source", minimumRetainingSequenceNumber, "test-source", addFuture);
        addFuture.actionGet();
        assertTrue(synced.get());
        synced.set(false);

        final long cloneTime = randomLongBetween(timeReference.get(), Long.MAX_VALUE);
        timeReference.set(cloneTime);
        final PlainActionFuture<ReplicationResponse> cloneFuture = new PlainActionFuture<>();
        final RetentionLease clonedLease = replicationTracker.cloneRetentionLease("source", "target", cloneFuture);
        cloneFuture.actionGet();
        assertTrue(synced.get());
        synced.set(false);

        assertThat(clonedLease.id(), equalTo("target"));
        assertThat(clonedLease.retainingSequenceNumber(), equalTo(minimumRetainingSequenceNumber));
        assertThat(clonedLease.timestamp(), equalTo(cloneTime));
        assertThat(clonedLease.source(), equalTo("test-source"));

        assertThat(replicationTracker.getRetentionLeases().get("target"), equalTo(clonedLease));
    }

    public void testCloneNonexistentRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomLongBetween(1, Long.MAX_VALUE),
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);

        assertThat(
            expectThrows(
                RetentionLeaseNotFoundException.class,
                () -> replicationTracker.cloneRetentionLease("nonexistent-lease-id", "target", ActionListener.wrap(() -> {}))
            ).getMessage(),
            equalTo("retention lease with ID [nonexistent-lease-id] not found")
        );
    }

    public void testCloneDuplicateRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomLongBetween(1, Long.MAX_VALUE),
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);

        replicationTracker.addRetentionLease("source", randomLongBetween(0L, Long.MAX_VALUE), "test-source", ActionListener.wrap(() -> {}));
        replicationTracker.addRetentionLease("exists", randomLongBetween(0L, Long.MAX_VALUE), "test-source", ActionListener.wrap(() -> {}));

        assertThat(
            expectThrows(
                RetentionLeaseAlreadyExistsException.class,
                () -> replicationTracker.cloneRetentionLease("source", "exists", ActionListener.wrap(() -> {}))
            ).getMessage(),
            equalTo("retention lease with ID [exists] already exists")
        );
    }

    public void testRemoveNotFound() {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final String id = randomAlphaOfLength(8);
        final RetentionLeaseNotFoundException e = expectThrows(
            RetentionLeaseNotFoundException.class,
            () -> replicationTracker.removeRetentionLease(id, ActionListener.wrap(() -> {}))
        );
        assertThat(e, hasToString(containsString("retention lease with ID [" + id + "] not found")));
    }

    public void testRemoveRetentionLeaseCausesRetentionLeaseSync() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final Map<String, Long> retainingSequenceNumbers = new HashMap<>();
        final AtomicBoolean invoked = new AtomicBoolean();
        final AtomicReference<ReplicationTracker> reference = new AtomicReference<>();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {
                // we do not want to hold a lock on the replication tracker in the callback!
                assertFalse(Thread.holdsLock(reference.get()));
                invoked.set(true);
                assertThat(
                    leases.leases().stream().collect(Collectors.toMap(RetentionLease::id, RetentionLease::retainingSequenceNumber)),
                    equalTo(retainingSequenceNumbers)
                );
            },
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        reference.set(replicationTracker);
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        retainingSequenceNumbers.put(ReplicationTracker.getPeerRecoveryRetentionLeaseId(nodeIdFromAllocationId(allocationId)), 0L);

        final int length = randomIntBetween(0, 8);
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            retainingSequenceNumbers.put(id, retainingSequenceNumber);
            replicationTracker.addRetentionLease(id, retainingSequenceNumber, "test", ActionListener.wrap(() -> {}));
            // assert that the new retention lease callback was invoked
            assertTrue(invoked.get());

            // reset the invocation marker so that we can assert the callback was not invoked when removing the lease
            invoked.set(false);
            retainingSequenceNumbers.remove(id);
            replicationTracker.removeRetentionLease(id, ActionListener.wrap(() -> {}));
            assertTrue(invoked.get());
        }
    }

    public void testExpirationOnPrimary() {
        runExpirationTest(true);
    }

    public void testExpirationOnReplica() {
        runExpirationTest(false);
    }

    private void runExpirationTest(final boolean primaryMode) {
        final AllocationId allocationId = AllocationId.newInitializing();
        final AtomicLong currentTimeMillis = new AtomicLong(randomLongBetween(0, 1024));
        final long retentionLeaseMillis = randomLongBetween(1, TimeValue.timeValueHours(12).millis());
        final Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), TimeValue.timeValueMillis(retentionLeaseMillis))
            .build();
        final long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", settings),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            currentTimeMillis::get,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        if (primaryMode) {
            replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        }
        final long[] retainingSequenceNumbers = new long[1];
        retainingSequenceNumbers[0] = randomLongBetween(0, Long.MAX_VALUE);
        if (primaryMode) {
            replicationTracker.addRetentionLease("0", retainingSequenceNumbers[0], "test-0", ActionListener.wrap(() -> {}));
        } else {
            final RetentionLeases retentionLeases = new RetentionLeases(
                primaryTerm,
                1,
                Collections.singleton(new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0"))
            );
            replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
        }

        {
            final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
            final long expectedVersion = primaryMode ? 2L : 1L;
            assertThat(retentionLeases.version(), equalTo(expectedVersion));
            assertThat(retentionLeases.leases(), hasSize(primaryMode ? 2 : 1));
            final RetentionLease retentionLease = retentionLeases.get("0");
            assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, primaryTerm, expectedVersion, primaryMode, false);
        }

        // renew the lease
        currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(0, 1024));
        retainingSequenceNumbers[0] = randomLongBetween(retainingSequenceNumbers[0], Long.MAX_VALUE);
        if (primaryMode) {
            replicationTracker.renewRetentionLease("0", retainingSequenceNumbers[0], "test-0");
        } else {
            final RetentionLeases retentionLeases = new RetentionLeases(
                primaryTerm,
                2,
                Collections.singleton(new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0"))
            );
            replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
        }

        {
            final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
            final long expectedVersion = primaryMode ? 3L : 2L;
            assertThat(retentionLeases.version(), equalTo(expectedVersion));
            assertThat(retentionLeases.leases(), hasSize(primaryMode ? 2 : 1));
            final RetentionLease retentionLease = retentionLeases.get("0");
            assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, primaryTerm, expectedVersion, primaryMode, false);
        }

        // now force the lease to expire
        currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(retentionLeaseMillis, Long.MAX_VALUE - currentTimeMillis.get()));
        if (primaryMode) {
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, primaryTerm, 3, true, false);
            assertRetentionLeases(replicationTracker, 0, new long[0], primaryTerm, 4, true, true);
        } else {
            // leases do not expire on replicas until synced from the primary
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, primaryTerm, 2, false, false);
        }
    }

    public void testReplicaIgnoresOlderRetentionLeasesVersion() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        final int length = randomIntBetween(0, 8);
        final List<RetentionLeases> retentionLeasesCollection = new ArrayList<>(length);
        long primaryTerm = 1;
        long version = 0;
        for (int i = 0; i < length; i++) {
            final int innerLength = randomIntBetween(0, 8);
            final Collection<RetentionLease> leases = new ArrayList<>();
            for (int j = 0; j < innerLength; j++) {
                leases.add(new RetentionLease(i + "-" + j, randomNonNegativeLong(), randomNonNegativeLong(), randomAlphaOfLength(8)));
            }
            version++;
            if (rarely()) {
                primaryTerm++;
            }
            retentionLeasesCollection.add(new RetentionLeases(primaryTerm, version, leases));
        }
        final Collection<RetentionLease> expectedLeases;
        if (length == 0 || retentionLeasesCollection.get(length - 1).leases().isEmpty()) {
            expectedLeases = Collections.emptyList();
        } else {
            expectedLeases = retentionLeasesCollection.get(length - 1).leases();
        }
        Collections.shuffle(retentionLeasesCollection, random());
        for (final RetentionLeases retentionLeases : retentionLeasesCollection) {
            replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
        }
        assertThat(replicationTracker.getRetentionLeases().version(), equalTo(version));
        if (expectedLeases.isEmpty()) {
            assertThat(replicationTracker.getRetentionLeases().leases(), empty());
        } else {
            assertThat(replicationTracker.getRetentionLeases().leases(), contains(expectedLeases.toArray(new RetentionLease[0])));
        }
    }

    public void testLoadAndPersistRetentionLeases() throws IOException {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final int length = randomIntBetween(0, 8);
        for (int i = 0; i < length; i++) {
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            replicationTracker.addRetentionLease(Integer.toString(i), retainingSequenceNumber, "test-" + i, ActionListener.wrap(() -> {}));
        }

        final Path path = createTempDir();
        replicationTracker.persistRetentionLeases(path);
        assertThat(replicationTracker.loadRetentionLeases(path), equalTo(replicationTracker.getRetentionLeases()));
    }

    public void testUnnecessaryPersistenceOfRetentionLeases() throws IOException {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final int length = randomIntBetween(0, 8);
        for (int i = 0; i < length; i++) {
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            replicationTracker.addRetentionLease(Integer.toString(i), retainingSequenceNumber, "test-" + i, ActionListener.wrap(() -> {}));
        }

        final Path path = createTempDir();
        replicationTracker.persistRetentionLeases(path);

        final Tuple<RetentionLeases, Long> retentionLeasesWithGeneration = RetentionLeases.FORMAT.loadLatestStateWithGeneration(
            logger,
            NamedXContentRegistry.EMPTY,
            path
        );

        replicationTracker.persistRetentionLeases(path);
        final Tuple<RetentionLeases, Long> retentionLeasesWithGenerationAfterUnnecessaryPersistence = RetentionLeases.FORMAT
            .loadLatestStateWithGeneration(logger, NamedXContentRegistry.EMPTY, path);

        assertThat(retentionLeasesWithGenerationAfterUnnecessaryPersistence.v1(), equalTo(retentionLeasesWithGeneration.v1()));
        assertThat(retentionLeasesWithGenerationAfterUnnecessaryPersistence.v2(), equalTo(retentionLeasesWithGeneration.v2()));
    }

    /**
     * Test that we correctly synchronize writing the retention lease state file in {@link ReplicationTracker#persistRetentionLeases(Path)}.
     * This test can fail without the synchronization block in that method.
     *
     * @throws IOException if an I/O exception occurs loading the retention lease state file
     */
    public void testPersistRetentionLeasesUnderConcurrency() throws IOException {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final int length = randomIntBetween(0, 8);
        for (int i = 0; i < length; i++) {
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            replicationTracker.addRetentionLease(Integer.toString(i), retainingSequenceNumber, "test-" + i, ActionListener.wrap(() -> {}));
        }

        final Path path = createTempDir();
        final int numberOfThreads = randomIntBetween(1, 2 * Runtime.getRuntime().availableProcessors());
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);
        final Thread[] threads = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            final String id = Integer.toString(length + i);
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                    replicationTracker.addRetentionLease(id, retainingSequenceNumber, "test-" + id, ActionListener.wrap(() -> {}));
                    replicationTracker.persistRetentionLeases(path);
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException | WriteStateException e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].start();
        }

        try {
            // synchronize the threads invoking ReplicationTracker#persistRetentionLeases(Path path)
            barrier.await();
            // wait for all the threads to finish
            barrier.await();
            for (int i = 0; i < numberOfThreads; i++) {
                threads[i].join();
            }
        } catch (final BrokenBarrierException | InterruptedException e) {
            throw new AssertionError(e);
        }
        assertThat(replicationTracker.loadRetentionLeases(path), equalTo(replicationTracker.getRetentionLeases()));
    }

    public void testRenewLeaseWithLowerRetainingSequenceNumber() throws Exception {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE,
            NON_REMOTE_DISCOVERY_NODE
        );
        replicationTracker.updateFromClusterManager(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);
        replicationTracker.addRetentionLease(id, retainingSequenceNumber, source, ActionListener.wrap(() -> {}));
        final long lowerRetainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, retainingSequenceNumber - 1);
        final RetentionLeaseInvalidRetainingSeqNoException e = expectThrows(
            RetentionLeaseInvalidRetainingSeqNoException.class,
            () -> replicationTracker.renewRetentionLease(id, lowerRetainingSequenceNumber, source)
        );
        assertThat(
            e,
            hasToString(
                containsString(
                    "the current retention lease with ["
                        + id
                        + "]"
                        + " is retaining a higher sequence number ["
                        + retainingSequenceNumber
                        + "]"
                        + " than the new retaining sequence number ["
                        + lowerRetainingSequenceNumber
                        + "] from ["
                        + source
                        + "]"
                )
            )
        );
    }

    private void assertRetentionLeases(
        final ReplicationTracker replicationTracker,
        final int size,
        final long[] minimumRetainingSequenceNumbers,
        final long primaryTerm,
        final long version,
        final boolean primaryMode,
        final boolean expireLeases
    ) {
        assertTrue(expireLeases == false || primaryMode);
        final RetentionLeases retentionLeases;
        if (expireLeases == false) {
            if (randomBoolean()) {
                retentionLeases = replicationTracker.getRetentionLeases();
            } else {
                final Tuple<Boolean, RetentionLeases> tuple = replicationTracker.getRetentionLeases(false);
                assertFalse(tuple.v1());
                retentionLeases = tuple.v2();
            }
        } else {
            final Tuple<Boolean, RetentionLeases> tuple = replicationTracker.getRetentionLeases(true);
            assertTrue(tuple.v1());
            retentionLeases = tuple.v2();
        }
        assertThat(retentionLeases.primaryTerm(), equalTo(primaryTerm));
        assertThat(retentionLeases.version(), equalTo(version));
        final Map<String, RetentionLease> idToRetentionLease = retentionLeases.leases()
            .stream()
            .filter(retentionLease -> ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(retentionLease.source()) == false)
            .collect(Collectors.toMap(RetentionLease::id, Function.identity()));

        assertThat(idToRetentionLease.entrySet(), hasSize(size));
        for (int i = 0; i < size; i++) {
            assertThat(idToRetentionLease.keySet(), hasItem(Integer.toString(i)));
            final RetentionLease retentionLease = idToRetentionLease.get(Integer.toString(i));
            assertThat(retentionLease.retainingSequenceNumber(), equalTo(minimumRetainingSequenceNumbers[i]));
            assertThat(retentionLease.source(), equalTo("test-" + i));
        }
    }

}
