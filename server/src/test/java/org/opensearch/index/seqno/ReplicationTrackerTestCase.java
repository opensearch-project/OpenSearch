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

import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.IndexSettingsModule;

import java.util.Collections;
import java.util.Set;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public abstract class ReplicationTrackerTestCase extends OpenSearchTestCase {

    ReplicationTracker newTracker(
        final AllocationId allocationId,
        final LongConsumer updatedGlobalCheckpoint,
        final LongSupplier currentTimeMillisSupplier,
        final Settings settings
    ) {
        return new ReplicationTracker(
            new ShardId("test", "_na_", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", settings),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            updatedGlobalCheckpoint,
            currentTimeMillisSupplier,
            (leases, listener) -> {},
            OPS_BASED_RECOVERY_ALWAYS_REASONABLE
        );
    }

    ReplicationTracker newTracker(
        final AllocationId allocationId,
        final LongConsumer updatedGlobalCheckpoint,
        final LongSupplier currentTimeMillisSupplier
    ) {
        return newTracker(allocationId, updatedGlobalCheckpoint, currentTimeMillisSupplier, Settings.EMPTY);
    }

    static final Supplier<SafeCommitInfo> OPS_BASED_RECOVERY_ALWAYS_REASONABLE = () -> SafeCommitInfo.EMPTY;

    static String nodeIdFromAllocationId(final AllocationId allocationId) {
        return "n-" + allocationId.getId().substring(0, 8);
    }

    static IndexShardRoutingTable routingTable(final Set<AllocationId> initializingIds, final AllocationId primaryId) {
        return routingTable(initializingIds, Collections.singleton(primaryId), primaryId);
    }

    static IndexShardRoutingTable routingTable(
        final Set<AllocationId> initializingIds,
        final Set<AllocationId> activeIds,
        final AllocationId primaryId
    ) {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ShardRouting primaryShard = TestShardRouting.newShardRouting(
            shardId,
            nodeIdFromAllocationId(primaryId),
            null,
            true,
            ShardRoutingState.STARTED,
            primaryId
        );
        return routingTable(initializingIds, activeIds, primaryShard);
    }

    static IndexShardRoutingTable routingTable(
        final Set<AllocationId> initializingIds,
        final Set<AllocationId> activeIds,
        final ShardRouting primaryShard
    ) {
        assert initializingIds != null && activeIds != null;
        assert !initializingIds.contains(primaryShard.allocationId());
        assert activeIds.contains(primaryShard.allocationId());
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(shardId);
        for (final AllocationId initializingId : initializingIds) {
            builder.addShard(
                TestShardRouting.newShardRouting(
                    shardId,
                    nodeIdFromAllocationId(initializingId),
                    null,
                    false,
                    ShardRoutingState.INITIALIZING,
                    initializingId
                )
            );
        }
        for (final AllocationId activeId : activeIds) {
            if (activeId.equals(primaryShard.allocationId())) {
                continue;
            }
            builder.addShard(
                TestShardRouting.newShardRouting(
                    shardId,
                    nodeIdFromAllocationId(activeId),
                    null,
                    false,
                    ShardRoutingState.STARTED,
                    activeId
                )
            );
        }

        builder.addShard(primaryShard);

        return builder.build();
    }

}
