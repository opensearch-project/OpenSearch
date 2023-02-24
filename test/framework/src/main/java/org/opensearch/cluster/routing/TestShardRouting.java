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

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.index.shard.ShardId;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.OpenSearchTestCase;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;

/**
 * A helper that allows to create shard routing instances within tests, while not requiring to expose
 * different simplified constructors on the ShardRouting itself.
 */
public class TestShardRouting {

    public static ShardRouting newShardRouting(String index, int shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return newShardRouting(new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId), currentNodeId, primary, state);
    }

    public static ShardRouting newShardRouting(ShardId shardId, String currentNodeId, boolean primary, ShardRoutingState state) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            buildUnassignedInfo(state),
            buildAllocationId(state),
            -1
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        boolean primary,
        ShardRoutingState state,
        RecoverySource recoverySource
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            null,
            primary,
            state,
            recoverySource,
            buildUnassignedInfo(state),
            buildAllocationId(state),
            -1
        );
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state
    ) {
        return newShardRouting(
            new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId),
            currentNodeId,
            relocatingNodeId,
            primary,
            state
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            buildUnassignedInfo(state),
            buildAllocationId(state),
            -1
        );
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        AllocationId allocationId
    ) {
        return newShardRouting(
            new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId),
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            allocationId
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        AllocationId allocationId
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            buildUnassignedInfo(state),
            allocationId,
            -1
        );
    }

    public static ShardRouting newShardRouting(
        String index,
        int shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        UnassignedInfo unassignedInfo
    ) {
        return newShardRouting(
            new ShardId(index, IndexMetadata.INDEX_UUID_NA_VALUE, shardId),
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            unassignedInfo
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        UnassignedInfo unassignedInfo
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            buildRecoveryTarget(primary, state),
            unassignedInfo,
            buildAllocationId(state),
            -1
        );
    }

    public static ShardRouting newShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo
    ) {
        return new ShardRouting(
            shardId,
            currentNodeId,
            relocatingNodeId,
            primary,
            state,
            recoverySource,
            unassignedInfo,
            buildAllocationId(state),
            -1
        );
    }

    public static ShardRouting relocate(ShardRouting shardRouting, String relocatingNodeId, long expectedShardSize) {
        return shardRouting.relocate(relocatingNodeId, expectedShardSize);
    }

    private static RecoverySource buildRecoveryTarget(boolean primary, ShardRoutingState state) {
        switch (state) {
            case UNASSIGNED:
            case INITIALIZING:
                if (primary) {
                    return OpenSearchTestCase.randomFrom(
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        RecoverySource.ExistingStoreRecoverySource.INSTANCE
                    );
                } else {
                    return RecoverySource.PeerRecoverySource.INSTANCE;
                }
            case STARTED:
            case RELOCATING:
                return null;
            default:
                throw new IllegalStateException("illegal state");
        }
    }

    private static AllocationId buildAllocationId(ShardRoutingState state) {
        switch (state) {
            case UNASSIGNED:
                return null;
            case INITIALIZING:
            case STARTED:
                return AllocationId.newInitializing();
            case RELOCATING:
                AllocationId allocationId = AllocationId.newInitializing();
                return AllocationId.newRelocation(allocationId);
            default:
                throw new IllegalStateException("illegal state");
        }
    }

    private static UnassignedInfo buildUnassignedInfo(ShardRoutingState state) {
        switch (state) {
            case UNASSIGNED:
            case INITIALIZING:
                return new UnassignedInfo(OpenSearchTestCase.randomFrom(UnassignedInfo.Reason.values()), "auto generated for test");
            case STARTED:
            case RELOCATING:
                return null;
            default:
                throw new IllegalStateException("illegal state");
        }
    }

    public static RecoverySource randomRecoverySource() {
        return OpenSearchTestCase.randomFrom(
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            RecoverySource.PeerRecoverySource.INSTANCE,
            RecoverySource.LocalShardsRecoverySource.INSTANCE,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                new Snapshot("repo", new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())),
                Version.CURRENT,
                new IndexId("some_index", UUIDs.randomBase64UUID(random()))
            )
        );
    }
}
