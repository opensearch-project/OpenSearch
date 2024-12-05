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

package org.opensearch.indices.cluster;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStoreStatsTrackerFactory;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndex;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices;
import org.opensearch.indices.cluster.IndicesClusterStateService.Shard;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoveryListener;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.collect.MapBuilder.newMapBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Abstract base class for tests against {@link IndicesClusterStateService}
 */
public abstract class AbstractIndicesClusterStateServiceTestCase extends OpenSearchTestCase {

    private boolean enableRandomFailures;

    @Before
    public void injectRandomFailures() {
        enableRandomFailures = randomBoolean();
    }

    protected void disableRandomFailures() {
        enableRandomFailures = false;
    }

    protected void failRandomly() {
        if (enableRandomFailures && rarely()) {
            throw new RuntimeException("dummy test failure");
        }
    }

    /**
     * Checks if cluster state matches internal state of IndicesClusterStateService instance
     *
     * @param state cluster state used for matching
     */
    public void assertClusterStateMatchesNodeState(ClusterState state, IndicesClusterStateService indicesClusterStateService) {
        MockIndicesService indicesService = (MockIndicesService) indicesClusterStateService.indicesService;
        ConcurrentMap<ShardId, ShardRouting> failedShardsCache = indicesClusterStateService.failedShardsCache;
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.getNodes().getLocalNodeId());
        if (localRoutingNode != null) {
            if (enableRandomFailures == false) {
                // initializing a shard should succeed when enableRandomFailures is disabled
                // active shards can be failed if state persistence was disabled in an earlier CS update
                if (failedShardsCache.values().stream().anyMatch(ShardRouting::initializing)) {
                    fail("failed shard cache should not contain initializing shard routing: " + failedShardsCache.values());
                }
            }
            // check that all shards in local routing nodes have been allocated
            for (ShardRouting shardRouting : localRoutingNode) {
                Index index = shardRouting.index();
                IndexMetadata indexMetadata = state.metadata().getIndexSafe(index);

                MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
                ShardRouting failedShard = failedShardsCache.get(shardRouting.shardId());

                if (state.blocks().disableStatePersistence()) {
                    if (shard != null) {
                        fail("Shard with id " + shardRouting + " should be removed from indicesService due to disabled state persistence");
                    }
                } else {
                    if (failedShard != null && failedShard.isSameAllocation(shardRouting) == false) {
                        fail("Shard cache has not been properly cleaned for " + failedShard);
                    }
                    if (shard == null && failedShard == null) {
                        // shard must either be there or there must be a failure
                        fail("Shard with id " + shardRouting + " expected but missing in indicesService and failedShardsCache");
                    }
                    if (enableRandomFailures == false) {
                        if (shard == null && shardRouting.initializing() && failedShard == shardRouting) {
                            // initializing a shard should succeed when enableRandomFailures is disabled
                            fail("Shard with id " + shardRouting + " expected but missing in indicesService " + failedShard);
                        }
                    }

                    if (shard != null) {
                        AllocatedIndex<? extends Shard> indexService = indicesService.indexService(index);
                        assertTrue("Index " + index + " expected but missing in indicesService", indexService != null);

                        // index metadata has been updated
                        assertThat(indexService.getIndexSettings().getIndexMetadata(), equalTo(indexMetadata));
                        // shard has been created
                        if (enableRandomFailures == false || failedShard == null) {
                            assertTrue("Shard with id " + shardRouting + " expected but missing in indexService", shard != null);
                            // shard has latest shard routing
                            assertThat(shard.routingEntry(), equalTo(shardRouting));
                        }

                        if (shard.routingEntry().primary() && shard.routingEntry().active()) {
                            IndexShardRoutingTable shardRoutingTable = state.routingTable().shardRoutingTable(shard.shardId());
                            Set<String> inSyncIds = state.metadata()
                                .index(shard.shardId().getIndex())
                                .inSyncAllocationIds(shard.shardId().id());
                            assertThat(
                                shard.routingEntry() + " isn't updated with in-sync aIDs",
                                shard.inSyncAllocationIds,
                                equalTo(inSyncIds)
                            );
                            assertThat(
                                shard.routingEntry() + " isn't updated with routing table",
                                shard.routingTable,
                                equalTo(shardRoutingTable)
                            );
                        }
                    }
                }
            }
        }

        // all other shards / indices have been cleaned up
        for (AllocatedIndex<? extends Shard> indexService : indicesService) {
            if (state.blocks().disableStatePersistence()) {
                fail("Index service " + indexService.index() + " should be removed from indicesService due to disabled state persistence");
            }

            assertTrue(state.metadata().getIndexSafe(indexService.index()) != null);

            boolean shardsFound = false;
            for (Shard shard : indexService) {
                shardsFound = true;
                ShardRouting persistedShardRouting = shard.routingEntry();
                ShardRouting shardRouting = localRoutingNode.getByShardId(persistedShardRouting.shardId());
                if (shardRouting == null) {
                    fail("Shard with id " + persistedShardRouting + " locally exists but missing in routing table");
                }
                if (shardRouting.equals(persistedShardRouting) == false) {
                    fail("Local shard " + persistedShardRouting + " has stale routing" + shardRouting);
                }
            }

            if (shardsFound == false) {
                // check if we have shards of that index in failedShardsCache
                // if yes, we might not have cleaned the index as failedShardsCache can be populated by another thread
                assertFalse(failedShardsCache.keySet().stream().noneMatch(shardId -> shardId.getIndex().equals(indexService.index())));
            }

        }
    }

    /**
     * Mock for {@link IndicesService}
     */
    protected class MockIndicesService implements AllocatedIndices<MockIndexShard, MockIndexService> {
        private volatile Map<String, MockIndexService> indices = emptyMap();

        @Override
        public synchronized MockIndexService createIndex(
            IndexMetadata indexMetadata,
            List<IndexEventListener> buildInIndexListener,
            boolean writeDanglingIndices
        ) throws IOException {
            MockIndexService indexService = new MockIndexService(new IndexSettings(indexMetadata, Settings.EMPTY));
            indices = newMapBuilder(indices).put(indexMetadata.getIndexUUID(), indexService).immutableMap();
            return indexService;
        }

        @Override
        public IndexMetadata verifyIndexIsDeleted(Index index, ClusterState state) {
            return null;
        }

        @Override
        public void deleteUnassignedIndex(String reason, IndexMetadata metadata, ClusterState clusterState) {

        }

        @Override
        public synchronized void removeIndex(Index index, IndexRemovalReason reason, String extraInfo) {
            if (hasIndex(index)) {
                Map<String, MockIndexService> newIndices = new HashMap<>(indices);
                newIndices.remove(index.getUUID());
                indices = unmodifiableMap(newIndices);
            }
        }

        @Override
        @Nullable
        public MockIndexService indexService(Index index) {
            return indices.get(index.getUUID());
        }

        @Override
        public MockIndexShard createShard(
            final ShardRouting shardRouting,
            final SegmentReplicationCheckpointPublisher checkpointPublisher,
            final PeerRecoveryTargetService recoveryTargetService,
            final RecoveryListener recoveryListener,
            final RepositoriesService repositoriesService,
            final Consumer<IndexShard.ShardFailure> onShardFailure,
            final Consumer<ShardId> globalCheckpointSyncer,
            final RetentionLeaseSyncer retentionLeaseSyncer,
            final DiscoveryNode targetNode,
            final DiscoveryNode sourceNode,
            final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory,
            final DiscoveryNodes discoveryNodes
        ) throws IOException {
            failRandomly();
            RecoveryState recoveryState = new RecoveryState(shardRouting, targetNode, sourceNode);
            MockIndexService indexService = indexService(recoveryState.getShardId().getIndex());
            MockIndexShard indexShard = indexService.createShard(shardRouting);
            indexShard.recoveryState = recoveryState;
            return indexShard;
        }

        @Override
        public void processPendingDeletes(Index index, IndexSettings indexSettings, TimeValue timeValue) throws IOException,
            InterruptedException {

        }

        private boolean hasIndex(Index index) {
            return indices.containsKey(index.getUUID());
        }

        @Override
        public Iterator<MockIndexService> iterator() {
            return indices.values().iterator();
        }
    }

    /**
     * Mock for {@link IndexService}
     */
    protected class MockIndexService implements AllocatedIndex<MockIndexShard> {
        private volatile Map<Integer, MockIndexShard> shards = emptyMap();

        private final IndexSettings indexSettings;

        public MockIndexService(IndexSettings indexSettings) {
            this.indexSettings = indexSettings;
        }

        @Override
        public IndexSettings getIndexSettings() {
            return indexSettings;
        }

        @Override
        public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
            failRandomly();
            return false;
        }

        @Override
        public void updateMetadata(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) {
            indexSettings.updateIndexMetadata(newIndexMetadata);
            for (MockIndexShard shard : shards.values()) {
                shard.updateTerm(newIndexMetadata.primaryTerm(shard.shardId().id()));
            }
        }

        @Override
        public MockIndexShard getShardOrNull(int shardId) {
            return shards.get(shardId);
        }

        public synchronized MockIndexShard createShard(ShardRouting routing) throws IOException {
            failRandomly();
            MockIndexShard shard = new MockIndexShard(routing, indexSettings.getIndexMetadata().primaryTerm(routing.shardId().id()));
            shards = newMapBuilder(shards).put(routing.id(), shard).immutableMap();
            return shard;
        }

        @Override
        public synchronized void removeShard(int shardId, String reason) {
            if (shards.containsKey(shardId) == false) {
                return;
            }
            HashMap<Integer, MockIndexShard> newShards = new HashMap<>(shards);
            MockIndexShard indexShard = newShards.remove(shardId);
            assert indexShard != null;
            shards = unmodifiableMap(newShards);
        }

        @Override
        public Iterator<MockIndexShard> iterator() {
            return shards.values().iterator();
        }

        @Override
        public Index index() {
            return indexSettings.getIndex();
        }
    }

    /**
     * Mock for {@link IndexShard}
     */
    protected class MockIndexShard implements IndicesClusterStateService.Shard {
        private volatile ShardRouting shardRouting;
        private volatile RecoveryState recoveryState;
        private volatile Set<String> inSyncAllocationIds;
        private volatile IndexShardRoutingTable routingTable;
        private volatile long term;

        public MockIndexShard(ShardRouting shardRouting, long term) {
            this.shardRouting = shardRouting;
            this.term = term;
        }

        @Override
        public ShardId shardId() {
            return shardRouting.shardId();
        }

        @Override
        public RecoveryState recoveryState() {
            return recoveryState;
        }

        @Override
        public void updateShardState(
            ShardRouting shardRouting,
            long newPrimaryTerm,
            BiConsumer<IndexShard, ActionListener<ResyncTask>> primaryReplicaSyncer,
            long applyingClusterStateVersion,
            Set<String> inSyncAllocationIds,
            IndexShardRoutingTable routingTable,
            DiscoveryNodes discoveryNodes
        ) throws IOException {
            failRandomly();
            assertThat(this.shardId(), equalTo(shardRouting.shardId()));
            assertTrue("current: " + this.shardRouting + ", got: " + shardRouting, this.shardRouting.isSameAllocation(shardRouting));
            if (this.shardRouting.active()) {
                assertTrue(
                    "an active shard must stay active, current: " + this.shardRouting + ", got: " + shardRouting,
                    shardRouting.active()
                );
            }
            if (this.shardRouting.primary()) {
                assertTrue("a primary shard can't be demoted", shardRouting.primary());
                if (this.shardRouting.initializing()) {
                    assertEquals("primary term can not be updated on an initializing primary shard: " + shardRouting, term, newPrimaryTerm);
                }
            } else if (shardRouting.primary()) {
                // note: it's ok for a replica in post recovery to be started and promoted at once
                // this can happen when the primary failed after we sent the start shard message
                assertTrue(
                    "a replica can only be promoted when active. current: " + this.shardRouting + " new: " + shardRouting,
                    shardRouting.active()
                );
            }
            this.shardRouting = shardRouting;
            if (shardRouting.primary()) {
                term = newPrimaryTerm;
                this.inSyncAllocationIds = inSyncAllocationIds;
                this.routingTable = routingTable;
            }
        }

        @Override
        public ShardRouting routingEntry() {
            return shardRouting;
        }

        @Override
        public IndexShardState state() {
            return null;
        }

        public long term() {
            return term;
        }

        public void updateTerm(long newTerm) {
            assertThat("term can only be incremented: " + shardRouting, newTerm, greaterThanOrEqualTo(term));
            if (shardRouting.primary() && shardRouting.active()) {
                assertThat("term can not be changed on an active primary shard: " + shardRouting, newTerm, equalTo(term));
            }
            this.term = newTerm;
        }
    }
}
