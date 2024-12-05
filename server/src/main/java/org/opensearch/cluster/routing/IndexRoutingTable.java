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

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RecoverySource.EmptyStoreRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.LocalShardsRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.RemoteStoreRecoverySource;
import org.opensearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.opensearch.common.Randomness;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.VerifiableWriteable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The {@link IndexRoutingTable} represents routing information for a single
 * index. The routing table maintains a list of all shards in the index. A
 * single shard in this context has one more instances namely exactly one
 * {@link ShardRouting#primary() primary} and 1 or more replicas. In other
 * words, each instance of a shard is considered a replica while only one
 * replica per shard is a {@code primary} replica. The {@code primary} replica
 * can be seen as the "leader" of the shard acting as the primary entry point
 * for operations on a specific shard.
 * <p>
 * Note: The term replica is not directly
 * reflected in the routing table or in related classes, replicas are
 * represented as {@link ShardRouting}.
 * </p>
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexRoutingTable extends AbstractDiffable<IndexRoutingTable>
    implements
        Iterable<IndexShardRoutingTable>,
        VerifiableWriteable {

    private final Index index;
    private final ShardShuffler shuffler;

    // note, we assume that when the index routing is created, ShardRoutings are created for all possible number of
    // shards with state set to UNASSIGNED
    private final Map<Integer, IndexShardRoutingTable> shards;

    private final List<ShardRouting> allActiveShards;

    IndexRoutingTable(Index index, final Map<Integer, IndexShardRoutingTable> shards) {
        this.index = index;
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = Collections.unmodifiableMap(shards);
        List<ShardRouting> allActiveShards = new ArrayList<>();
        for (IndexShardRoutingTable cursor : shards.values()) {
            for (ShardRouting shardRouting : cursor) {
                if (shardRouting.active()) {
                    allActiveShards.add(shardRouting);
                }
            }
        }
        this.allActiveShards = Collections.unmodifiableList(allActiveShards);
    }

    /**
     * Return the index id
     *
     * @return id of the index
     */
    public Index getIndex() {
        return index;
    }

    boolean validate(Metadata metadata) {
        // check index exists
        if (!metadata.hasIndex(index.getName())) {
            throw new IllegalStateException(index + " exists in routing does not exists in metadata");
        }
        IndexMetadata indexMetadata = metadata.index(index.getName());
        if (indexMetadata.getIndexUUID().equals(index.getUUID()) == false) {
            throw new IllegalStateException(index.getName() + " exists in routing does not exists in metadata with the same uuid");
        }

        // check the number of shards
        if (indexMetadata.getNumberOfShards() != shards().size()) {
            Set<Integer> expected = new HashSet<>();
            for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                expected.add(i);
            }
            for (IndexShardRoutingTable indexShardRoutingTable : this) {
                expected.remove(indexShardRoutingTable.shardId().id());
            }
            throw new IllegalStateException("Wrong number of shards in routing table, missing: " + expected);
        }

        // check the replicas
        for (IndexShardRoutingTable indexShardRoutingTable : this) {
            int routingNumberOfReplicas = indexShardRoutingTable.size() - 1;
            if (routingNumberOfReplicas != indexMetadata.getNumberOfReplicas() + indexMetadata.getNumberOfSearchOnlyReplicas()) {
                throw new IllegalStateException(
                    "Shard ["
                        + indexShardRoutingTable.shardId().id()
                        + "] routing table has wrong number of replicas, expected ["
                        + indexMetadata.getNumberOfReplicas()
                        + "], got ["
                        + routingNumberOfReplicas
                        + "]"
                );
            }
            for (ShardRouting shardRouting : indexShardRoutingTable) {
                if (!shardRouting.index().equals(index)) {
                    throw new IllegalStateException(
                        "shard routing has an index [" + shardRouting.index() + "] that is different " + "from the routing table"
                    );
                }
                final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(shardRouting.id());
                if (shardRouting.active()
                    && inSyncAllocationIds.contains(shardRouting.allocationId().getId()) == false
                    && shardRouting.isSearchOnly() == false) {
                    throw new IllegalStateException(
                        "active shard routing "
                            + shardRouting
                            + " has no corresponding entry in the in-sync "
                            + "allocation set "
                            + inSyncAllocationIds
                    );
                }

                if (shardRouting.primary()
                    && shardRouting.initializing()
                    && shardRouting.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    if (inSyncAllocationIds.contains(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)) {
                        if (inSyncAllocationIds.size() != 1) {
                            throw new IllegalStateException(
                                "a primary shard routing "
                                    + shardRouting
                                    + " is a primary that is recovering from a stale primary has unexpected allocation ids in in-sync "
                                    + "allocation set "
                                    + inSyncAllocationIds
                            );
                        }
                    } else if (inSyncAllocationIds.contains(shardRouting.allocationId().getId()) == false) {
                        throw new IllegalStateException(
                            "a primary shard routing "
                                + shardRouting
                                + " is a primary that is recovering from a known allocation id but has no corresponding entry in the in-sync "
                                + "allocation set "
                                + inSyncAllocationIds
                        );
                    }
                }
            }
        }
        return true;
    }

    @Override
    public Iterator<IndexShardRoutingTable> iterator() {
        return shards.values().iterator();
    }

    /**
     * Calculates the number of nodes that hold one or more shards of this index
     * {@link IndexRoutingTable} excluding the nodes with the node ids give as
     * the <code>excludedNodes</code> parameter.
     *
     * @param excludedNodes id of nodes that will be excluded
     * @return number of distinct nodes this index has at least one shard allocated on
     */
    public int numberOfNodesShardsAreAllocatedOn(String... excludedNodes) {
        Set<String> nodes = new HashSet<>();
        for (IndexShardRoutingTable shardRoutingTable : this) {
            for (ShardRouting shardRouting : shardRoutingTable) {
                if (shardRouting.assignedToNode()) {
                    String currentNodeId = shardRouting.currentNodeId();
                    boolean excluded = false;
                    if (excludedNodes != null) {
                        for (String excludedNode : excludedNodes) {
                            if (currentNodeId.equals(excludedNode)) {
                                excluded = true;
                                break;
                            }
                        }
                    }
                    if (!excluded) {
                        nodes.add(currentNodeId);
                    }
                }
            }
        }
        return nodes.size();
    }

    public Map<Integer, IndexShardRoutingTable> shards() {
        return shards;
    }

    public Map<Integer, IndexShardRoutingTable> getShards() {
        return shards();
    }

    public IndexShardRoutingTable shard(int shardId) {
        return shards.get(shardId);
    }

    /**
     * Returns <code>true</code> if all shards are primary and active. Otherwise <code>false</code>.
     */
    public boolean allPrimaryShardsActive() {
        return primaryShardsActive() == shards().size();
    }

    /**
     * Calculates the number of primary shards in active state in routing table
     *
     * @return number of active primary shards
     */
    public int primaryShardsActive() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this) {
            if (shardRoutingTable.primaryShard().active()) {
                counter++;
            }
        }
        return counter;
    }

    /**
     * Returns <code>true</code> if all primary shards are in
     * {@link ShardRoutingState#UNASSIGNED} state. Otherwise <code>false</code>.
     */
    public boolean allPrimaryShardsUnassigned() {
        return primaryShardsUnassigned() == shards.size();
    }

    /**
     * Calculates the number of primary shards in the routing table the are in
     * {@link ShardRoutingState#UNASSIGNED} state.
     */
    public int primaryShardsUnassigned() {
        int counter = 0;
        for (IndexShardRoutingTable shardRoutingTable : this) {
            if (shardRoutingTable.primaryShard().unassigned()) {
                counter++;
            }
        }
        return counter;
    }

    /**
     * Returns a {@link List} of shards that match one of the states listed in {@link ShardRoutingState states}
     *
     * @param state {@link ShardRoutingState} to retrieve
     * @return a {@link List} of shards that match one of the given {@link ShardRoutingState states}
     */
    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        List<ShardRouting> shards = new ArrayList<>();
        for (IndexShardRoutingTable shardRoutingTable : this) {
            shards.addAll(shardRoutingTable.shardsWithState(state));
        }
        return shards;
    }

    /**
     * Returns a {@link List} of shards that match the provided {@link Predicate}
     *
     * @param predicate {@link Predicate} to apply
     * @return a {@link List} of shards that match one of the given {@link Predicate}
     */
    public List<ShardRouting> shardsMatchingPredicate(Predicate<ShardRouting> predicate) {
        List<ShardRouting> shards = new ArrayList<>();
        for (IndexShardRoutingTable shardRoutingTable : this) {
            shards.addAll(shardRoutingTable.shardsMatchingPredicate(predicate));
        }
        return shards;
    }

    public int shardsMatchingPredicateCount(Predicate<ShardRouting> predicate) {
        int count = 0;
        for (IndexShardRoutingTable shardRoutingTable : this) {
            count += shardRoutingTable.shardsMatchingPredicateCount(predicate);
        }
        return count;
    }

    /**
     * Returns an unordered iterator over all active shards (including replicas).
     */
    public ShardsIterator randomAllActiveShardsIt() {
        return new PlainShardsIterator(shuffler.shuffle(allActiveShards));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexRoutingTable that = (IndexRoutingTable) o;

        if (!index.equals(that.index)) return false;
        if (!shards.equals(that.shards)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "IndexRoutingTable{" + "shards=" + shards + ", index=" + index + '}';
    }

    public static IndexRoutingTable readFrom(StreamInput in) throws IOException {
        Index index = new Index(in);
        Builder builder = new Builder(index);

        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.addIndexShard(IndexShardRoutingTable.Builder.readFromThin(in, index));
        }

        return builder.build();
    }

    public static Diff<IndexRoutingTable> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(IndexRoutingTable::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shards.size());
        for (IndexShardRoutingTable indexShard : this) {
            IndexShardRoutingTable.Builder.writeToThin(indexShard, out);
        }
    }

    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeMapValues(shards, (stream, value) -> IndexShardRoutingTable.Builder.writeVerifiableTo(value, stream));
    }

    public static Builder builder(Index index) {
        return new Builder(index);
    }

    /**
     * Builder of a routing table.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private final Index index;
        private final Map<Integer, IndexShardRoutingTable> shards = new HashMap<>();

        public Builder(Index index) {
            this.index = index;
        }

        /**
         * Initializes a new empty index, as if it was created from an API.
         */
        public Builder initializeAsNew(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null));
        }

        /**
         * Initializes an existing index.
         */
        public Builder initializeAsRecovery(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, null));
        }

        /**
         * Initializes a new index caused by dangling index imported.
         */
        public Builder initializeAsFromDangling(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.DANGLING_INDEX_IMPORTED, null));
        }

        /**
         * Initializes a new empty index, as as a result of opening a closed index.
         */
        public Builder initializeAsFromCloseToOpen(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, null));
        }

        /**
         * Initializes a new empty index, as as a result of closing an opened index.
         */
        public Builder initializeAsFromOpenToClose(IndexMetadata indexMetadata) {
            return initializeEmpty(indexMetadata, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CLOSED, null));
        }

        /**
         * Initializes a new empty index, to be restored from a snapshot
         */
        public Builder initializeAsNewRestore(
            IndexMetadata indexMetadata,
            SnapshotRecoverySource recoverySource,
            final Set<Integer> ignoreShards
        ) {
            final UnassignedInfo unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.NEW_INDEX_RESTORED,
                "restore_source["
                    + recoverySource.snapshot().getRepository()
                    + "/"
                    + recoverySource.snapshot().getSnapshotId().getName()
                    + "]"
            );
            return initializeAsRestore(indexMetadata, recoverySource, ignoreShards, true, unassignedInfo);
        }

        /**
         * Initializes an existing index, to be restored from a snapshot
         */
        public Builder initializeAsRestore(IndexMetadata indexMetadata, SnapshotRecoverySource recoverySource) {
            final UnassignedInfo unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
                "restore_source["
                    + recoverySource.snapshot().getRepository()
                    + "/"
                    + recoverySource.snapshot().getSnapshotId().getName()
                    + "]"
            );
            return initializeAsRestore(indexMetadata, recoverySource, null, false, unassignedInfo);
        }

        /**
         * Initializes an existing index, to be restored from remote store
         */
        public Builder initializeAsRemoteStoreRestore(
            IndexMetadata indexMetadata,
            RemoteStoreRecoverySource recoverySource,
            Map<ShardId, IndexShardRoutingTable> indexShardRoutingTableMap,
            boolean forceRecoverAllPrimaries
        ) {
            final UnassignedInfo unassignedInfo = new UnassignedInfo(
                UnassignedInfo.Reason.EXISTING_INDEX_RESTORED,
                "restore_source[remote_store]"
            );
            assert indexMetadata.getIndex().equals(index);
            if (shards.isEmpty() == false) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
                ShardId shardId = new ShardId(index, shardNumber);
                if (indexShardRoutingTableMap.containsKey(shardId) == false) {
                    throw new IllegalStateException("IndexShardRoutingTable is not present for shardId: " + shardId);
                }
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                IndexShardRoutingTable indexShardRoutingTable = indexShardRoutingTableMap.get(shardId);
                if (forceRecoverAllPrimaries || indexShardRoutingTable.primaryShard().unassigned()) {
                    // Primary shard to be recovered from remote store.
                    indexShardRoutingBuilder.addShard(ShardRouting.newUnassigned(shardId, true, recoverySource, unassignedInfo));
                    // All the replica shards to be recovered from peer recovery.
                    for (int replicaNumber = 0; replicaNumber < indexMetadata.getNumberOfReplicas(); replicaNumber++) {
                        indexShardRoutingBuilder.addShard(
                            ShardRouting.newUnassigned(shardId, false, PeerRecoverySource.INSTANCE, unassignedInfo)
                        );
                    }
                } else {
                    // Primary is either active or initializing. Do not trigger restore.
                    indexShardRoutingBuilder.addShard(indexShardRoutingTable.primaryShard());
                    // Replica, if unassigned, trigger peer recovery else no action.
                    for (ShardRouting shardRouting : indexShardRoutingTable.replicaShards()) {
                        if (shardRouting.unassigned()) {
                            indexShardRoutingBuilder.addShard(
                                ShardRouting.newUnassigned(shardId, false, PeerRecoverySource.INSTANCE, unassignedInfo)
                            );
                        } else {
                            indexShardRoutingBuilder.addShard(shardRouting);
                        }
                    }
                }
                shards.put(shardNumber, indexShardRoutingBuilder.build());
            }
            return this;
        }

        /**
         * Initializes an index, to be restored from snapshot
         */
        private Builder initializeAsRestore(
            IndexMetadata indexMetadata,
            RecoverySource recoverySource,
            final Set<Integer> ignoreShards,
            boolean asNew,
            UnassignedInfo unassignedInfo
        ) {
            assert indexMetadata.getIndex().equals(index);
            if (!shards.isEmpty()) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
                ShardId shardId = new ShardId(index, shardNumber);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                for (int i = 0; i <= indexMetadata.getNumberOfReplicas(); i++) {
                    boolean primary = i == 0;
                    if (asNew && ignoreShards.contains(shardNumber)) {
                        // This shards wasn't completely snapshotted - restore it as new shard
                        indexShardRoutingBuilder.addShard(
                            ShardRouting.newUnassigned(
                                shardId,
                                primary,
                                primary ? EmptyStoreRecoverySource.INSTANCE : PeerRecoverySource.INSTANCE,
                                unassignedInfo
                            )
                        );
                    } else {
                        indexShardRoutingBuilder.addShard(
                            ShardRouting.newUnassigned(
                                shardId,
                                primary,
                                primary ? recoverySource : PeerRecoverySource.INSTANCE,
                                unassignedInfo
                            )
                        );
                    }
                }
                for (int i = 0; i < indexMetadata.getNumberOfSearchOnlyReplicas(); i++) {
                    indexShardRoutingBuilder.addShard(
                        ShardRouting.newUnassigned(
                            shardId,
                            false,
                            true,
                            PeerRecoverySource.INSTANCE, // TODO: Update to remote store if enabled
                            unassignedInfo
                        )
                    );
                }
                shards.put(shardNumber, indexShardRoutingBuilder.build());
            }
            return this;
        }

        /**
         * Initializes a new empty index, with an option to control if its from an API or not.
         */
        private Builder initializeEmpty(IndexMetadata indexMetadata, UnassignedInfo unassignedInfo) {
            assert indexMetadata.getIndex().equals(index);
            if (!shards.isEmpty()) {
                throw new IllegalStateException("trying to initialize an index with fresh shards, but already has shards created");
            }
            for (int shardNumber = 0; shardNumber < indexMetadata.getNumberOfShards(); shardNumber++) {
                ShardId shardId = new ShardId(index, shardNumber);
                final RecoverySource primaryRecoverySource;
                if (indexMetadata.inSyncAllocationIds(shardNumber).isEmpty() == false) {
                    // we have previous valid copies for this shard. use them for recovery
                    primaryRecoverySource = ExistingStoreRecoverySource.INSTANCE;
                } else if (indexMetadata.getResizeSourceIndex() != null) {
                    // this is a new index but the initial shards should merged from another index
                    primaryRecoverySource = LocalShardsRecoverySource.INSTANCE;
                } else {
                    // a freshly created index with no restriction
                    primaryRecoverySource = EmptyStoreRecoverySource.INSTANCE;
                }
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                for (int i = 0; i <= indexMetadata.getNumberOfReplicas(); i++) {
                    boolean primary = i == 0;
                    indexShardRoutingBuilder.addShard(
                        ShardRouting.newUnassigned(
                            shardId,
                            primary,
                            primary ? primaryRecoverySource : PeerRecoverySource.INSTANCE,
                            unassignedInfo
                        )
                    );
                }
                for (int i = 0; i < indexMetadata.getNumberOfSearchOnlyReplicas(); i++) {
                    indexShardRoutingBuilder.addShard(
                        ShardRouting.newUnassigned(
                            shardId,
                            false,
                            true,
                            PeerRecoverySource.INSTANCE, // TODO: Update to remote store if enabled
                            unassignedInfo
                        )
                    );
                }
                shards.put(shardNumber, indexShardRoutingBuilder.build());
            }
            return this;
        }

        public Builder addReplica() {
            for (final int shardNumber : shards.keySet()) {
                ShardId shardId = new ShardId(index, shardNumber);
                // version 0, will get updated when reroute will happen
                ShardRouting shard = ShardRouting.newUnassigned(
                    shardId,
                    false,
                    PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, null)
                );
                shards.put(shardNumber, new IndexShardRoutingTable.Builder(shards.get(shard.id())).addShard(shard).build());
            }
            return this;
        }

        /**
         * Add a Search only replica to the IndexShardRoutingTable
         * @return The Builder
         */
        public Builder addSearchReplica() {
            for (final int shardNumber : shards.keySet()) {
                ShardId shardId = new ShardId(index, shardNumber);
                // version 0, will get updated when reroute will happen
                ShardRouting shard = ShardRouting.newUnassigned(
                    shardId,
                    false,
                    true,
                    PeerRecoverySource.INSTANCE, // TODO: Change to remote store if enabled
                    new UnassignedInfo(UnassignedInfo.Reason.REPLICA_ADDED, null)
                );
                shards.put(shardNumber, new IndexShardRoutingTable.Builder(shards.get(shard.id())).addShard(shard).build());
            }
            return this;
        }

        public Builder removeReplica() {
            for (final int shardId : shards.keySet()) {
                IndexShardRoutingTable indexShard = shards.get(shardId);
                if (indexShard.replicaShards().isEmpty()) {
                    // nothing to do here!
                    return this;
                }
                // re-add all the current ones
                IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(indexShard.shardId());
                for (ShardRouting shardRouting : indexShard) {
                    builder.addShard(shardRouting);
                }
                // first check if there is one that is not assigned to a node, and remove it
                boolean removed = false;
                for (ShardRouting shardRouting : indexShard) {
                    if (!shardRouting.primary() && !shardRouting.assignedToNode() && !shardRouting.isSearchOnly()) {
                        builder.removeShard(shardRouting);
                        removed = true;
                        break;
                    }
                }
                if (!removed) {
                    for (ShardRouting shardRouting : indexShard) {
                        if (!shardRouting.primary() && !shardRouting.isSearchOnly()) {
                            builder.removeShard(shardRouting);
                            break;
                        }
                    }
                }
                shards.put(shardId, builder.build());
            }
            return this;
        }

        /**
         * Remove a Search only replica from the IndexShardRoutingTable
         * @return The Builder
         */
        public Builder removeSearchReplica() {
            for (final int shardId : shards.keySet()) {
                IndexShardRoutingTable indexShardRoutingTable = shards.get(shardId);
                if (indexShardRoutingTable.searchOnlyReplicas().isEmpty()) {
                    // nothing to do here!
                    return this;
                }
                // re-add all the current ones
                IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(indexShardRoutingTable.shardId());
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    builder.addShard(shardRouting);
                }
                // first check if there is one that is not assigned to a node, and remove it
                boolean removed = false;
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.isSearchOnly() && !shardRouting.assignedToNode()) {
                        builder.removeShard(shardRouting);
                        removed = true;
                        break;
                    }
                }
                if (!removed) {
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        if (shardRouting.isSearchOnly()) {
                            builder.removeShard(shardRouting);
                            break;
                        }
                    }
                }
                shards.put(shardId, builder.build());
            }
            return this;
        }

        public Builder addIndexShard(IndexShardRoutingTable indexShard) {
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        /**
         * Adds a new shard routing (makes a copy of it), with reference data used from the index shard routing table
         * if it needs to be created.
         */
        public Builder addShard(ShardRouting shard) {
            IndexShardRoutingTable indexShard = shards.get(shard.id());
            if (indexShard == null) {
                indexShard = new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard).build();
            } else {
                indexShard = new IndexShardRoutingTable.Builder(indexShard).addShard(shard).build();
            }
            shards.put(indexShard.shardId().id(), indexShard);
            return this;
        }

        public IndexRoutingTable build() {
            return new IndexRoutingTable(index, shards);
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder("-- index [" + index + "]\n");

        List<IndexShardRoutingTable> ordered = new ArrayList<>();
        for (IndexShardRoutingTable indexShard : this) {
            ordered.add(indexShard);
        }

        CollectionUtil.timSort(ordered, (o1, o2) -> {
            int v = o1.shardId().getIndex().getName().compareTo(o2.shardId().getIndex().getName());
            if (v == 0) {
                v = Integer.compare(o1.shardId().id(), o2.shardId().id());
            }
            return v;
        });

        for (IndexShardRoutingTable indexShard : ordered) {
            sb.append("----shard_id [")
                .append(indexShard.shardId().getIndex().getName())
                .append("][")
                .append(indexShard.shardId().id())
                .append("]\n");
            for (ShardRouting shard : indexShard) {
                sb.append("--------").append(shard.shortSummary()).append("\n");
            }
        }
        return sb.toString();
    }

}
