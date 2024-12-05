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

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A {@link RoutingNode} represents a cluster node associated with a single {@link DiscoveryNode} including all shards
 * that are hosted on that nodes. Each {@link RoutingNode} has a unique node id that can be used to identify the node.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RoutingNode implements Iterable<ShardRouting> {

    static class BucketedShards implements Iterable<ShardRouting> {
        private final Tuple<LinkedHashMap<ShardId, ShardRouting>, LinkedHashMap<ShardId, ShardRouting>> shardTuple; // LinkedHashMap to
                                                                                                                    // preserve order

        BucketedShards(LinkedHashMap<ShardId, ShardRouting> primaryShards, LinkedHashMap<ShardId, ShardRouting> replicaShards) {
            this.shardTuple = new Tuple(primaryShards, replicaShards);
        }

        public boolean isEmpty() {
            return this.shardTuple.v1().isEmpty() && this.shardTuple.v2().isEmpty();
        }

        public int size() {
            return this.shardTuple.v1().size() + this.shardTuple.v2().size();
        }

        public boolean containsKey(ShardId shardId) {
            return this.shardTuple.v1().containsKey(shardId) || this.shardTuple.v2().containsKey(shardId);
        }

        public ShardRouting get(ShardId shardId) {
            if (this.shardTuple.v1().containsKey(shardId)) {
                return this.shardTuple.v1().get(shardId);
            }
            return this.shardTuple.v2().get(shardId);
        }

        public ShardRouting put(ShardRouting shardRouting) {
            return put(shardRouting.shardId(), shardRouting);
        }

        public ShardRouting put(ShardId shardId, ShardRouting shardRouting) {
            ShardRouting ret;
            if (shardRouting.primary()) {
                ret = this.shardTuple.v1().put(shardId, shardRouting);
                if (this.shardTuple.v2().containsKey(shardId)) {
                    ret = this.shardTuple.v2().remove(shardId);
                }
            } else {
                ret = this.shardTuple.v2().put(shardId, shardRouting);
                if (this.shardTuple.v1().containsKey(shardId)) {
                    ret = this.shardTuple.v1().remove(shardId);
                }
            }

            return ret;
        }

        public ShardRouting remove(ShardId shardId) {
            if (this.shardTuple.v1().containsKey(shardId)) {
                return this.shardTuple.v1().remove(shardId);
            }
            return this.shardTuple.v2().remove(shardId);
        }

        @Override
        public Iterator<ShardRouting> iterator() {
            return Stream.concat(
                Collections.unmodifiableCollection(this.shardTuple.v1().values()).stream(),
                Collections.unmodifiableCollection(this.shardTuple.v2().values()).stream()
            ).iterator();
        }
    }

    private final String nodeId;

    private final DiscoveryNode node;

    private final BucketedShards shards;

    private final LinkedHashSet<ShardRouting> initializingShards;

    private final LinkedHashSet<ShardRouting> relocatingShards;

    private final HashMap<Index, LinkedHashSet<ShardRouting>> shardsByIndex;

    public RoutingNode(String nodeId, DiscoveryNode node, ShardRouting... shardRoutings) {
        this.nodeId = nodeId;
        this.node = node;
        final LinkedHashMap<ShardId, ShardRouting> primaryShards = new LinkedHashMap<>();
        final LinkedHashMap<ShardId, ShardRouting> replicaShards = new LinkedHashMap<>();
        this.shards = new BucketedShards(primaryShards, replicaShards);
        this.relocatingShards = new LinkedHashSet<>();
        this.initializingShards = new LinkedHashSet<>();
        this.shardsByIndex = new LinkedHashMap<>();

        for (ShardRouting shardRouting : shardRoutings) {
            if (shardRouting.initializing()) {
                initializingShards.add(shardRouting);
            } else if (shardRouting.relocating()) {
                relocatingShards.add(shardRouting);
            }
            shardsByIndex.computeIfAbsent(shardRouting.index(), k -> new LinkedHashSet<>()).add(shardRouting);

            ShardRouting previousValue;
            if (shardRouting.primary()) {
                previousValue = primaryShards.put(shardRouting.shardId(), shardRouting);
            } else {
                previousValue = replicaShards.put(shardRouting.shardId(), shardRouting);
            }

            if (previousValue != null) {
                throw new IllegalArgumentException(
                    "Cannot have two different shards with same shard id " + shardRouting.shardId() + " on same node "
                );
            }
        }

        assert invariant();
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    /**
     * Returns the nodes {@link DiscoveryNode}.
     *
     * @return discoveryNode of this node
     */
    public DiscoveryNode node() {
        return this.node;
    }

    @Nullable
    public ShardRouting getByShardId(ShardId id) {
        return shards.get(id);
    }

    /**
     * Get the id of this node
     * @return id of the node
     */
    public String nodeId() {
        return this.nodeId;
    }

    public int size() {
        return shards.size();
    }

    public Collection<ShardRouting> getInitializingShards() {
        return initializingShards;
    }

    /**
     * Add a new shard to this node
     * @param shard Shard to create on this Node
     */
    void add(ShardRouting shard) {
        assert invariant();
        if (shards.put(shard) != null) {
            throw new IllegalStateException(
                "Trying to add a shard "
                    + shard.shardId()
                    + " to a node ["
                    + nodeId
                    + "] where it already exists. current ["
                    + shards.get(shard.shardId())
                    + "]. new ["
                    + shard
                    + "]"
            );
        }

        if (shard.initializing()) {
            initializingShards.add(shard);
        } else if (shard.relocating()) {
            relocatingShards.add(shard);
        }
        shardsByIndex.computeIfAbsent(shard.index(), k -> new LinkedHashSet<>()).add(shard);
        assert invariant();
    }

    void update(ShardRouting oldShard, ShardRouting newShard) {
        assert invariant();
        if (shards.containsKey(oldShard.shardId()) == false) {
            // Shard was already removed by routing nodes iterator
            // TODO: change caller logic in RoutingNodes so that this check can go away
            return;
        }
        ShardRouting previousValue = shards.put(newShard.shardId(), newShard);
        assert previousValue == oldShard : "expected shard " + previousValue + " but was " + oldShard;

        if (oldShard.initializing()) {
            boolean exist = initializingShards.remove(oldShard);
            assert exist : "expected shard " + oldShard + " to exist in initializingShards";
        } else if (oldShard.relocating()) {
            boolean exist = relocatingShards.remove(oldShard);
            assert exist : "expected shard " + oldShard + " to exist in relocatingShards";
        }
        shardsByIndex.get(oldShard.index()).remove(oldShard);
        if (shardsByIndex.get(oldShard.index()).isEmpty()) {
            shardsByIndex.remove(oldShard.index());
        }
        if (newShard.initializing()) {
            initializingShards.add(newShard);
        } else if (newShard.relocating()) {
            relocatingShards.add(newShard);
        }
        shardsByIndex.computeIfAbsent(newShard.index(), k -> new LinkedHashSet<>()).add(newShard);
        assert invariant();
    }

    void remove(ShardRouting shard) {
        assert invariant();
        ShardRouting previousValue = shards.remove(shard.shardId());
        assert previousValue == shard : "expected shard " + previousValue + " but was " + shard;
        if (shard.initializing()) {
            boolean exist = initializingShards.remove(shard);
            assert exist : "expected shard " + shard + " to exist in initializingShards";
        } else if (shard.relocating()) {
            boolean exist = relocatingShards.remove(shard);
            assert exist : "expected shard " + shard + " to exist in relocatingShards";
        }
        shardsByIndex.get(shard.index()).remove(shard);
        if (shardsByIndex.get(shard.index()).isEmpty()) {
            shardsByIndex.remove(shard.index());
        }
        assert invariant();
    }

    /**
     * Determine the number of shards with a specific state
     * @param states set of states which should be counted
     * @return number of shards
     */
    public int numberOfShardsWithState(ShardRoutingState... states) {
        if (states.length == 1) {
            if (states[0] == ShardRoutingState.INITIALIZING) {
                return initializingShards.size();
            } else if (states[0] == ShardRoutingState.RELOCATING) {
                return relocatingShards.size();
            }
        }

        int count = 0;
        for (ShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Determine the shards with a specific state
     * @param states set of states which should be listed
     * @return List of shards
     */
    public List<ShardRouting> shardsWithState(ShardRoutingState... states) {
        if (states.length == 1) {
            if (states[0] == ShardRoutingState.INITIALIZING) {
                return new ArrayList<>(initializingShards);
            } else if (states[0] == ShardRoutingState.RELOCATING) {
                return new ArrayList<>(relocatingShards);
            }
        }

        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : this) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    /**
     * Determine the shards of an index with a specific state
     * @param index id of the index
     * @param states set of states which should be listed
     * @return a list of shards
     */
    public List<ShardRouting> shardsWithState(String index, ShardRoutingState... states) {
        List<ShardRouting> shards = new ArrayList<>();

        if (states.length == 1) {
            if (states[0] == ShardRoutingState.INITIALIZING) {
                for (ShardRouting shardEntry : initializingShards) {
                    if (shardEntry.getIndexName().equals(index) == false) {
                        continue;
                    }
                    shards.add(shardEntry);
                }
                return shards;
            } else if (states[0] == ShardRoutingState.RELOCATING) {
                for (ShardRouting shardEntry : relocatingShards) {
                    if (shardEntry.getIndexName().equals(index) == false) {
                        continue;
                    }
                    shards.add(shardEntry);
                }
                return shards;
            }
        }

        for (ShardRouting shardEntry : this) {
            if (!shardEntry.getIndexName().equals(index)) {
                continue;
            }
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    /**
     * The number of shards on this node that will not be eventually relocated.
     */
    public int numberOfOwningShards() {
        return shards.size() - relocatingShards.size();
    }

    public int numberOfOwningShardsForIndex(final Index index) {
        final LinkedHashSet<ShardRouting> shardRoutings = shardsByIndex.get(index);
        if (shardRoutings == null) {
            return 0;
        } else {
            return Math.toIntExact(shardRoutings.stream().filter(sr -> sr.relocating() == false).count());
        }
    }

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("-----node_id[").append(nodeId).append("][").append(node == null ? "X" : "V").append("]\n");
        for (ShardRouting entry : shards) {
            sb.append("--------").append(entry.shortSummary()).append('\n');
        }
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("routingNode ([");
        sb.append(node.getName());
        sb.append("][");
        sb.append(node.getId());
        sb.append("][");
        sb.append(node.getHostName());
        sb.append("][");
        sb.append(node.getHostAddress());
        sb.append("], [");
        sb.append(shards.size());
        sb.append(" assigned shards])");
        return sb.toString();
    }

    public List<ShardRouting> copyShards() {
        List<ShardRouting> result = new ArrayList<>();
        shards.forEach(result::add);
        return result;
    }

    public boolean isEmpty() {
        return shards.isEmpty();
    }

    private boolean invariant() {

        // initializingShards must consistent with that in shards
        Collection<ShardRouting> shardRoutingsInitializing = StreamSupport.stream(shards.spliterator(), false)
            .filter(ShardRouting::initializing)
            .collect(Collectors.toList());
        assert initializingShards.size() == shardRoutingsInitializing.size();
        assert initializingShards.containsAll(shardRoutingsInitializing);

        // relocatingShards must consistent with that in shards
        Collection<ShardRouting> shardRoutingsRelocating = StreamSupport.stream(shards.spliterator(), false)
            .filter(ShardRouting::relocating)
            .collect(Collectors.toList());
        assert relocatingShards.size() == shardRoutingsRelocating.size();
        assert relocatingShards.containsAll(shardRoutingsRelocating);

        final Map<Index, Set<ShardRouting>> shardRoutingsByIndex = StreamSupport.stream(shards.spliterator(), false)
            .collect(Collectors.groupingBy(ShardRouting::index, Collectors.toSet()));
        assert shardRoutingsByIndex.equals(shardsByIndex);

        return true;
    }
}
