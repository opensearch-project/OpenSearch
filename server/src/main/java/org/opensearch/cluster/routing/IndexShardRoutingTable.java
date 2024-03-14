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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Nullable;
import org.opensearch.common.Randomness;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.node.ResponseCollectorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

/**
 * {@link IndexShardRoutingTable} encapsulates all instances of a single shard.
 * Each OpenSearch index consists of multiple shards, each shard encapsulates
 * a disjoint set of the index data and each shard has one or more instances
 * referred to as replicas of a shard. Given that, this class encapsulates all
 * replicas (instances) for a single index shard.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexShardRoutingTable implements Iterable<ShardRouting> {

    final ShardShuffler shuffler;
    // Shuffler for weighted round-robin shard routing. This uses rotation to permute shards.
    final ShardShuffler shufflerForWeightedRouting;
    final ShardId shardId;

    final ShardRouting primary;
    final List<ShardRouting> replicas;
    final List<ShardRouting> shards;
    final List<ShardRouting> activeShards;
    final List<ShardRouting> assignedShards;
    final Set<String> allAllocationIds;
    final boolean allShardsStarted;

    private volatile Map<AttributesKey, AttributesRoutings> activeShardsByAttributes = emptyMap();
    private volatile Map<AttributesKey, AttributesRoutings> initializingShardsByAttributes = emptyMap();
    private final Object shardsByAttributeMutex = new Object();
    private final Object shardsByWeightMutex = new Object();
    private volatile Map<WeightedRoutingKey, List<ShardRouting>> activeShardsByWeight = emptyMap();
    private volatile Map<WeightedRoutingKey, List<ShardRouting>> initializingShardsByWeight = emptyMap();

    private static final Logger logger = LogManager.getLogger(IndexShardRoutingTable.class);

    /**
     * The initializing list, including ones that are initializing on a target node because of relocation.
     * If we can come up with a better variable name, it would be nice...
     */
    final List<ShardRouting> allInitializingShards;

    IndexShardRoutingTable(ShardId shardId, List<ShardRouting> shards) {
        this.shardId = shardId;
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shufflerForWeightedRouting = new RotationShardShuffler(Randomness.get().nextInt());
        this.shards = Collections.unmodifiableList(shards);

        ShardRouting primary = null;
        List<ShardRouting> replicas = new ArrayList<>();
        List<ShardRouting> activeShards = new ArrayList<>();
        List<ShardRouting> assignedShards = new ArrayList<>();
        List<ShardRouting> allInitializingShards = new ArrayList<>();
        Set<String> allAllocationIds = new HashSet<>();
        boolean allShardsStarted = true;
        for (ShardRouting shard : shards) {
            if (shard.primary()) {
                primary = shard;
            } else {
                replicas.add(shard);
            }
            if (shard.active()) {
                activeShards.add(shard);
            }
            if (shard.initializing()) {
                allInitializingShards.add(shard);
            }
            if (shard.relocating()) {
                // create the target initializing shard routing on the node the shard is relocating to
                allInitializingShards.add(shard.getTargetRelocatingShard());
                allAllocationIds.add(shard.getTargetRelocatingShard().allocationId().getId());

                assert shard.assignedToNode() : "relocating from unassigned " + shard;
                assert shard.getTargetRelocatingShard().assignedToNode() : "relocating to unassigned " + shard.getTargetRelocatingShard();
                assignedShards.add(shard.getTargetRelocatingShard());
            }
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
                allAllocationIds.add(shard.allocationId().getId());
            }
            if (shard.state() != ShardRoutingState.STARTED) {
                allShardsStarted = false;
            }
        }
        this.allShardsStarted = allShardsStarted;
        this.primary = primary;
        this.replicas = Collections.unmodifiableList(replicas);
        this.activeShards = Collections.unmodifiableList(activeShards);
        this.assignedShards = Collections.unmodifiableList(assignedShards);
        this.allInitializingShards = Collections.unmodifiableList(allInitializingShards);
        this.allAllocationIds = Collections.unmodifiableSet(allAllocationIds);
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId getShardId() {
        return shardId();
    }

    @Override
    public Iterator<ShardRouting> iterator() {
        return shards.iterator();
    }

    /**
     * Returns the number of this shards instances.
     */
    public int size() {
        return shards.size();
    }

    /**
     * Returns the number of this shards instances.
     */
    public int getSize() {
        return size();
    }

    /**
     * Returns a {@link List} of shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> shards() {
        return this.shards;
    }

    /**
     * Returns a {@link List} of shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> getShards() {
        return shards();
    }

    /**
     * Returns a {@link List} of active shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> activeShards() {
        return this.activeShards;
    }

    /**
     * Returns a {@link List} of all initializing shards, including target shards of relocations
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> getAllInitializingShards() {
        return this.allInitializingShards;
    }

    /**
     * Returns a {@link List} of active shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> getActiveShards() {
        return activeShards();
    }

    /**
     * Returns a {@link List} of assigned shards, including relocation targets
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> assignedShards() {
        return this.assignedShards;
    }

    public Map<WeightedRoutingKey, List<ShardRouting>> getActiveShardsByWeight() {
        return activeShardsByWeight;
    }

    public ShardIterator shardsRandomIt() {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards));
    }

    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, shards);
    }

    public ShardIterator shardsIt(int seed) {
        return new PlainShardIterator(shardId, shuffler.shuffle(shards, seed));
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsRandomIt() {
        return activeInitializingShardsIt(shuffler.nextSeed());
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsIt(int seed) {
        if (allInitializingShards.isEmpty()) {
            return new PlainShardIterator(shardId, shuffler.shuffle(activeShards, seed));
        }
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ordered.addAll(shuffler.shuffle(activeShards, seed));
        ordered.addAll(allInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator over active and initializing shards, ordered by the adaptive replica
     * selection formula. Making sure though that its random within the active shards of the same
     * (or missing) rank, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsRankedIt(
        @Nullable ResponseCollectorService collector,
        @Nullable Map<String, Long> nodeSearchCounts
    ) {
        final int seed = shuffler.nextSeed();
        if (allInitializingShards.isEmpty()) {
            return new PlainShardIterator(
                shardId,
                rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), collector, nodeSearchCounts)
            );
        }

        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        List<ShardRouting> rankedActiveShards = rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), collector, nodeSearchCounts);
        ordered.addAll(rankedActiveShards);
        List<ShardRouting> rankedInitializingShards = rankShardsAndUpdateStats(allInitializingShards, collector, nodeSearchCounts);
        ordered.addAll(rankedInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator over active and initializing shards, shards are ordered by weighted
     * round-robin scheduling policy.
     *
     * @param weightedRouting entity
     * @param nodes           discovered nodes in the cluster
     * @param isFailOpenEnabled if true, shards search requests in case of failures are tried on shard copies present
     *                          in node attribute value with weight zero
     * @return an iterator over active and initializing shards, ordered by weighted round-robin
     * scheduling policy. Making sure that initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsWeightedIt(
        WeightedRouting weightedRouting,
        DiscoveryNodes nodes,
        double defaultWeight,
        boolean isFailOpenEnabled,
        @Nullable Integer seed
    ) {
        if (seed == null) {
            seed = shufflerForWeightedRouting.nextSeed();
        }
        List<ShardRouting> ordered = activeInitializingShardsWithWeights(weightedRouting, nodes, defaultWeight, seed);

        // append shards for attribute value with weight zero, so that shard search requests can be tried on
        // shard copies in case of request failure from other attribute values.
        if (isFailOpenEnabled) {
            try {
                Stream<String> keys = weightedRouting.weights()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().intValue() == WeightedRoutingMetadata.WEIGHED_AWAY_WEIGHT)
                    .map(Map.Entry::getKey);
                keys.forEach(key -> {
                    ShardIterator iterator = onlyNodeSelectorActiveInitializingShardsIt(weightedRouting.attributeName() + ":" + key, nodes);
                    while (iterator.remaining() > 0) {
                        ordered.add(iterator.nextOrNull());
                    }
                });
            } catch (IllegalArgumentException e) {
                // this exception is thrown by {@link onlyNodeSelectorActiveInitializingShardsIt} in case count of shard
                // copies found is zero
                logger.debug("no shard copies found for shard id [{}] for node attribute with weight zero", shardId);
            }
        }

        return new PlainShardIterator(shardId, ordered);
    }

    private List<ShardRouting> activeInitializingShardsWithWeights(
        WeightedRouting weightedRouting,
        DiscoveryNodes nodes,
        double defaultWeight,
        int seed
    ) {
        List<ShardRouting> ordered = new ArrayList<>();
        List<ShardRouting> orderedActiveShards = getActiveShardsByWeight(weightedRouting, nodes, defaultWeight);
        ordered.addAll(shufflerForWeightedRouting.shuffle(orderedActiveShards, seed));
        if (!allInitializingShards.isEmpty()) {
            List<ShardRouting> orderedInitializingShards = getInitializingShardsByWeight(weightedRouting, nodes, defaultWeight);
            ordered.addAll(orderedInitializingShards);
        }
        List<ShardRouting> orderedListWithDistinctShards;
        orderedListWithDistinctShards = ordered.stream().distinct().collect(Collectors.toList());
        return orderedListWithDistinctShards;
    }

    /**
     * Returns a list containing shard routings ordered using weighted round-robin scheduling.
     */
    private List<ShardRouting> shardsOrderedByWeight(
        List<ShardRouting> shards,
        WeightedRouting weightedRouting,
        DiscoveryNodes nodes,
        double defaultWeight
    ) {
        WeightedRoundRobin<ShardRouting> weightedRoundRobin = new WeightedRoundRobin<>(
            calculateShardWeight(shards, weightedRouting, nodes, defaultWeight)
        );
        List<WeightedRoundRobin.Entity<ShardRouting>> shardsOrderedbyWeight = weightedRoundRobin.orderEntities();
        List<ShardRouting> orderedShardRouting = new ArrayList<>(activeShards.size());
        if (shardsOrderedbyWeight != null) {
            for (WeightedRoundRobin.Entity<ShardRouting> shardRouting : shardsOrderedbyWeight) {
                orderedShardRouting.add(shardRouting.getTarget());
            }
        }
        return orderedShardRouting;
    }

    /**
     * Returns a list containing shard routing and associated weight. This function iterates through all the shards and
     * uses weighted routing to find weight for the corresponding shard. This is fed to weighted round-robin scheduling
     * to order shards by weight.
     */
    private List<WeightedRoundRobin.Entity<ShardRouting>> calculateShardWeight(
        List<ShardRouting> shards,
        WeightedRouting weightedRouting,
        DiscoveryNodes nodes,
        double defaultWeight
    ) {
        List<WeightedRoundRobin.Entity<ShardRouting>> shardsWithWeights = new ArrayList<>();
        for (ShardRouting shard : shards) {
            DiscoveryNode node = nodes.get(shard.currentNodeId());
            if (node != null) {
                String attVal = node.getAttributes().get(weightedRouting.attributeName());
                // If weight for a zone is not defined, considering it as 1 by default
                Double weight = weightedRouting.weights().getOrDefault(attVal, defaultWeight);
                shardsWithWeights.add(new WeightedRoundRobin.Entity<>(weight, shard));
            }
        }
        return shardsWithWeights;
    }

    private static Set<String> getAllNodeIds(final List<ShardRouting> shards) {
        final Set<String> nodeIds = new HashSet<>();
        for (ShardRouting shard : shards) {
            nodeIds.add(shard.currentNodeId());
        }
        return nodeIds;
    }

    private static Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> getNodeStats(
        final Set<String> nodeIds,
        final ResponseCollectorService collector
    ) {

        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>(nodeIds.size());
        for (String nodeId : nodeIds) {
            nodeStats.put(nodeId, collector.getNodeStatistics(nodeId));
        }
        return nodeStats;
    }

    private static Map<String, Double> rankNodes(
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats,
        final Map<String, Long> nodeSearchCounts
    ) {
        final Map<String, Double> nodeRanks = new HashMap<>(nodeStats.size());
        for (Map.Entry<String, Optional<ResponseCollectorService.ComputedNodeStats>> entry : nodeStats.entrySet()) {
            Optional<ResponseCollectorService.ComputedNodeStats> maybeStats = entry.getValue();
            maybeStats.ifPresent(stats -> {
                final String nodeId = entry.getKey();
                nodeRanks.put(nodeId, stats.rank(nodeSearchCounts.getOrDefault(nodeId, 1L)));
            });
        }
        return nodeRanks;
    }

    /**
     * Adjust the for all other nodes' collected stats. In the original ranking paper there is no need to adjust other nodes' stats because
     * Cassandra sends occasional requests to all copies of the data, so their stats will be updated during that broadcast phase. In
     * OpenSearch, however, we do not have that sort of broadcast-to-all behavior. In order to prevent a node that gets a high score and
     * then never gets any more requests, we must ensure it eventually returns to a more normal score and can be a candidate for serving
     * requests.
     * <p>
     * This adjustment takes the "winning" node's statistics and adds the average of those statistics with each non-winning node. Let's say
     * the winning node had a queue size of 10 and a non-winning node had a queue of 18. The average queue size is (10 + 18) / 2 = 14 so the
     * non-winning node will have statistics added for a queue size of 14. This is repeated for the response time and service times as well.
     */
    private static void adjustStats(
        final ResponseCollectorService collector,
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats,
        final String minNodeId,
        final ResponseCollectorService.ComputedNodeStats minStats
    ) {
        if (minNodeId != null) {
            for (Map.Entry<String, Optional<ResponseCollectorService.ComputedNodeStats>> entry : nodeStats.entrySet()) {
                final String nodeId = entry.getKey();
                final Optional<ResponseCollectorService.ComputedNodeStats> maybeStats = entry.getValue();
                if (nodeId.equals(minNodeId) == false && maybeStats.isPresent()) {
                    final ResponseCollectorService.ComputedNodeStats stats = maybeStats.get();
                    final int updatedQueue = (minStats.queueSize + stats.queueSize) / 2;
                    final long updatedResponse = (long) (minStats.responseTime + stats.responseTime) / 2;
                    final long updatedService = (long) (minStats.serviceTime + stats.serviceTime) / 2;
                    collector.addNodeStatistics(nodeId, updatedQueue, updatedResponse, updatedService);
                }
            }
        }
    }

    private static List<ShardRouting> rankShardsAndUpdateStats(
        List<ShardRouting> shards,
        final ResponseCollectorService collector,
        final Map<String, Long> nodeSearchCounts
    ) {
        if (collector == null || nodeSearchCounts == null || shards.size() <= 1) {
            return shards;
        }

        // Retrieve which nodes we can potentially send the query to
        final Set<String> nodeIds = getAllNodeIds(shards);
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = getNodeStats(nodeIds, collector);

        // Retrieve all the nodes the shards exist on
        final Map<String, Double> nodeRanks = rankNodes(nodeStats, nodeSearchCounts);

        // sort all shards based on the shard rank
        ArrayList<ShardRouting> sortedShards = new ArrayList<>(shards);
        Collections.sort(sortedShards, new NodeRankComparator(nodeRanks));

        // adjust the non-winner nodes' stats so they will get a chance to receive queries
        if (sortedShards.size() > 1) {
            ShardRouting minShard = sortedShards.get(0);
            // If the winning shard is not started we are ranking initializing
            // shards, don't bother to do adjustments
            if (minShard.started()) {
                String minNodeId = minShard.currentNodeId();
                Optional<ResponseCollectorService.ComputedNodeStats> maybeMinStats = nodeStats.get(minNodeId);
                if (maybeMinStats.isPresent()) {
                    adjustStats(collector, nodeStats, minNodeId, maybeMinStats.get());
                    // Increase the number of searches for the "winning" node by one.
                    // Note that this doesn't actually affect the "real" counts, instead
                    // it only affects the captured node search counts, which is
                    // captured once for each query in TransportSearchAction
                    nodeSearchCounts.compute(minNodeId, (id, conns) -> conns == null ? 1 : conns + 1);
                }
            }
        }

        return sortedShards;
    }

    private static class NodeRankComparator implements Comparator<ShardRouting> {
        private final Map<String, Double> nodeRanks;

        NodeRankComparator(Map<String, Double> nodeRanks) {
            this.nodeRanks = nodeRanks;
        }

        @Override
        public int compare(ShardRouting s1, ShardRouting s2) {
            if (s1.currentNodeId().equals(s2.currentNodeId())) {
                // these shards on the same node
                return 0;
            }
            Double shard1rank = nodeRanks.get(s1.currentNodeId());
            Double shard2rank = nodeRanks.get(s2.currentNodeId());
            if (shard1rank != null) {
                if (shard2rank != null) {
                    return shard1rank.compareTo(shard2rank);
                } else {
                    // place non-nulls after null values
                    return 1;
                }
            } else {
                if (shard2rank != null) {
                    // place nulls before non-null values
                    return -1;
                } else {
                    // Both nodes do not have stats, they are equal
                    return 0;
                }
            }
        }
    }

    /**
     * Returns an iterator only on the primary shard.
     */
    public ShardIterator primaryShardIt() {
        if (primary != null) {
            return new PlainShardIterator(shardId, Collections.singletonList(primary));
        }
        return new PlainShardIterator(shardId, Collections.emptyList());
    }

    /**
     * Returns true if no primaries are active or initializing for this shard
     */
    private boolean noPrimariesActive() {
        return this.primary != null && !this.primary.active() && !this.primary.initializing();
    }

    /**
     * Returns an iterator only on the active primary shard.
     */
    public ShardIterator primaryActiveInitializingShardIt() {
        if (noPrimariesActive()) {
            return new PlainShardIterator(shardId, Collections.emptyList());
        }
        return primaryShardIt();
    }

    /**
     * Returns an ordered iterator on the active primary shard, followed by replica shards.
     */
    public ShardIterator primaryFirstActiveInitializingShardsIt() {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // fill it in a randomized fashion
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            ordered.add(shardRouting);
            if (shardRouting.primary()) {
                // switch, its the matching node id
                ordered.set(ordered.size() - 1, ordered.get(0));
                ordered.set(0, shardRouting);
            }
        }
        // no need to worry about primary first here..., its temporal
        if (!allInitializingShards.isEmpty()) {
            ordered.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator on replica shards.
     */
    public ShardIterator replicaActiveInitializingShardIt() {
        // If the primaries are unassigned, return an empty list (there aren't
        // any replicas to query anyway)
        if (noPrimariesActive()) {
            return new PlainShardIterator(shardId, Collections.emptyList());
        }

        LinkedList<ShardRouting> ordered = new LinkedList<>();
        for (ShardRouting replica : shuffler.shuffle(replicas)) {
            if (replica.active()) {
                ordered.addFirst(replica);
            } else if (replica.initializing()) {
                ordered.addLast(replica);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an ordered iterator on active replica shards, followed by the primary shard.
     */
    public ShardIterator replicaFirstActiveInitializingShardsIt() {
        // If the primaries are unassigned, return an empty list (there aren't
        // any replicas to query anyway)
        if (noPrimariesActive()) {
            return new PlainShardIterator(shardId, Collections.emptyList());
        }

        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // fill it in a randomized fashion with the active replicas
        for (ShardRouting replica : shuffler.shuffle(replicas)) {
            if (replica.active()) {
                ordered.add(replica);
            }
        }

        // Add the primary shard
        ordered.add(primary);

        // Add initializing shards last
        if (!allInitializingShards.isEmpty()) {
            ordered.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator on active and initializing shards residing on the provided nodeId.
     */
    public ShardIterator onlyNodeActiveInitializingShardsIt(String nodeId) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        int seed = shuffler.nextSeed();
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String nodeAttributes, DiscoveryNodes discoveryNodes) {
        return onlyNodeSelectorActiveInitializingShardsIt(new String[] { nodeAttributes }, discoveryNodes);
    }

    /**
     * Returns shards based on nodeAttributes given  such as node name , node attribute, node IP
     * Supports node specifications in cluster API
     */
    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String[] nodeAttributes, DiscoveryNodes discoveryNodes) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        Set<String> selectedNodes = Sets.newHashSet(discoveryNodes.resolveNodes(nodeAttributes));
        int seed = shuffler.nextSeed();
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        if (ordered.isEmpty()) {
            final String message = String.format(
                Locale.ROOT,
                "no data nodes with %s [%s] found for shard: %s",
                nodeAttributes.length == 1 ? "criteria" : "criterion",
                String.join(",", nodeAttributes),
                shardId()
            );
            throw new IllegalArgumentException(message);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator preferNodeActiveInitializingShardsIt(Set<String> nodeIds) {
        ArrayList<ShardRouting> preferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ArrayList<ShardRouting> notPreferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // fill it in a randomized fashion
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            if (nodeIds.contains(shardRouting.currentNodeId())) {
                preferred.add(shardRouting);
            } else {
                notPreferred.add(shardRouting);
            }
        }
        preferred.addAll(notPreferred);
        if (!allInitializingShards.isEmpty()) {
            preferred.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, preferred);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexShardRoutingTable that = (IndexShardRoutingTable) o;

        if (!shardId.equals(that.shardId)) return false;
        if (!shards.equals(that.shards)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + shards.hashCode();
        return result;
    }

    /**
     * Returns <code>true</code> iff all shards in the routing table are started otherwise <code>false</code>
     */
    public boolean allShardsStarted() {
        return allShardsStarted;
    }

    @Nullable
    public ShardRouting getByAllocationId(String allocationId) {
        for (ShardRouting shardRouting : assignedShards()) {
            if (shardRouting.allocationId().getId().equals(allocationId)) {
                return shardRouting;
            }
        }
        return null;
    }

    public Set<String> getAllAllocationIds() {
        return allAllocationIds;
    }

    static class AttributesKey {

        final List<String> attributes;

        AttributesKey(List<String> attributes) {
            this.attributes = attributes;
        }

        @Override
        public int hashCode() {
            return attributes.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AttributesKey && attributes.equals(((AttributesKey) obj).attributes);
        }
    }

    static class AttributesRoutings {

        public final List<ShardRouting> withSameAttribute;
        public final List<ShardRouting> withoutSameAttribute;
        public final int totalSize;

        AttributesRoutings(List<ShardRouting> withSameAttribute, List<ShardRouting> withoutSameAttribute) {
            this.withSameAttribute = withSameAttribute;
            this.withoutSameAttribute = withoutSameAttribute;
            this.totalSize = withoutSameAttribute.size() + withSameAttribute.size();
        }
    }

    private AttributesRoutings getActiveAttribute(AttributesKey key, DiscoveryNodes nodes) {
        AttributesRoutings shardRoutings = activeShardsByAttributes.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByAttributeMutex) {
                ArrayList<ShardRouting> from = new ArrayList<>(activeShards);
                List<ShardRouting> to = collectAttributeShards(key, nodes, from);

                shardRoutings = new AttributesRoutings(to, Collections.unmodifiableList(from));
                activeShardsByAttributes = MapBuilder.newMapBuilder(activeShardsByAttributes).put(key, shardRoutings).immutableMap();
            }
        }
        return shardRoutings;
    }

    private AttributesRoutings getInitializingAttribute(AttributesKey key, DiscoveryNodes nodes) {
        AttributesRoutings shardRoutings = initializingShardsByAttributes.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByAttributeMutex) {
                ArrayList<ShardRouting> from = new ArrayList<>(allInitializingShards);
                List<ShardRouting> to = collectAttributeShards(key, nodes, from);
                shardRoutings = new AttributesRoutings(to, Collections.unmodifiableList(from));
                initializingShardsByAttributes = MapBuilder.newMapBuilder(initializingShardsByAttributes)
                    .put(key, shardRoutings)
                    .immutableMap();
            }
        }
        return shardRoutings;
    }

    private static List<ShardRouting> collectAttributeShards(AttributesKey key, DiscoveryNodes nodes, ArrayList<ShardRouting> from) {
        final ArrayList<ShardRouting> to = new ArrayList<>();
        for (final String attribute : key.attributes) {
            final String localAttributeValue = nodes.getLocalNode().getAttributes().get(attribute);
            if (localAttributeValue != null) {
                for (Iterator<ShardRouting> iterator = from.iterator(); iterator.hasNext();) {
                    ShardRouting fromShard = iterator.next();
                    final DiscoveryNode discoveryNode = nodes.get(fromShard.currentNodeId());
                    if (discoveryNode == null) {
                        iterator.remove(); // node is not present anymore - ignore shard
                    } else if (localAttributeValue.equals(discoveryNode.getAttributes().get(attribute))) {
                        iterator.remove();
                        to.add(fromShard);
                    }
                }
            }
        }
        return Collections.unmodifiableList(to);
    }

    public ShardIterator preferAttributesActiveInitializingShardsIt(List<String> attributes, DiscoveryNodes nodes) {
        return preferAttributesActiveInitializingShardsIt(attributes, nodes, shuffler.nextSeed());
    }

    public ShardIterator preferAttributesActiveInitializingShardsIt(List<String> attributes, DiscoveryNodes nodes, int seed) {
        AttributesKey key = new AttributesKey(attributes);
        AttributesRoutings activeRoutings = getActiveAttribute(key, nodes);
        AttributesRoutings initializingRoutings = getInitializingAttribute(key, nodes);

        // we now randomize, once between the ones that have the same attributes, and once for the ones that don't
        // we don't want to mix between the two!
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeRoutings.totalSize + initializingRoutings.totalSize);
        ordered.addAll(shuffler.shuffle(activeRoutings.withSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(activeRoutings.withoutSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(initializingRoutings.withSameAttribute, seed));
        ordered.addAll(shuffler.shuffle(initializingRoutings.withoutSameAttribute, seed));
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardRouting primaryShard() {
        return primary;
    }

    public List<ShardRouting> replicaShards() {
        return this.replicas;
    }

    public List<ShardRouting> replicaShardsWithState(ShardRoutingState... states) {
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : replicas) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        if (state == ShardRoutingState.INITIALIZING) {
            return allInitializingShards;
        }
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : this) {
            if (shardEntry.state() == state) {
                shards.add(shardEntry);
            }
        }
        return shards;
    }

    public int shardsMatchingPredicateCount(Predicate<ShardRouting> predicate) {
        int count = 0;
        for (ShardRouting shardEntry : this) {
            if (predicate.test(shardEntry)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Key for WeightedRouting Shard Iterator
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.4.0")
    public static class WeightedRoutingKey {
        private final WeightedRouting weightedRouting;

        public WeightedRoutingKey(WeightedRouting weightedRouting) {
            this.weightedRouting = weightedRouting;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WeightedRoutingKey key = (WeightedRoutingKey) o;
            if (!weightedRouting.equals(key.weightedRouting)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = weightedRouting.hashCode();
            return result;
        }
    }

    /**
     * *
     * Gets active shard routing from memory if available, else calculates and put it in memory.
     */
    private List<ShardRouting> getActiveShardsByWeight(WeightedRouting weightedRouting, DiscoveryNodes nodes, double defaultWeight) {
        WeightedRoutingKey key = new WeightedRoutingKey(weightedRouting);
        List<ShardRouting> shardRoutings = activeShardsByWeight.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByWeightMutex) {
                shardRoutings = shardsOrderedByWeight(activeShards, weightedRouting, nodes, defaultWeight);
                activeShardsByWeight = new MapBuilder().put(key, shardRoutings).immutableMap();
            }
        }
        return shardRoutings;
    }

    /**
     * *
     * Gets initializing shard routing from memory if available, else calculates and put it in memory.
     */
    private List<ShardRouting> getInitializingShardsByWeight(WeightedRouting weightedRouting, DiscoveryNodes nodes, double defaultWeight) {
        WeightedRoutingKey key = new WeightedRoutingKey(weightedRouting);
        List<ShardRouting> shardRoutings = initializingShardsByWeight.get(key);
        if (shardRoutings == null) {
            synchronized (shardsByWeightMutex) {
                shardRoutings = shardsOrderedByWeight(activeShards, weightedRouting, nodes, defaultWeight);
                initializingShardsByWeight = new MapBuilder().put(key, shardRoutings).immutableMap();
            }
        }
        return shardRoutings;
    }

    /**
     * Builder of an index shard routing table.
     *
     * @opensearch.internal
     */
    public static class Builder {

        private ShardId shardId;
        private final List<ShardRouting> shards;

        public Builder(IndexShardRoutingTable indexShard) {
            this.shardId = indexShard.shardId;
            this.shards = new ArrayList<>(indexShard.shards);
        }

        public Builder(ShardId shardId) {
            this.shardId = shardId;
            this.shards = new ArrayList<>();
        }

        public Builder addShard(ShardRouting shardEntry) {
            shards.add(shardEntry);
            return this;
        }

        public Builder removeShard(ShardRouting shardEntry) {
            shards.remove(shardEntry);
            return this;
        }

        public IndexShardRoutingTable build() {
            // don't allow more than one shard copy with same id to be allocated to same node
            assert distinctNodes(shards) : "more than one shard with same id assigned to same node (shards: " + shards + ")";
            return new IndexShardRoutingTable(shardId, Collections.unmodifiableList(new ArrayList<>(shards)));
        }

        static boolean distinctNodes(List<ShardRouting> shards) {
            Set<String> nodes = new HashSet<>();
            for (ShardRouting shard : shards) {
                if (shard.assignedToNode()) {
                    if (nodes.add(shard.currentNodeId()) == false) {
                        return false;
                    }
                    if (shard.relocating()) {
                        if (nodes.add(shard.relocatingNodeId()) == false) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        public static IndexShardRoutingTable readFrom(StreamInput in) throws IOException {
            Index index = new Index(in);
            return readFromThin(in, index);
        }

        public static IndexShardRoutingTable readFromThin(StreamInput in, Index index) throws IOException {
            int iShardId = in.readVInt();
            ShardId shardId = new ShardId(index, iShardId);
            Builder builder = new Builder(shardId);

            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                ShardRouting shard = new ShardRouting(shardId, in);
                builder.addShard(shard);
            }

            return builder.build();
        }

        public static void writeTo(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            indexShard.shardId().getIndex().writeTo(out);
            writeToThin(indexShard, out);
        }

        public static void writeToThin(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());

            out.writeVInt(indexShard.shards.size());
            for (ShardRouting entry : indexShard) {
                entry.writeToThin(out);
            }
        }

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexShardRoutingTable(").append(shardId()).append("){");
        final int numShards = shards.size();
        for (int i = 0; i < numShards; i++) {
            sb.append(shards.get(i).shortSummary());
            if (i < numShards - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
