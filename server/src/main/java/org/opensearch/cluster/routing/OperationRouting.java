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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.node.ResponseCollectorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Routes cluster operations
 *
 * @opensearch.internal
 */
public class OperationRouting {

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING = Setting.boolSetting(
        "cluster.routing.use_adaptive_replica_selection",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final String IGNORE_AWARENESS_ATTRIBUTES = "cluster.search.ignore_awareness_attributes";
    public static final Setting<Boolean> IGNORE_AWARENESS_ATTRIBUTES_SETTING = Setting.boolSetting(
        IGNORE_AWARENESS_ATTRIBUTES,
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Double> WEIGHTED_ROUTING_DEFAULT_WEIGHT = Setting.doubleSetting(
        "cluster.routing.weighted.default_weight",
        1.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> WEIGHTED_ROUTING_FAILOPEN_ENABLED = Setting.boolSetting(
        "cluster.routing.weighted.fail_open",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> STRICT_WEIGHTED_SHARD_ROUTING_ENABLED = Setting.boolSetting(
        "cluster.routing.weighted.strict",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> IGNORE_WEIGHTED_SHARD_ROUTING = Setting.boolSetting(
        "cluster.routing.ignore_weighted_routing",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final List<Preference> WEIGHTED_ROUTING_RESTRICTED_PREFERENCES = Arrays.asList(
        Preference.ONLY_NODES,
        Preference.PREFER_NODES
    );

    private volatile List<String> awarenessAttributes;
    private volatile boolean useAdaptiveReplicaSelection;
    private volatile boolean ignoreAwarenessAttr;
    private volatile double weightedRoutingDefaultWeight;
    private volatile boolean isFailOpenEnabled;
    private volatile boolean isStrictWeightedShardRouting;
    private volatile boolean ignoreWeightedRouting;

    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        // whether to ignore awareness attributes when routing requests
        this.ignoreAwarenessAttr = clusterSettings.get(IGNORE_AWARENESS_ATTRIBUTES_SETTING);
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        this.weightedRoutingDefaultWeight = WEIGHTED_ROUTING_DEFAULT_WEIGHT.get(settings);
        this.isFailOpenEnabled = WEIGHTED_ROUTING_FAILOPEN_ENABLED.get(settings);
        this.isStrictWeightedShardRouting = STRICT_WEIGHTED_SHARD_ROUTING_ENABLED.get(settings);
        this.ignoreWeightedRouting = IGNORE_WEIGHTED_SHARD_ROUTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
        clusterSettings.addSettingsUpdateConsumer(IGNORE_AWARENESS_ATTRIBUTES_SETTING, this::setIgnoreAwarenessAttributes);
        clusterSettings.addSettingsUpdateConsumer(WEIGHTED_ROUTING_DEFAULT_WEIGHT, this::setWeightedRoutingDefaultWeight);
        clusterSettings.addSettingsUpdateConsumer(WEIGHTED_ROUTING_FAILOPEN_ENABLED, this::setFailOpenEnabled);
        clusterSettings.addSettingsUpdateConsumer(STRICT_WEIGHTED_SHARD_ROUTING_ENABLED, this::setStrictWeightedShardRouting);
        clusterSettings.addSettingsUpdateConsumer(IGNORE_WEIGHTED_SHARD_ROUTING, this::setIgnoreWeightedRouting);
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    void setIgnoreAwarenessAttributes(boolean ignoreAwarenessAttributes) {
        this.ignoreAwarenessAttr = ignoreAwarenessAttributes;
    }

    void setWeightedRoutingDefaultWeight(double weightedRoutingDefaultWeight) {
        this.weightedRoutingDefaultWeight = weightedRoutingDefaultWeight;
    }

    void setFailOpenEnabled(boolean isFailOpenEnabled) {
        this.isFailOpenEnabled = isFailOpenEnabled;
    }

    void setStrictWeightedShardRouting(boolean strictWeightedShardRouting) {
        this.isStrictWeightedShardRouting = strictWeightedShardRouting;
    }

    void setIgnoreWeightedRouting(boolean isWeightedRoundRobinEnabled) {
        this.ignoreWeightedRouting = isWeightedRoundRobinEnabled;
    }

    public boolean isIgnoreAwarenessAttr() {
        return ignoreAwarenessAttr;
    }

    List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    public boolean ignoreAwarenessAttributes() {
        return this.awarenessAttributes.isEmpty() || this.ignoreAwarenessAttr;
    }

    public double getWeightedRoutingDefaultWeight() {
        return this.weightedRoutingDefaultWeight;
    }

    public ShardIterator indexShards(ClusterState clusterState, String index, String id, @Nullable String routing) {
        return shards(clusterState, index, id, routing).shardsIt();
    }

    public ShardIterator getShards(
        ClusterState clusterState,
        String index,
        String id,
        @Nullable String routing,
        @Nullable String preference
    ) {
        return preferenceActiveShardIterator(
            shards(clusterState, index, id, routing),
            clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(),
            preference,
            null,
            null,
            clusterState.getMetadata().weightedRoutingMetadata()
        );
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
        return preferenceActiveShardIterator(
            indexShard,
            clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(),
            preference,
            null,
            null,
            clusterState.metadata().weightedRoutingMetadata()
        );
    }

    public GroupShardsIterator<ShardIterator> searchShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference
    ) {
        return searchShards(clusterState, concreteIndices, routing, preference, null, null);
    }

    public GroupShardsIterator<ShardIterator> searchShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts
    ) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = new HashSet<>(shards.size());
        for (IndexShardRoutingTable shard : shards) {
            IndexMetadata indexMetadataForShard = indexMetadata(clusterState, shard.shardId.getIndex().getName());
            if (IndexModule.Type.REMOTE_SNAPSHOT.match(
                indexMetadataForShard.getSettings().get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
            ) && (preference == null || preference.isEmpty())) {
                preference = Preference.PRIMARY.type();
            }

            ShardIterator iterator = preferenceActiveShardIterator(
                shard,
                clusterState.nodes().getLocalNodeId(),
                clusterState.nodes(),
                preference,
                collectorService,
                nodeCounts,
                clusterState.metadata().weightedRoutingMetadata()
            );
            if (iterator != null) {
                set.add(iterator);
            }
        }
        return GroupShardsIterator.sortAndCreate(new ArrayList<>(set));
    }

    public static ShardIterator getShards(ClusterState clusterState, ShardId shardId) {
        final IndexShardRoutingTable shard = clusterState.routingTable().shardRoutingTable(shardId);
        return shard.activeInitializingShardsRandomIt();
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();

    private Set<IndexShardRoutingTable> computeTargetedShards(
        ClusterState clusterState,
        String[] concreteIndices,
        @Nullable Map<String, Set<String>> routing
    ) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final IndexMetadata indexMetadata = indexMetadata(clusterState, index);
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                for (String r : effectiveRouting) {
                    final int routingPartitionSize = indexMetadata.getRoutingPartitionSize();
                    for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {
                        set.add(RoutingTable.shardRoutingTable(indexRouting, calculateScaledShardId(indexMetadata, r, partitionOffset)));
                    }
                }
            } else {
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }
        }
        return set;
    }

    private ShardIterator preferenceActiveShardIterator(
        IndexShardRoutingTable indexShard,
        String localNodeId,
        DiscoveryNodes nodes,
        @Nullable String preference,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts,
        @Nullable WeightedRoutingMetadata weightedRoutingMetadata
    ) {
        if (preference == null || preference.isEmpty()) {
            return shardRoutings(indexShard, nodes, collectorService, nodeCounts, weightedRoutingMetadata);
        }

        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                String shards;
                if (index == -1) {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    return shardRoutings(indexShard, nodes, collectorService, nodeCounts, weightedRoutingMetadata);
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);
                }
            }
            preferenceType = Preference.parse(preference);
            checkPreferenceBasedRoutingAllowed(preferenceType, weightedRoutingMetadata);
            switch (preferenceType) {
                case PREFER_NODES:
                    final Set<String> nodesIds = Arrays.stream(preference.substring(Preference.PREFER_NODES.type().length() + 1).split(","))
                        .collect(Collectors.toSet());
                    return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                case LOCAL:
                    return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                case PRIMARY:
                    return indexShard.primaryActiveInitializingShardIt();
                case REPLICA:
                    return indexShard.replicaActiveInitializingShardIt();
                case PRIMARY_FIRST:
                    return indexShard.primaryFirstActiveInitializingShardsIt();
                case REPLICA_FIRST:
                    return indexShard.replicaFirstActiveInitializingShardsIt();
                case ONLY_LOCAL:
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODES:
                    String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }
        // if not, then use it as the index
        int routingHash = Murmur3HashFunction.hash(preference);
        // The AllocationService lists shards in a fixed order based on nodes
        // so earlier versions of this class would have a tendency to
        // select the same node across different shardIds.
        // Better overall balancing can be achieved if each shardId opts
        // for a different element in the list by also incorporating the
        // shard ID into the hash of the user-supplied preference key.
        routingHash = 31 * routingHash + indexShard.shardId.hashCode();
        if (WeightedRoutingUtils.shouldPerformStrictWeightedRouting(
            isStrictWeightedShardRouting,
            ignoreWeightedRouting,
            weightedRoutingMetadata
        )) {
            return indexShard.activeInitializingShardsWeightedIt(
                weightedRoutingMetadata.getWeightedRouting(),
                nodes,
                getWeightedRoutingDefaultWeight(),
                isFailOpenEnabled,
                routingHash
            );
        } else if (ignoreAwarenessAttributes()) {
            return indexShard.activeInitializingShardsIt(routingHash);
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes, routingHash);
        }
    }

    private ShardIterator shardRoutings(
        IndexShardRoutingTable indexShard,
        DiscoveryNodes nodes,
        @Nullable ResponseCollectorService collectorService,
        @Nullable Map<String, Long> nodeCounts,
        @Nullable WeightedRoutingMetadata weightedRoutingMetadata
    ) {
        if (WeightedRoutingUtils.shouldPerformWeightedRouting(ignoreWeightedRouting, weightedRoutingMetadata)) {
            return indexShard.activeInitializingShardsWeightedIt(
                weightedRoutingMetadata.getWeightedRouting(),
                nodes,
                getWeightedRoutingDefaultWeight(),
                isFailOpenEnabled,
                null
            );
        } else if (ignoreAwarenessAttributes()) {
            if (useAdaptiveReplicaSelection) {
                return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
            } else {
                return indexShard.activeInitializingShardsRandomIt();
            }
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
        }
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    protected IndexMetadata indexMetadata(ClusterState clusterState, String index) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetadata;
    }

    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String id, String routing) {
        int shardId = generateShardId(indexMetadata(clusterState, index), id, routing);
        return clusterState.getRoutingTable().shardRoutingTable(index, shardId);
    }

    public ShardId shardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        IndexMetadata indexMetadata = indexMetadata(clusterState, index);
        return new ShardId(indexMetadata.getIndex(), generateShardId(indexMetadata, id, routing));
    }

    public static int generateShardId(IndexMetadata indexMetadata, @Nullable String id, @Nullable String routing) {
        final String effectiveRouting;
        final int partitionOffset;

        if (routing == null) {
            assert (indexMetadata.isRoutingPartitionedIndex() == false) : "A routing value is required for gets from a partitioned index";
            effectiveRouting = id;
        } else {
            effectiveRouting = routing;
        }

        if (indexMetadata.isRoutingPartitionedIndex()) {
            partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), indexMetadata.getRoutingPartitionSize());
        } else {
            // we would have still got 0 above but this check just saves us an unnecessary hash calculation
            partitionOffset = 0;
        }

        return calculateScaledShardId(indexMetadata, effectiveRouting, partitionOffset);
    }

    private static int calculateScaledShardId(IndexMetadata indexMetadata, String effectiveRouting, int partitionOffset) {
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;

        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        return Math.floorMod(hash, indexMetadata.getRoutingNumShards()) / indexMetadata.getRoutingFactor();
    }

    private void checkPreferenceBasedRoutingAllowed(Preference preference, @Nullable WeightedRoutingMetadata weightedRoutingMetadata) {
        if (WeightedRoutingUtils.shouldPerformStrictWeightedRouting(
            isStrictWeightedShardRouting,
            ignoreWeightedRouting,
            weightedRoutingMetadata
        ) && WEIGHTED_ROUTING_RESTRICTED_PREFERENCES.contains(preference)) {
            throw new PreferenceBasedSearchNotAllowedException(
                "Preference type based routing not allowed with strict weighted shard routing enabled"
            );
        }
    }
}
