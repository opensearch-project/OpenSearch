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

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING;

/**
 * This {@link AllocationDecider} controls shard allocation based on
 * {@code awareness} key-value pairs defined in the node configuration.
 * Awareness explicitly controls where replicas should be allocated based on
 * attributes like node or physical rack locations. Awareness attributes accept
 * arbitrary configuration keys like a rack data-center identifier. For example
 * the setting:
 * <pre>
 * cluster.routing.allocation.awareness.attributes: rack_id
 * </pre>
 * <p>
 * will cause allocations to be distributed over different racks such that
 * ideally at least one replicas of the all shard is available on the same rack.
 * To enable allocation awareness in this example nodes should contain a value
 * for the {@code rack_id} key like:
 * <pre>
 * node.attr.rack_id:1
 * </pre>
 * <p>
 * Awareness can also be used to prevent over-allocation in the case of node or
 * even "zone" failure. For example in cloud-computing infrastructures like
 * Amazon AWS a cluster might span over multiple "zones". Awareness can be used
 * to distribute replicas to individual zones by setting:
 * <pre>
 * cluster.routing.allocation.awareness.attributes: zone
 * </pre>
 * <p>
 * and forcing allocation to be aware of the following zone the data resides in:
 * <pre>
 * cluster.routing.allocation.awareness.force.zone.values: zone1,zone2
 * </pre>
 * <p>
 * In contrast to regular awareness this setting will prevent over-allocation on
 * {@code zone1} even if {@code zone2} fails partially or becomes entirely
 * unavailable. Nodes that belong to a certain zone / group should be started
 * with the zone id configured on the node-level settings like:
 * <pre>
 * node.zone: zone1
 * </pre>
 *
 * @opensearch.internal
 */
public class AwarenessAllocationDecider extends AllocationDecider {

    public static final String NAME = "awareness";

    public static final Setting<List<String>> CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING = Setting.listSetting(
        "cluster.routing.allocation.awareness.attributes",
        emptyList(),
        Function.identity(),
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Settings> CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING = Setting.groupSetting(
        "cluster.routing.allocation.awareness.force.",
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile List<String> awarenessAttributes;
    private volatile Map<String, List<String>> forcedAwarenessAttributes;

    public AwarenessAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.awarenessAttributes = CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING, this::setAwarenessAttributes);
        setForcedAwarenessAttributes(CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
            this::setForcedAwarenessAttributes
        );
    }

    private void setForcedAwarenessAttributes(Settings forceSettings) {
        Map<String, List<String>> forcedAwarenessAttributes = new HashMap<>();
        Map<String, Settings> forceGroups = forceSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
            List<String> aValues = entry.getValue().getAsList("values");
            if (aValues.size() > 0) {
                forcedAwarenessAttributes.put(entry.getKey(), aValues);
            }
        }
        this.forcedAwarenessAttributes = forcedAwarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        if (awarenessAttributes.isEmpty()) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "allocation awareness is not enabled, set cluster setting [%s] to enable it",
                CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey()
            );
        }

        if (INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(indexMetadata.getSettings()).autoExpandToAll()) {
            return allocation.decision(Decision.YES, NAME, "allocation awareness is ignored, this index is set to auto-expand to all");
        }

        for (String awarenessAttribute : awarenessAttributes) {
            String nodeAttributeValue = node.getAttributes().get(awarenessAttribute);
            if (nodeAttributeValue == null) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not contain the awareness attribute [%s]; required attributes cluster setting [%s=%s]",
                    awarenessAttribute,
                    CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
                    allocation.debugDecision() ? Strings.collectionToCommaDelimitedString(awarenessAttributes) : null
                );
            }

            Map<String, Integer> nodeCountPerAttributeValue = new HashMap<>();
            for (DiscoveryNode dataNode : allocation.nodes().getDataNodes().values()) {
                if (dataNode.isSearchNode()) {
                    continue;
                }
                String attrValue = dataNode.getAttributes().get(awarenessAttribute);
                if (attrValue != null) {
                    nodeCountPerAttributeValue.merge(attrValue, 1, Integer::sum);
                }
            }

            Set<String> allAttributeValues = new HashSet<>(nodeCountPerAttributeValue.keySet());
            List<String> forcedValues = forcedAwarenessAttributes.get(awarenessAttribute);
            if (forcedValues != null) {
                allAttributeValues.addAll(forcedValues);
            }
            int numberOfAttributeValues = allAttributeValues.size();

            if (numberOfAttributeValues <= 1) {
                continue;
            }

            int totalDataNodes = nodeCountPerAttributeValue.values().stream().mapToInt(Integer::intValue).sum();
            int autoExpandMaxReplicas = INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(indexMetadata.getSettings()).getMaxReplicas();
            int upperBound = Math.min(totalDataNodes, autoExpandMaxReplicas + 1);
            int maxCopies = computeMaxAchievableCopies(nodeCountPerAttributeValue, numberOfAttributeValues, upperBound);
            int maxPerAttributeValue = (maxCopies + numberOfAttributeValues - 1) / numberOfAttributeValues;

            int nodesInSameGroup = nodeCountPerAttributeValue.getOrDefault(nodeAttributeValue, 0);
            if (nodesInSameGroup > maxPerAttributeValue) {
                long position = 0;
                for (DiscoveryNode dataNode : allocation.nodes().getDataNodes().values()) {
                    if (dataNode.isSearchNode()) {
                        continue;
                    }
                    String attrValue = dataNode.getAttributes().get(awarenessAttribute);
                    if (nodeAttributeValue.equals(attrValue) && dataNode.getId().compareTo(node.getId()) < 0) {
                        position++;
                    }
                }
                if (position >= maxPerAttributeValue) {
                    return allocation.decision(
                        Decision.NO,
                        NAME,
                        "too many nodes in awareness group [%s=%s] for auto-expand replicas; "
                            + "[%d] nodes in group but only [%d] are needed to satisfy awareness with [%d] attribute values",
                        awarenessAttribute,
                        nodeAttributeValue,
                        nodesInSameGroup,
                        maxPerAttributeValue,
                        numberOfAttributeValues
                    );
                }
            }
        }

        return allocation.decision(Decision.YES, NAME, "node meets all awareness attribute requirements for auto-expand");
    }

    /**
     * Computes the maximum number of shard copies that can be placed while respecting
     * awareness constraints. With K awareness attribute values and varying numbers of
     * nodes per value, finds the largest S such that sum(min(nodesPerValue_i, ceil(S/K))) &gt;= S.
     */
    public static int computeMaxAchievableCopies(Map<String, Integer> nodeCountPerAttributeValue, int numberOfAttributeValues) {
        int totalNodes = nodeCountPerAttributeValue.values().stream().mapToInt(Integer::intValue).sum();
        return computeMaxAchievableCopies(nodeCountPerAttributeValue, numberOfAttributeValues, totalNodes);
    }

    /**
     * Computes the maximum number of shard copies that can be placed while respecting
     * awareness constraints, with an upper bound on the number of copies to consider.
     * The upper bound accounts for auto_expand_replicas max setting so we don't compute
     * a node count that would be reduced by the max setting to a value awareness can't handle.
     */
    public static int computeMaxAchievableCopies(
        Map<String, Integer> nodeCountPerAttributeValue,
        int numberOfAttributeValues,
        int upperBound
    ) {
        for (int s = upperBound; s >= 1; s--) {
            int maxPerValue = (s + numberOfAttributeValues - 1) / numberOfAttributeValues;
            int capacity = 0;
            for (int nodeCount : nodeCountPerAttributeValue.values()) {
                capacity += Math.min(nodeCount, maxPerValue);
            }
            if (capacity >= s) {
                return s;
            }
        }
        return 1;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, true);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return underCapacity(shardRouting, node, allocation, false);
    }

    private Decision underCapacity(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation, boolean moveToNode) {
        if (awarenessAttributes.isEmpty()) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "allocation awareness is not enabled, set cluster setting [%s] to enable it",
                CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey()
            );
        }

        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
        if (INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(indexMetadata.getSettings()).autoExpandToAll()) {
            return allocation.decision(Decision.YES, NAME, "allocation awareness is ignored, this index is set to auto-expand to all");
        }
        int shardCount = shardRouting.isSearchOnly()
            ? indexMetadata.getNumberOfSearchOnlyReplicas()
            : indexMetadata.getNumberOfReplicas() + 1; // 1 for primary

        for (String awarenessAttribute : awarenessAttributes) {
            // the node the shard exists on must be associated with an awareness attribute.
            if (isAwarenessAttributeAssociatedWithNode(node, awarenessAttribute) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not contain the awareness attribute [%s]; required attributes cluster setting [%s=%s]",
                    awarenessAttribute,
                    CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(),
                    allocation.debugDecision() ? Strings.collectionToCommaDelimitedString(awarenessAttributes) : null
                );
            }

            int currentNodeCount = getCurrentNodeCountForAttribute(shardRouting, node, allocation, moveToNode, awarenessAttribute);
            Set<String> attributeValues = getAttributeValues(shardRouting, allocation, awarenessAttribute);
            int numberOfAttributes = attributeValues.size();

            List<String> fullValues = forcedAwarenessAttributes.get(awarenessAttribute);
            if (fullValues != null) {
                // If forced awareness is enabled, numberOfAttributes = count(distinct((union(discovered_attributes, forced_attributes)))
                Set<String> attributesSet = new HashSet<>(fullValues);
                attributesSet.addAll(attributeValues);
                numberOfAttributes = attributesSet.size();
            }

            // TODO should we remove ones that are not part of full list?
            final int maximumNodeCount = (shardCount + numberOfAttributes - 1) / numberOfAttributes; // ceil(shardCount/numberOfAttributes)
            if (currentNodeCount > maximumNodeCount) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "there are too many copies of the shard allocated to nodes with attribute [%s], there are [%d] total configured "
                        + "shard copies for this shard id and [%d] total attribute values, expected the allocated shard count per "
                        + "attribute [%d] to be less than or equal to the upper bound of the required number of shards per attribute [%d]",
                    awarenessAttribute,
                    shardCount,
                    numberOfAttributes,
                    currentNodeCount,
                    maximumNodeCount
                );
            }
        }

        return allocation.decision(Decision.YES, NAME, "node meets all awareness attribute requirements");
    }

    private Set<String> getAttributeValues(ShardRouting shardRouting, RoutingAllocation allocation, String awarenessAttribute) {
        return allocation.routingNodes()
            .nodesPerAttributesCounts(awarenessAttribute, routingNode -> routingNode.node().isSearchNode() == shardRouting.isSearchOnly());
    }

    private int getCurrentNodeCountForAttribute(
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        boolean moveToNode,
        String awarenessAttribute
    ) {
        // build the count of shards per attribute value
        final String shardAttributeForNode = getAttributeValueForNode(node, awarenessAttribute);
        // Get all assigned shards of the same type
        List<ShardRouting> assignedShards = getAssignedShards(allocation, shardRouting);
        int currentNodeCount = 0;
        for (ShardRouting assignedShard : assignedShards) {
            if (assignedShard.started() || assignedShard.initializing()) {
                // Note: this also counts relocation targets as that will be the new location of the shard.
                // Relocation sources should not be counted as the shard is moving away
                RoutingNode routingNode = allocation.routingNodes().node(assignedShard.currentNodeId());
                // Increase node count when
                if (getAttributeValueForNode(routingNode, awarenessAttribute).equals(shardAttributeForNode)) {
                    ++currentNodeCount;
                }
            }
        }

        if (moveToNode) {
            if (shardRouting.assignedToNode()) {
                String nodeId = shardRouting.relocating() ? shardRouting.relocatingNodeId() : shardRouting.currentNodeId();
                if (node.nodeId().equals(nodeId) == false) {
                    // we work on different nodes, move counts around
                    if (getAttributeValueForNode(allocation.routingNodes().node(nodeId), awarenessAttribute).equals(shardAttributeForNode)
                        && currentNodeCount > 0) {
                        --currentNodeCount;
                    }

                    ++currentNodeCount;
                }
            } else {
                ++currentNodeCount;
            }
        }

        return currentNodeCount;
    }

    private List<ShardRouting> getAssignedShards(RoutingAllocation allocation, ShardRouting shardRouting) {
        return allocation.routingNodes()
            .assignedShards(shardRouting.shardId())
            .stream()
            .filter(s -> s.isSearchOnly() == shardRouting.isSearchOnly())
            .collect(Collectors.toList());
    }

    private boolean isAwarenessAttributeAssociatedWithNode(RoutingNode node, String awarenessAttribute) {
        return node.node().getAttributes().containsKey(awarenessAttribute);
    }

    private String getAttributeValueForNode(final RoutingNode node, final String awarenessAttribute) {
        return node.node().getAttributes().get(awarenessAttribute);
    }
}
