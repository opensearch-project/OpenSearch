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

import static java.util.Collections.emptyList;

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
        int shardCount = indexMetadata.getNumberOfReplicas() + 1; // 1 for primary
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

            // build attr_value -> nodes map
            Set<String> nodesPerAttribute = allocation.routingNodes().nodesPerAttributesCounts(awarenessAttribute);
            int numberOfAttributes = nodesPerAttribute.size();
            List<String> fullValues = forcedAwarenessAttributes.get(awarenessAttribute);

            if (fullValues != null) {
                // If forced awareness is enabled, numberOfAttributes = count(distinct((union(discovered_attributes, forced_attributes)))
                Set<String> attributesSet = new HashSet<>(fullValues);
                for (String stringObjectCursor : nodesPerAttribute) {
                    attributesSet.add(stringObjectCursor);
                }
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

    private int getCurrentNodeCountForAttribute(
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        boolean moveToNode,
        String awarenessAttribute
    ) {
        // build the count of shards per attribute value
        final String shardAttributeForNode = getAttributeValueForNode(node, awarenessAttribute);
        int currentNodeCount = 0;
        final List<ShardRouting> assignedShards = allocation.routingNodes().assignedShards(shardRouting.shardId());

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

    private boolean isAwarenessAttributeAssociatedWithNode(RoutingNode node, String awarenessAttribute) {
        return node.node().getAttributes().containsKey(awarenessAttribute);
    }

    private String getAttributeValueForNode(final RoutingNode node, final String awarenessAttribute) {
        return node.node().getAttributes().get(awarenessAttribute);
    }

}
