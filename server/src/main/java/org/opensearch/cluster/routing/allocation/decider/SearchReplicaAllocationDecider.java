/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeFilters;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.remotestore.RemoteStoreNodeService;

import java.util.Map;

import static org.opensearch.cluster.node.DiscoveryNodeFilters.IP_VALIDATOR;
import static org.opensearch.cluster.node.DiscoveryNodeFilters.OpType.OR;

/**
 * This allocation decider is similar to FilterAllocationDecider but provides
 * the option to filter specifically for search replicas.
 * The filter behaves similar to an include for any defined node attribute.
 * A search replica can be allocated to only nodes with one of the specified attributes while
 * other shard types will be rejected from nodes with any othe attributes.
 * @opensearch.internal
 */
public class SearchReplicaAllocationDecider extends AllocationDecider {

    public static final String NAME = "filter";
    private static final String SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_PREFIX = "cluster.routing.allocation.search.replica.dedicated.include";
    public static final Setting.AffixSetting<String> SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_PREFIX + ".",
        key -> Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.NodeScope)
    );

    private volatile DiscoveryNodeFilters searchReplicaIncludeFilters;

    private volatile RemoteStoreNodeService.Direction migrationDirection;
    private volatile RemoteStoreNodeService.CompatibilityMode compatibilityMode;

    public SearchReplicaAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        setSearchReplicaIncludeFilters(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(settings));
        clusterSettings.addAffixMapUpdateConsumer(
            SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING,
            this::setSearchReplicaIncludeFilters,
            (a, b) -> {}
        );
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node.node(), allocation);
    }

    private Decision shouldFilter(ShardRouting shardRouting, DiscoveryNode node, RoutingAllocation allocation) {
        if (searchReplicaIncludeFilters != null) {
            final boolean match = searchReplicaIncludeFilters.match(node);
            if (match == false && shardRouting.isSearchOnly()) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match shard setting [%s] filters [%s]",
                    SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_PREFIX,
                    searchReplicaIncludeFilters
                );
            }
            // filter will only apply to search replicas
            if (shardRouting.isSearchOnly() == false && match) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "only search replicas can be allocated to node with setting [%s] filters [%s]",
                    SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_PREFIX,
                    searchReplicaIncludeFilters
                );
            }
        }
        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private void setSearchReplicaIncludeFilters(Map<String, String> filters) {
        searchReplicaIncludeFilters = DiscoveryNodeFilters.trimTier(
            DiscoveryNodeFilters.buildOrUpdateFromKeyValue(searchReplicaIncludeFilters, OR, filters)
        );
    }
}
