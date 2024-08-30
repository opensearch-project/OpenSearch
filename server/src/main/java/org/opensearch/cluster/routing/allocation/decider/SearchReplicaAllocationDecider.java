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
 * This {@link AllocationDecider} control shard allocation by include and
 * exclude filters via dynamic cluster and index routing settings.
 * <p>
 * This filter is used to make explicit decision on which nodes certain shard
 * can / should be allocated. The decision if a shard can be allocated, must not
 * be allocated or should be allocated is based on either cluster wide dynamic
 * settings ({@code cluster.routing.allocation.*}) or index specific dynamic
 * settings ({@code index.routing.allocation.*}). All of those settings can be
 * changed at runtime via the cluster or the index update settings API.
 * </p>
 * Note: Cluster settings are applied first and will override index specific
 * settings such that if a shard can be allocated according to the index routing
 * settings it wont be allocated on a node if the cluster specific settings
 * would disallow the allocation. Filters are applied in the following order:
 * <ol>
 * <li>{@code required} - filters required allocations.
 * If any {@code required} filters are set the allocation is denied if the index is <b>not</b> in the set of {@code required} to allocate
 * on the filtered node</li>
 * <li>{@code include} - filters "allowed" allocations.
 * If any {@code include} filters are set the allocation is denied if the index is <b>not</b> in the set of {@code include} filters for
 * the filtered node</li>
 * <li>{@code exclude} - filters "prohibited" allocations.
 * If any {@code exclude} filters are set the allocation is denied if the index is in the set of {@code exclude} filters for the
 * filtered node</li>
 * </ol>
 *
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
        Decision decision = shouldSearchReplicaShardTypeFilter(shardRouting, node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private Decision shouldSearchReplicaShardTypeFilter(ShardRouting routing, DiscoveryNode node, RoutingAllocation allocation) {
        if (searchReplicaIncludeFilters != null) {
            final boolean match = searchReplicaIncludeFilters.match(node);
            if (match == false && routing.isSearchOnly()) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match shard setting [%s] filters [%s]",
                    SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_PREFIX,
                    searchReplicaIncludeFilters
                );
            }
            // filter will only apply to search replicas
            if (routing.isSearchOnly() == false && match) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "only search replicas can be allocated to node with setting [%s] filters [%s]",
                    SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_PREFIX,
                    searchReplicaIncludeFilters
                );
            }
        }
        return null;
    }

    private void setSearchReplicaIncludeFilters(Map<String, String> filters) {
        searchReplicaIncludeFilters = DiscoveryNodeFilters.trimTier(
            DiscoveryNodeFilters.buildOrUpdateFromKeyValue(searchReplicaIncludeFilters, OR, filters)
        );
    }
}
