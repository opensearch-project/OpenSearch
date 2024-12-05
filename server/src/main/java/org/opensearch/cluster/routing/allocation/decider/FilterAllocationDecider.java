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
import org.opensearch.cluster.node.DiscoveryNodeFilters;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.remotestore.RemoteStoreNodeService;

import java.util.Map;

import static org.opensearch.cluster.node.DiscoveryNodeFilters.IP_VALIDATOR;
import static org.opensearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
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
public class FilterAllocationDecider extends AllocationDecider {

    public static final String NAME = "filter";

    private static final String CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX = "cluster.routing.allocation.require";
    private static final String CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX = "cluster.routing.allocation.include";
    private static final String CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX = "cluster.routing.allocation.exclude";
    public static final Setting.AffixSetting<String> CLUSTER_ROUTING_REQUIRE_GROUP_SETTING = Setting.prefixKeySetting(
        CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX + ".",
        key -> Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> CLUSTER_ROUTING_INCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX + ".",
        key -> Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX + ".",
        key -> Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.NodeScope)
    );

    private volatile DiscoveryNodeFilters clusterRequireFilters;
    private volatile DiscoveryNodeFilters clusterIncludeFilters;
    private volatile DiscoveryNodeFilters clusterExcludeFilters;
    private volatile RemoteStoreNodeService.Direction migrationDirection;
    private volatile RemoteStoreNodeService.CompatibilityMode compatibilityMode;

    public FilterAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        setClusterRequireFilters(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(settings));
        setClusterExcludeFilters(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getAsMap(settings));
        setClusterIncludeFilters(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(settings));
        this.migrationDirection = RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING.get(settings);
        this.compatibilityMode = RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING.get(settings);

        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_REQUIRE_GROUP_SETTING, this::setClusterRequireFilters, (a, b) -> {});
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING, this::setClusterExcludeFilters, (a, b) -> {});
        clusterSettings.addAffixMapUpdateConsumer(CLUSTER_ROUTING_INCLUDE_GROUP_SETTING, this::setClusterIncludeFilters, (a, b) -> {});
        clusterSettings.addSettingsUpdateConsumer(RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING, this::setMigrationDirection);
        clusterSettings.addSettingsUpdateConsumer(
            RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING,
            this::setCompatibilityMode
        );
    }

    private void setMigrationDirection(RemoteStoreNodeService.Direction migrationDirection) {
        this.migrationDirection = migrationDirection;
    }

    private void setCompatibilityMode(RemoteStoreNodeService.CompatibilityMode compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.unassigned()) {
            // only for unassigned - we filter allocation right after the index creation (for shard shrinking) to ensure
            // that once it has been allocated post API the replicas can be allocated elsewhere without user interaction
            // this is a setting that can only be set within the system!
            IndexMetadata indexMd = allocation.metadata().getIndexSafe(shardRouting.index());
            DiscoveryNodeFilters initialRecoveryFilters = DiscoveryNodeFilters.trimTier(indexMd.getInitialRecoveryFilters());
            if (initialRecoveryFilters != null
                && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS
                && initialRecoveryFilters.match(node.node()) == false) {
                String explanation =
                    "initial allocation of the shrunken index is only allowed on nodes [%s] that hold a copy of every shard in the index";
                return allocation.decision(Decision.NO, NAME, explanation, initialRecoveryFilters);
            }

            Decision decision = isRemoteStoreMigrationReplicaDecision(shardRouting, allocation);
            if (decision != null) return decision;
        }
        return shouldFilter(shardRouting, node.node(), allocation);
    }

    public Decision isRemoteStoreMigrationReplicaDecision(ShardRouting shardRouting, RoutingAllocation allocation) {
        assert shardRouting.unassigned();
        boolean primaryOnRemote = RemoteStoreMigrationAllocationDecider.isPrimaryOnRemote(shardRouting.shardId(), allocation);
        if (shardRouting.primary() == false
            && shardRouting.unassignedInfo().getReason() != UnassignedInfo.Reason.INDEX_CREATED
            && (compatibilityMode.equals(RemoteStoreNodeService.CompatibilityMode.MIXED))
            && (migrationDirection.equals(RemoteStoreNodeService.Direction.REMOTE_STORE))
            && primaryOnRemote == false) {
            String explanation =
                "in  remote store migration, allocation filters are not applicable for replica copies whose primary is on doc rep node";
            return allocation.decision(Decision.YES, NAME, explanation);
        }
        return null;
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(indexMetadata, node.node(), allocation);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return shouldFilter(shardRouting, node.node(), allocation);
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) return decision;

        decision = shouldIndexFilter(indexMetadata, node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    @Override
    public Decision canAllocateAnyShardToNode(RoutingNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node.node(), allocation);
        return decision != null && decision == Decision.NO ? decision : Decision.ALWAYS;
    }

    private Decision shouldFilter(ShardRouting shardRouting, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) return decision;

        decision = shouldIndexFilter(allocation.metadata().getIndexSafe(shardRouting.index()), node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private Decision shouldFilter(IndexMetadata indexMd, DiscoveryNode node, RoutingAllocation allocation) {
        Decision decision = shouldClusterFilter(node, allocation);
        if (decision != null) return decision;

        decision = shouldIndexFilter(indexMd, node, allocation);
        if (decision != null) return decision;

        return allocation.decision(Decision.YES, NAME, "node passes include/exclude/require filters");
    }

    private Decision shouldIndexFilter(IndexMetadata indexMd, DiscoveryNode node, RoutingAllocation allocation) {
        DiscoveryNodeFilters indexRequireFilters = DiscoveryNodeFilters.trimTier(indexMd.requireFilters());
        DiscoveryNodeFilters indexIncludeFilters = DiscoveryNodeFilters.trimTier(indexMd.includeFilters());
        DiscoveryNodeFilters indexExcludeFilters = DiscoveryNodeFilters.trimTier(indexMd.excludeFilters());

        if (indexRequireFilters != null) {
            if (indexRequireFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match index setting [%s] filters [%s]",
                    IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX,
                    indexRequireFilters
                );
            }
        }
        if (indexIncludeFilters != null) {
            if (indexIncludeFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match index setting [%s] filters [%s]",
                    IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX,
                    indexIncludeFilters
                );
            }
        }
        if (indexExcludeFilters != null) {
            if (indexExcludeFilters.match(node)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node matches index setting [%s] filters [%s]",
                    IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey(),
                    indexExcludeFilters
                );
            }
        }
        return null;
    }

    private Decision shouldClusterFilter(DiscoveryNode node, RoutingAllocation allocation) {
        if (clusterRequireFilters != null) {
            if (clusterRequireFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not match cluster setting [%s] filters [%s]",
                    CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX,
                    clusterRequireFilters
                );
            }
        }
        if (clusterIncludeFilters != null) {
            if (clusterIncludeFilters.match(node) == false) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node does not cluster setting [%s] filters [%s]",
                    CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX,
                    clusterIncludeFilters
                );
            }
        }
        if (clusterExcludeFilters != null) {
            if (clusterExcludeFilters.match(node)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "node matches cluster setting [%s] filters [%s]",
                    CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX,
                    clusterExcludeFilters
                );
            }
        }
        return null;
    }

    private void setClusterRequireFilters(Map<String, String> filters) {
        clusterRequireFilters = DiscoveryNodeFilters.trimTier(
            DiscoveryNodeFilters.buildOrUpdateFromKeyValue(clusterRequireFilters, AND, filters)
        );
    }

    private void setClusterIncludeFilters(Map<String, String> filters) {
        clusterIncludeFilters = DiscoveryNodeFilters.trimTier(
            DiscoveryNodeFilters.buildOrUpdateFromKeyValue(clusterIncludeFilters, OR, filters)
        );
    }

    private void setClusterExcludeFilters(Map<String, String> filters) {
        clusterExcludeFilters = DiscoveryNodeFilters.trimTier(
            DiscoveryNodeFilters.buildOrUpdateFromKeyValue(clusterExcludeFilters, OR, filters)
        );
    }
}
