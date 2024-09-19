/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.action.cat.RestSegmentsAction;
import org.opensearch.rest.action.cat.RestShardsAction;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Class to define dynamic settings for putting circuit breakers on the actions and functions to evaluate if block is required.
 */
public class RequestLimitSettings {

    /**
     * Enum to represent action names against whom we need to perform limit checks.
     */
    public enum BlockAction {
        CAT_INDICES,
        CAT_SHARDS,
        CAT_SEGMENTS
    }

    private volatile int catIndicesLimit;
    private volatile int catShardsLimit;
    private volatile int catSegmentsLimit;

    /**
     * Setting to enable circuit breaker on {@link RestIndicesAction}. The limit will be applied on number of indices.
     */
    public static final Setting<Integer> CAT_INDICES_LIMIT_SETTING = Setting.intSetting(
        "cat.indices.limit",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to enable circuit breaker on {@link RestShardsAction}. The limit will be applied on number of shards.
     */
    public static final Setting<Integer> CAT_SHARDS_LIMIT_SETTING = Setting.intSetting(
        "cat.shards.limit",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to enable circuit breaker on {@link RestSegmentsAction}. The limit will be applied on number of indices.
     */
    public static final Setting<Integer> CAT_SEGMENTS_LIMIT_SETTING = Setting.intSetting(
        "cat.segments.limit",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public RequestLimitSettings(ClusterSettings clusterSettings, Settings settings) {
        setCatShardsLimitSetting(CAT_SHARDS_LIMIT_SETTING.get(settings));
        setCatIndicesLimitSetting(CAT_INDICES_LIMIT_SETTING.get(settings));
        setCatSegmentsLimitSetting(CAT_SEGMENTS_LIMIT_SETTING.get(settings));

        clusterSettings.addSettingsUpdateConsumer(CAT_SHARDS_LIMIT_SETTING, this::setCatShardsLimitSetting);
        clusterSettings.addSettingsUpdateConsumer(CAT_INDICES_LIMIT_SETTING, this::setCatIndicesLimitSetting);
        clusterSettings.addSettingsUpdateConsumer(CAT_SEGMENTS_LIMIT_SETTING, this::setCatSegmentsLimitSetting);
    }

    /**
     * Method to check if the circuit breaker limit has reached for an action.
     * The limits are controlled via dynamic settings.
     *
     * @param clusterState  {@link ClusterState}
     * @param actionToCheck {@link BlockAction}
     * @return True/False
     */
    public boolean isCircuitLimitBreached(final ClusterState clusterState, final BlockAction actionToCheck) {
        if (Objects.isNull(clusterState)) return false;
        switch (actionToCheck) {
            case CAT_INDICES:
                if (catIndicesLimit <= 0) return false;
                int indicesCount = chainWalk(() -> clusterState.getMetadata().getIndices().size(), 0);
                if (indicesCount > catIndicesLimit) return true;
                break;
            case CAT_SHARDS:
                if (catShardsLimit <= 0) return false;
                final RoutingTable routingTable = clusterState.getRoutingTable();
                final Map<String, IndexRoutingTable> indexRoutingTableMap = routingTable.getIndicesRouting();
                int totalShards = 0;
                for (final Map.Entry<String, IndexRoutingTable> entry : indexRoutingTableMap.entrySet()) {
                    for (final Map.Entry<Integer, IndexShardRoutingTable> indexShardRoutingTableEntry : entry.getValue()
                        .getShards()
                        .entrySet()) {
                        totalShards += indexShardRoutingTableEntry.getValue().getShards().size();
                        // Fail fast if catShardsLimit value is breached and avoid unnecessary computation.
                        if (totalShards > catShardsLimit) return true;
                    }
                }
                break;
            case CAT_SEGMENTS:
                if (catSegmentsLimit <= 0) return false;
                int indicesCountForCatSegment = chainWalk(() -> clusterState.getRoutingTable().getIndicesRouting().size(), 0);
                if (indicesCountForCatSegment > catSegmentsLimit) return true;
                break;
        }
        return false;
    }

    private void setCatShardsLimitSetting(final int catShardsLimit) {
        this.catShardsLimit = catShardsLimit;
    }

    private void setCatIndicesLimitSetting(final int catIndicesLimit) {
        this.catIndicesLimit = catIndicesLimit;
    }

    private void setCatSegmentsLimitSetting(final int catSegmentsLimit) {
        this.catSegmentsLimit = catSegmentsLimit;
    }

    // TODO: Evaluate if we can move this to common util.
    private static <T> T chainWalk(Supplier<T> supplier, T defaultValue) {
        try {
            return supplier.get();
        } catch (NullPointerException e) {
            return defaultValue;
        }
    }
}
