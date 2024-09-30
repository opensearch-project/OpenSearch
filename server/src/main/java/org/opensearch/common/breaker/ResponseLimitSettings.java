/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.breaker;

import org.opensearch.cluster.metadata.Metadata;
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
import java.util.function.Function;

/**
 * Class to define dynamic settings for putting response limits on the actions and methods to evaluate if block is required.
 */
public class ResponseLimitSettings {

    /**
     * Enum to represent entity against which we need to perform limit checks.
     */
    public enum LimitEntity {
        INDICES,
        SHARDS
    }

    private volatile int catIndicesResponseLimit;
    private volatile int catShardsResponseLimit;
    private volatile int catSegmentsResponseLimit;

    /**
     * Setting to enable response limit on {@link RestIndicesAction}. The limit will be applied on number of indices.
     */
    public static final Setting<Integer> CAT_INDICES_RESPONSE_LIMIT_SETTING = Setting.intSetting(
        "cat.indices.response.limit.number_of_indices",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to enable response limit on {@link RestShardsAction}. The limit will be applied on number of shards.
     */
    public static final Setting<Integer> CAT_SHARDS_RESPONSE_LIMIT_SETTING = Setting.intSetting(
        "cat.shards.response.limit.number_of_shards",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting to enable response limit on {@link RestSegmentsAction}. The limit will be applied on number of indices.
     */
    public static final Setting<Integer> CAT_SEGMENTS_RESPONSE_LIMIT_SETTING = Setting.intSetting(
        "cat.segments.response.limit.number_of_indices",
        -1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public ResponseLimitSettings(ClusterSettings clusterSettings, Settings settings) {
        setCatShardsResponseLimit(CAT_SHARDS_RESPONSE_LIMIT_SETTING.get(settings));
        setCatIndicesResponseLimit(CAT_INDICES_RESPONSE_LIMIT_SETTING.get(settings));
        setCatSegmentsResponseLimit(CAT_SEGMENTS_RESPONSE_LIMIT_SETTING.get(settings));

        clusterSettings.addSettingsUpdateConsumer(CAT_SHARDS_RESPONSE_LIMIT_SETTING, this::setCatShardsResponseLimit);
        clusterSettings.addSettingsUpdateConsumer(CAT_INDICES_RESPONSE_LIMIT_SETTING, this::setCatIndicesResponseLimit);
        clusterSettings.addSettingsUpdateConsumer(CAT_SEGMENTS_RESPONSE_LIMIT_SETTING, this::setCatSegmentsResponseLimit);
    }

    /**
     * Method to check if the response limit has reached for an action.
     * The limits are controlled via dynamic settings.
     *
     * @param metadata     {@link Metadata}
     * @param limitEntity  {@link LimitEntity}
     * @param limit        Integer limit on block entity
     * @return True/False
     */
    public static boolean isResponseLimitBreached(final Metadata metadata, final LimitEntity limitEntity, final int limit) {
        if (Objects.isNull(metadata) || limit <= 0) return false;
        if (limitEntity == LimitEntity.INDICES) {
            int indicesCount = getTotalIndicesFromMetadata.apply(metadata);
            return indicesCount > limit;
        } else {
            throw new IllegalArgumentException("Unsupported limit entity [" + limitEntity + "]");
        }
    }

    /**
     * Method to check if the response limit has reached for an action.
     * The limits are controlled via dynamic settings.
     *
     * @param routingTable {@link RoutingTable}
     * @param limitEntity  {@link LimitEntity}
     * @param limit        Integer limit on block entity
     * @return True/False
     */
    public static boolean isResponseLimitBreached(final RoutingTable routingTable, final LimitEntity limitEntity, final int limit) {
        if (Objects.isNull(routingTable) || limit <= 0) return false;
        if (Objects.isNull(limitEntity)) {
            throw new IllegalArgumentException("Limit entity cannot be null");
        }
        switch (limitEntity) {
            case INDICES:
                int indicesCount = getTotalIndicesFromRoutingTable.apply(routingTable);
                if (indicesCount > limit) return true;
                break;
            case SHARDS:
                if (isShardsLimitBreached(routingTable, limit)) return true;
                break;
            default:
                throw new IllegalArgumentException("Unsupported limit entity [" + limitEntity + "]");
        }
        return false;
    }

    private static boolean isShardsLimitBreached(final RoutingTable routingTable, final int limit) {
        final Map<String, IndexRoutingTable> indexRoutingTableMap = routingTable.getIndicesRouting();
        int totalShards = 0;
        for (final Map.Entry<String, IndexRoutingTable> entry : indexRoutingTableMap.entrySet()) {
            for (final Map.Entry<Integer, IndexShardRoutingTable> indexShardRoutingTableEntry : entry.getValue().getShards().entrySet()) {
                totalShards += indexShardRoutingTableEntry.getValue().getShards().size();
                // Fail fast if limit value is breached and avoid unnecessary computation.
                if (totalShards > limit) return true;
            }
        }
        return false;
    }

    private void setCatShardsResponseLimit(final int catShardsResponseLimit) {
        this.catShardsResponseLimit = catShardsResponseLimit;
    }

    private void setCatIndicesResponseLimit(final int catIndicesResponseLimit) {
        this.catIndicesResponseLimit = catIndicesResponseLimit;
    }

    private void setCatSegmentsResponseLimit(final int catSegmentsResponseLimit) {
        this.catSegmentsResponseLimit = catSegmentsResponseLimit;
    }

    public int getCatShardsResponseLimit() {
        return this.catShardsResponseLimit;
    }

    public int getCatIndicesResponseLimit() {
        return this.catIndicesResponseLimit;
    }

    public int getCatSegmentsResponseLimit() {
        return this.catSegmentsResponseLimit;
    }

    static Function<Metadata, Integer> getTotalIndicesFromMetadata = (metadata) -> {
        if (Objects.nonNull(metadata) && Objects.nonNull(metadata.getIndices())) {
            return metadata.getIndices().size();
        }
        return 0;
    };

    static Function<RoutingTable, Integer> getTotalIndicesFromRoutingTable = (routingTable) -> {
        if (Objects.nonNull(routingTable) && Objects.nonNull(routingTable.getIndicesRouting())) {
            return routingTable.getIndicesRouting().size();
        }
        return 0;
    };
}
