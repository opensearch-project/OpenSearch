/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.annotation.PublicApi;

import java.util.Locale;

/**
 * ShardMovementStrategy defines the order in which shard movement occurs.
 * <p>
 * ShardMovementStrategy values or rather their string representation to be used with
 * {@link BalancedShardsAllocator#SHARD_MOVEMENT_STRATEGY_SETTING} via cluster settings.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.9.0")
public enum ShardMovementStrategy {
    /**
     * default behavior in which order of shard movement doesn't matter.
     */
    NO_PREFERENCE,

    /**
     * primary shards are moved first
     */
    PRIMARY_FIRST,

    /**
     * replica shards are moved first
     */
    REPLICA_FIRST;

    public static ShardMovementStrategy parse(String strValue) {
        if (strValue == null) {
            return null;
        } else {
            strValue = strValue.toUpperCase(Locale.ROOT);
            try {
                return ShardMovementStrategy.valueOf(strValue);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Illegal allocation.shard_movement_strategy value [" + strValue + "]");
            }
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

}
