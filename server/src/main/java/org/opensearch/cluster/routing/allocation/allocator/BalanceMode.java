/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import java.util.Locale;

/**
 * Selects the scale on which the shard, per-index, and disk-usage balance terms are expressed
 * in {@link BalancedShardsAllocator.WeightFunction}. String values are used as-is for the
 * {@link BalancedShardsAllocator#BALANCE_MODE_SETTING} cluster setting.
 *
 * @opensearch.internal
 */
public enum BalanceMode {
    /**
     * Raw shard-count deltas for the shard and per-index terms; ratio for the disk term.
     * Preserves the historical behavior and {@link BalancedShardsAllocator#THRESHOLD_SETTING}
     * semantics ({@code threshold} = "at least N shards of imbalance"). This is the default.
     */
    COUNT,

    /**
     * All three terms are expressed as relative deviations from the per-axis cluster average.
     * The shard, per-index, and disk-usage balance factors then operate on the same
     * dimensionless scale. {@code threshold} is interpreted as a relative deviation
     * (e.g. {@code 0.1} = "at least 10% imbalance"); operators enabling ratio mode typically
     * lower {@code threshold} from its default of {@code 1.0}.
     */
    RATIO;

    public static BalanceMode parse(String strValue) {
        if (strValue == null) {
            return null;
        }
        try {
            return BalanceMode.valueOf(strValue.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Illegal value ["
                    + strValue
                    + "] for ["
                    + BalancedShardsAllocator.BALANCE_MODE_SETTING.getKey()
                    + "]; accepted values are [count, ratio]"
            );
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
