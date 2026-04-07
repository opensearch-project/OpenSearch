/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;

import java.util.Locale;
import java.util.Set;

/**
 * Utility class for handling tiering operations in OpenSearch Warm.
 * Utility methods (resolveRequestIndex, isMigrationAllowed, getTieringSourceType, isShardStateValidForTier,
 * getTieringStatefromIndexSettings, getTierPairForTargetTier, getTieringStatefromTargetTier, isTerminalTier,
 * getTieringStartTime, isCCRFollowerIndex) will be added in the implementation PR.
 */
public class TieringUtils {

    /** Private constructor to prevent instantiation of utility class. */
    private TieringUtils() {}

    private static final Logger logger = LogManager.getLogger(TieringUtils.class);

    /** Custom metadata key for hot-to-warm tiering start time. */
    public static final String H2W_TIERING_START_TIME_KEY = "h2w_start_time";
    /** Custom metadata key for warm-to-hot tiering start time. */
    public static final String W2H_TIERING_START_TIME_KEY = "w2h_start_time";
    /** Custom metadata key for tiering information. */
    public static final String TIERING_CUSTOM_KEY = "tiering";
    /** Tiering type identifier for hot-to-warm tiering. */
    public static final String H2W_TIERING_TYPE_KEY = "hot to warm tiering";
    /** Tiering type identifier for warm-to-hot tiering. */
    public static final String W2H_TIERING_TYPE_KEY = "warm to hot tiering";

    static final int DEFAULT_H2W_MAX_CONCURRENT_TIERING_REQUESTS = 200;
    /** Setting key for maximum concurrent hot-to-warm tiering requests. */
    public static final String H2W_MAX_CONCURRENT_TIEIRNG_REQUESTS_KEY = "cluster.tiering.max_concurrent_hot_to_warm_requests";
    /** Setting for maximum concurrent hot-to-warm tiering requests. */
    public static final Setting<Integer> H2W_MAX_CONCURRENT_TIEIRNG_REQUESTS = Setting.intSetting(
        H2W_MAX_CONCURRENT_TIEIRNG_REQUESTS_KEY,
        DEFAULT_H2W_MAX_CONCURRENT_TIERING_REQUESTS,
        0,
        1000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final int DEFAULT_W2H_MAX_CONCURRENT_TIERING_REQUESTS = 10;
    /** Setting key for maximum concurrent warm-to-hot tiering requests. */
    public static final String W2H_MAX_CONCURRENT_TIEIRNG_REQUESTS_KEY = "cluster.tiering.max_concurrent_warm_to_hot_requests";
    /** Setting for maximum concurrent warm-to-hot tiering requests. */
    public static final Setting<Integer> W2H_MAX_CONCURRENT_TIEIRNG_REQUESTS = Setting.intSetting(
        W2H_MAX_CONCURRENT_TIEIRNG_REQUESTS_KEY,
        DEFAULT_W2H_MAX_CONCURRENT_TIERING_REQUESTS,
        0,
        1000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final int DEFAULT_FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT = 90;
    /** Setting key for file cache active usage tiering threshold percentage. */
    public static final String FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_KEY = "cluster.tiering.filecache_active_threshold_percent";
    /** Setting for file cache active usage tiering threshold percentage. */
    public static final Setting<Integer> FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT = Setting.intSetting(
        FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_KEY,
        DEFAULT_FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT,
        0,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final int DEFAULT_JVM_USAGE_TIERING_THRESHOLD_PERCENT = 95;
    /** Setting key for JVM usage tiering threshold percentage. */
    public static final String JVM_USAGE_TIERING_THRESHOLD_PERCENT_KEY = "cluster.tiering.jvm_usage_threshold_percent";
    /** Setting for JVM usage tiering threshold percentage. */
    public static final Setting<Integer> JVM_USAGE_TIERING_THRESHOLD_PERCENT = Setting.intSetting(
        JVM_USAGE_TIERING_THRESHOLD_PERCENT_KEY,
        DEFAULT_JVM_USAGE_TIERING_THRESHOLD_PERCENT,
        0,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Set<String> ALLOWLISTED_INDEX_PREFIXES = Set.of(".ds-");
    private static final Set<String> BLOCKLISTED_INDEX_PREFIXES = Set.of(".");
    private static final String CCR_LEADER_INDEX_SETTING_KEY = "index.plugins.replication.follower.leader_index";

    /** Represents the storage tiers available in tiered storage. */
    public enum Tier {
        /** Hot tier for frequently accessed data. */
        HOT,
        /** Warm tier for infrequently accessed data. */
        WARM;

        /**
         * Converts a string to a Tier enum value.
         * @param name the string name
         */
        public static Tier fromString(final String name) {
            if (name == null) {
                throw new IllegalArgumentException("Tiering type cannot be null");
            }
            String upperCase = name.trim().toUpperCase(Locale.ROOT);
            switch (upperCase) {
                case "HOT":
                    return HOT;
                case "WARM":
                    return WARM;
                default:
                    throw new IllegalArgumentException(
                        "Tiering type [" + name + "] is not supported. Supported types are " + HOT + " and " + WARM
                    );
            }
        }

        /** Returns the lowercase string value of this tier. */
        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
