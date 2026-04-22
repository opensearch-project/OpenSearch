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
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.TieringState.HOT;
import static org.opensearch.index.IndexModule.TieringState.HOT_TO_WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM_TO_HOT;

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

    /**
     * Resolves the concrete index from a request index name.
     *
     * @param indexNameExpressionResolver resolver for index names
     * @param requestIndex                the requested index name
     * @param currentState                current cluster state
     * @return resolved Index object
     */
    public static Index resolveRequestIndex(
        IndexNameExpressionResolver indexNameExpressionResolver,
        String requestIndex,
        ClusterState currentState
    ) {
        try {
            final Index[] requestIndices = indexNameExpressionResolver.concreteIndices(
                currentState,
                IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED,
                requestIndex
            );
            if (requestIndices.length != 1) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Expected single index but got %d indices", requestIndices.length)
                );
            }
            logger.info(() -> String.format(Locale.ROOT, "Resolved index [%s] to [%s]", requestIndex, requestIndices[0].getName()));
            return requestIndices[0];
        } catch (Exception e) {
            logger.error(() -> String.format(Locale.ROOT, "Failed to resolve index: [%s]", requestIndex), e);
            throw new IllegalArgumentException("Failed to resolve index: " + requestIndex, e);
        }
    }

    /**
     * Returns the TieringState based on the index settings.
     * @param indexSettings the index settings
     * @return the TieringState
     */
    public static IndexModule.TieringState getTieringStatefromIndexSettings(Settings indexSettings) {
        final IndexModule.TieringState tieringState = IndexModule.TieringState.valueOf(
            indexSettings.get(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.name())
        );
        switch (tieringState) {
            case HOT_TO_WARM:
            case WARM:
                return HOT_TO_WARM;
            case HOT:
            case WARM_TO_HOT:
                return WARM_TO_HOT;
            default:
                throw new IllegalArgumentException("Unsupported index migration state: " + tieringState);
        }
    }

    /**
     * Returns an array of tier names representing the source and target tiers for a given tiering state.
     *
     * @param tieringState The target tiering state of the index
     * @return String array containing source and target tier names in order [source, target]
     * @throws IllegalArgumentException if the tiering state is not recognized
     */
    public static String[] getTierPairForTargetTier(IndexModule.TieringState tieringState) {
        return switch (tieringState) {
            case WARM -> new String[] { HOT.toString(), WARM.toString() };
            case HOT -> new String[] { WARM.toString(), HOT.toString() };
            default -> throw new IllegalArgumentException("Unknown state: " + tieringState);
        };
    }

    /**
     * Determines the tiering state based on the target tier.
     *
     * @param tieringState String representation of the target tier state
     * @return The corresponding TieringState enum value
     * @throws IllegalArgumentException if the tiering state is not recognized
     */
    public static IndexModule.TieringState getTieringStatefromTargetTier(String tieringState) {
        return switch (IndexModule.TieringState.valueOf(tieringState)) {
            case HOT -> WARM_TO_HOT;
            case WARM -> HOT_TO_WARM;
            default -> throw new IllegalArgumentException("Unknown state: " + tieringState);
        };
    }

    /**
     * Checks if the given tier is a terminal tier (HOT or WARM).
     *
     * @param targetTier The tier to check
     * @return true if the tier is terminal (HOT or WARM), false otherwise
     */
    public static boolean isTerminalTier(String targetTier) {
        return Arrays.asList(IndexModule.TieringState.HOT.toString(), IndexModule.TieringState.WARM.toString())
            .contains(targetTier.toUpperCase(Locale.ROOT));
    }

    /**
     * Retrieves the start time of a tiering operation for a specific index.
     * The time is stored as a custom metadata field in the cluster state.
     *
     * @param clusterState The current state of the cluster
     * @param index The index for which to retrieve the tiering start time
     * @param tieringStartTimeKey The key used to store the tiering start time in custom metadata
     * @return The timestamp when the tiering operation started (in milliseconds since epoch)
     * @throws NullPointerException if the tiering metadata or start time is not found
     * @throws NumberFormatException if the stored start time value cannot be parsed as a long
     */
    public static long getTieringStartTime(ClusterState clusterState, Index index, String tieringStartTimeKey) {
        Map<String, String> customData = clusterState.getMetadata().getIndexSafe(index).getCustomData(TIERING_CUSTOM_KEY);
        if (customData == null || customData.get(tieringStartTimeKey) == null) {
            throw new IllegalStateException("Tiering metadata not found for index [" + index.getName() + "]");
        }
        return Long.parseLong(customData.get(tieringStartTimeKey));
    }

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
