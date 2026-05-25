/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.Map;

/**
 * Registry of valid workload group settings with their validators.
 * <p>
 * Each WLM setting is defined as a {@link Setting} object with proper type validation,
 * default values, and documentation.
 */
@ExperimentalApi
public class WorkloadGroupSearchSettings {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private WorkloadGroupSearchSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * The WLM search timeout setting. Uses the same key as the cluster-level setting
     * {@code search.default_search_timeout}. A value of -1 (MINUS_ONE) means no timeout.
     */
    public static final Setting<TimeValue> WLM_SEARCH_TIMEOUT = Setting.timeSetting("search.default_search_timeout", TimeValue.MINUS_ONE);

    /**
     * The WLM cancel after time interval setting. Specifies the time after which a search
     * request should be cancelled if it has not completed.
     */
    public static final Setting<TimeValue> WLM_CANCEL_AFTER_TIME_INTERVAL = Setting.timeSetting(
        "search.cancel_after_time_interval",
        TimeValue.MINUS_ONE
    );

    /**
     * The WLM max concurrent shard requests setting. Controls the number of shard requests
     * that should be executed concurrently on a single node. Must be >= 1.
     */
    public static final Setting<Integer> WLM_MAX_CONCURRENT_SHARD_REQUESTS = Setting.intSetting(
        "search.max_concurrent_shard_requests",
        5,
        1
    );

    /**
     * The WLM batched reduce size setting. Controls the number of shard results to reduce
     * at once on the coordinating node. Must be >= 2.
     */
    public static final Setting<Integer> WLM_BATCHED_REDUCE_SIZE = Setting.intSetting("search.batched_reduce_size", 512, 2);

    /**
     * Controls whether WLM search settings should override values explicitly set in the
     * search request query parameters. When {@code false} (default), WLM settings are only
     * applied when the request does not have an explicit value. When {@code true}, WLM
     * settings always take precedence over request-level values.
     */
    public static final Setting<Boolean> WLM_OVERRIDE_REQUEST_VALUES = Setting.boolSetting("override_request_values", false);

    /**
     * All registered WLM settings, keyed by their canonical key name.
     */
    private static final Map<String, Setting<?>> REGISTERED_SETTINGS = Map.of(
        "search.default_search_timeout",
        WLM_SEARCH_TIMEOUT,
        "search.cancel_after_time_interval",
        WLM_CANCEL_AFTER_TIME_INTERVAL,
        "search.max_concurrent_shard_requests",
        WLM_MAX_CONCURRENT_SHARD_REQUESTS,
        "search.batched_reduce_size",
        WLM_BATCHED_REDUCE_SIZE,
        "override_request_values",
        WLM_OVERRIDE_REQUEST_VALUES
    );

    /**
     * Validates a {@link Settings} object against registered WLM settings.
     * All keys in the settings must be registered, and all values must pass type validation.
     *
     * @param settings the settings to validate
     * @throws IllegalArgumentException if any key is unknown or any value is invalid
     */
    public static void validate(Settings settings) {
        if (settings == null) {
            return;
        }
        for (String key : settings.keySet()) {
            String value = settings.get(key);
            Setting<?> setting = REGISTERED_SETTINGS.get(key);
            if (setting == null) {
                throw new IllegalArgumentException("Unknown WLM setting: " + key);
            }
            // null value means "clear this setting" — skip type validation
            if (value == null) {
                continue;
            }
            try {
                Settings testSettings = Settings.builder().put(key, value).build();
                setting.get(testSettings);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid value '" + value + "' for " + key + ": " + e.getMessage());
            }
        }
    }
}
