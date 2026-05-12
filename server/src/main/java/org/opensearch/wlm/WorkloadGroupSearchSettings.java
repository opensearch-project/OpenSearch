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
     * All registered WLM settings, keyed by their canonical key name.
     */
    private static final Map<String, Setting<?>> REGISTERED_SETTINGS = Map.of("search.default_search_timeout", WLM_SEARCH_TIMEOUT);

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
            try {
                Settings testSettings = Settings.builder().put(key, value).build();
                setting.get(testSettings);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid value '" + value + "' for " + key + ": " + e.getMessage());
            }
        }
    }

    /**
     * Returns an unmodifiable view of the registered settings.
     *
     * @return map of canonical key names to their {@link Setting} objects
     */
    public static Map<String, Setting<?>> getRegisteredSettings() {
        return REGISTERED_SETTINGS;
    }
}
