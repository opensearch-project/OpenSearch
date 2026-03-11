/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.unit.TimeValue;

import java.util.Map;
import java.util.function.Function;

/**
 * Registry of valid workload group search settings with their validators
 */
public class WorkloadGroupSearchSettings {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private WorkloadGroupSearchSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Enum defining valid workload group search settings with their validation logic.
     * Settings are categorized as either query parameters or cluster settings.
     */
    public enum WlmSearchSetting {
        // Query parameters (applied to SearchRequest)
        /** Setting for search request timeout */
        TIMEOUT("timeout", WorkloadGroupSearchSettings::validateTimeValue);

        private final String settingName;
        private final Function<String, String> validator;

        WlmSearchSetting(String settingName, Function<String, String> validator) {
            this.settingName = settingName;
            this.validator = validator;
        }

        /**
         * Returns the setting name.
         * @return the setting name
         */
        public String getSettingName() {
            return settingName;
        }

        /**
         * Validates the given value for this setting.
         * @param value the value to validate
         * @throws IllegalArgumentException if the value is invalid
         */
        void validate(String value) {
            String error = validator.apply(value);
            if (error != null) {
                throw new IllegalArgumentException("Invalid value '" + value + "' for " + settingName + ": " + error);
            }
        }

        /**
         * Finds a setting by its name.
         * @param settingName the setting name
         * @return the setting or null if not found
         */
        public static WlmSearchSetting fromKey(String settingName) {
            for (WlmSearchSetting setting : values()) {
                if (setting.settingName.equals(settingName)) {
                    return setting;
                }
            }
            return null;
        }
    }

    /**
     * Validates all search settings in the provided map.
     * @param searchSettings map of setting names to values
     * @throws IllegalArgumentException if any setting is unknown or invalid
     */
    public static void validateSearchSettings(Map<String, String> searchSettings) {
        if (searchSettings == null) {
            return;
        }
        for (Map.Entry<String, String> entry : searchSettings.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("Search setting key cannot be null");
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("Search setting value cannot be null for key: " + entry.getKey());
            }
            WlmSearchSetting setting = WlmSearchSetting.fromKey(entry.getKey());
            if (setting == null) {
                throw new IllegalArgumentException("Unknown search setting: " + entry.getKey());
            }
            setting.validate(entry.getValue());
        }
    }

    /**
     * Validates a time value string.
     * @param value the string to validate
     * @return null if valid, error message if invalid
     */
    private static String validateTimeValue(String value) {
        try {
            TimeValue.parseTimeValue(value, "validation");
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }
}
