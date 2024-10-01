/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;

import java.util.List;

/**
 * Utility class to manage feature flags. Feature flags are system properties that must be set on the JVM.
 * These are used to gate the visibility/availability of incomplete features. For more information, see
 * https://featureflags.io/feature-flag-introduction/
 *
 * @opensearch.internal
 */
public class FeatureFlags {
    /**
     * Gates the visibility of the remote store to docrep migration.
     */
    public static final String REMOTE_STORE_MIGRATION_EXPERIMENTAL = "opensearch.experimental.feature.remote_store.migration.enabled";

    /**
     * Gates the ability for Searchable Snapshots to read snapshots that are older than the
     * guaranteed backward compatibility for OpenSearch (one prior major version) on a best effort basis.
     */
    public static final String SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY =
        "opensearch.experimental.feature.searchable_snapshot.extended_compatibility.enabled";

    /**
     * Gates the functionality of extensions.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String EXTENSIONS = "opensearch.experimental.feature.extensions.enabled";

    /**
     * Gates the functionality of telemetry framework.
     */
    public static final String TELEMETRY = "opensearch.experimental.feature.telemetry.enabled";

    /**
     * Gates the optimization of datetime formatters caching along with change in default datetime formatter.
     */
    public static final String DATETIME_FORMATTER_CACHING = "opensearch.experimental.optimization.datetime_formatter_caching.enabled";

    /**
     * Gates the functionality of remote index having the capability to move across different tiers
     * Once the feature is ready for release, this feature flag can be removed.
     */
    public static final String TIERED_REMOTE_INDEX = "opensearch.experimental.feature.tiered_remote_index.enabled";

    /**
     * Gates the functionality of pluggable cache.
     * Enables OpenSearch to use pluggable caches with respective store names via setting.
     */
    public static final String PLUGGABLE_CACHE = "opensearch.experimental.feature.pluggable.caching.enabled";

    public static final String READER_WRITER_SPLIT_EXPERIMENTAL = "opensearch.experimental.feature.read.write.split.enabled";

    /**
     * Gates the functionality of background task execution.
     */
    public static final String BACKGROUND_TASK_EXECUTION_EXPERIMENTAL = "opensearch.experimental.feature.task.background.enabled";

    public static final Setting<Boolean> REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING = Setting.boolSetting(
        REMOTE_STORE_MIGRATION_EXPERIMENTAL,
        false,
        Property.NodeScope
    );

    public static final Setting<Boolean> EXTENSIONS_SETTING = Setting.boolSetting(EXTENSIONS, false, Property.NodeScope);

    public static final Setting<Boolean> TELEMETRY_SETTING = Setting.boolSetting(TELEMETRY, false, Property.NodeScope);

    public static final Setting<Boolean> DATETIME_FORMATTER_CACHING_SETTING = Setting.boolSetting(
        DATETIME_FORMATTER_CACHING,
        false,
        Property.NodeScope
    );

    public static final Setting<Boolean> TIERED_REMOTE_INDEX_SETTING = Setting.boolSetting(TIERED_REMOTE_INDEX, false, Property.NodeScope);

    public static final Setting<Boolean> PLUGGABLE_CACHE_SETTING = Setting.boolSetting(PLUGGABLE_CACHE, false, Property.NodeScope);

    public static final Setting<Boolean> READER_WRITER_SPLIT_EXPERIMENTAL_SETTING = Setting.boolSetting(
        READER_WRITER_SPLIT_EXPERIMENTAL,
        false,
        Property.NodeScope
    );

    /**
     * Gates the functionality of application based configuration templates.
     */
    public static final String APPLICATION_BASED_CONFIGURATION_TEMPLATES = "opensearch.experimental.feature.application_templates.enabled";
    public static final Setting<Boolean> APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING = Setting.boolSetting(
        APPLICATION_BASED_CONFIGURATION_TEMPLATES,
        false,
        Property.NodeScope
    );

    /**
     * Gates the functionality of star tree index, which improves the performance of search
     * aggregations.
     */
    public static final String STAR_TREE_INDEX = "opensearch.experimental.feature.composite_index.star_tree.enabled";
    public static final Setting<Boolean> STAR_TREE_INDEX_SETTING = Setting.boolSetting(STAR_TREE_INDEX, false, Property.NodeScope);

    /**
     * Gates the functionality of ApproximatePointRangeQuery where we approximate query results.
     */
    public static final String APPROXIMATE_POINT_RANGE_QUERY = "opensearch.experimental.feature.approximate_point_range_query.enabled";
    public static final Setting<Boolean> APPROXIMATE_POINT_RANGE_QUERY_SETTING = Setting.boolSetting(
        APPROXIMATE_POINT_RANGE_QUERY,
        false,
        Property.NodeScope
    );
    public static final String TERM_VERSION_PRECOMMIT_ENABLE = "opensearch.experimental.optimization.termversion.precommit.enabled";
    public static final Setting<Boolean> TERM_VERSION_PRECOMMIT_ENABLE_SETTING = Setting.boolSetting(
        TERM_VERSION_PRECOMMIT_ENABLE,
        false,
        Property.NodeScope
    );

    private static final List<Setting<Boolean>> ALL_FEATURE_FLAG_SETTINGS = List.of(
        REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING,
        EXTENSIONS_SETTING,
        TELEMETRY_SETTING,
        DATETIME_FORMATTER_CACHING_SETTING,
        TIERED_REMOTE_INDEX_SETTING,
        PLUGGABLE_CACHE_SETTING,
        APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING,
        STAR_TREE_INDEX_SETTING,
        READER_WRITER_SPLIT_EXPERIMENTAL_SETTING,
        TERM_VERSION_PRECOMMIT_ENABLE_SETTING
    );

    /**
     * Should store the settings from opensearch.yml.
     */
    private static Settings settings;

    static {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Setting<Boolean> ffSetting : ALL_FEATURE_FLAG_SETTINGS) {
            settingsBuilder = settingsBuilder.put(ffSetting.getKey(), ffSetting.getDefault(Settings.EMPTY));
        }
        settings = settingsBuilder.build();
    }

    /**
     * This method is responsible to map settings from opensearch.yml to local stored
     * settings value. That is used for the existing isEnabled method.
     *
     * @param openSearchSettings The settings stored in opensearch.yml.
     */
    public static void initializeFeatureFlags(Settings openSearchSettings) {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Setting<Boolean> ffSetting : ALL_FEATURE_FLAG_SETTINGS) {
            settingsBuilder = settingsBuilder.put(
                ffSetting.getKey(),
                openSearchSettings.getAsBoolean(ffSetting.getKey(), ffSetting.getDefault(openSearchSettings))
            );
        }
        settings = settingsBuilder.build();
    }

    /**
     * Used to test feature flags whose values are expected to be booleans.
     * This method returns true if the value is "true" (case-insensitive),
     * and false otherwise.
     */
    public static boolean isEnabled(String featureFlagName) {
        if ("true".equalsIgnoreCase(System.getProperty(featureFlagName))) {
            // TODO: Remove the if condition once FeatureFlags are only supported via opensearch.yml
            return true;
        }
        return settings != null && settings.getAsBoolean(featureFlagName, false);
    }

    public static boolean isEnabled(Setting<Boolean> featureFlag) {
        if ("true".equalsIgnoreCase(System.getProperty(featureFlag.getKey()))) {
            // TODO: Remove the if condition once FeatureFlags are only supported via opensearch.yml
            return true;
        } else if (settings != null) {
            return featureFlag.get(settings);
        } else {
            return featureFlag.getDefault(Settings.EMPTY);
        }
    }
}
