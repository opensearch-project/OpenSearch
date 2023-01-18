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

/**
 * Utility class to manage feature flags. Feature flags are system properties that must be set on the JVM.
 * These are used to gate the visibility/availability of incomplete features. Fore more information, see
 * https://featureflags.io/feature-flag-introduction/
 *
 * @opensearch.internal
 */
public class FeatureFlags {

    /**
     * Gates the visibility of the index setting that allows changing of replication type.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String REPLICATION_TYPE = "opensearch.experimental.feature.replication_type.enabled";

    /**
     * Gates the visibility of the index setting that allows persisting data to remote store along with local disk.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String REMOTE_STORE = "opensearch.experimental.feature.remote_store.enabled";

    /**
     * Gates the functionality of a new parameter to the snapshot restore API
     * that allows for creation of a new index type that searches a snapshot
     * directly in a remote repository without restoring all index data to disk
     * ahead of time.
     */
    public static final String SEARCHABLE_SNAPSHOT = "opensearch.experimental.feature.searchable_snapshot.enabled";

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
     * Should store the settings from opensearch.yml.
     */
    private static Settings settings;

    /**
     * This method is responsible to map settings from opensearch.yml to local stored
     * settings value. That is used for the existing isEnabled method.
     *
     * @param openSearchSettings The settings stored in opensearch.yml.
     */
    public static void initializeFeatureFlags(Settings openSearchSettings) {
        settings = openSearchSettings;
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

    public static final Setting<Boolean> REPLICATION_TYPE_SETTING = Setting.boolSetting(REPLICATION_TYPE, false, Property.NodeScope);

    public static final Setting<Boolean> REMOTE_STORE_SETTING = Setting.boolSetting(REMOTE_STORE, false, Property.NodeScope);

    public static final Setting<Boolean> SEARCHABLE_SNAPSHOT_SETTING = Setting.boolSetting(SEARCHABLE_SNAPSHOT, false, Property.NodeScope);

    public static final Setting<Boolean> EXTENSIONS_SETTING = Setting.boolSetting(EXTENSIONS, false, Property.NodeScope);
}
