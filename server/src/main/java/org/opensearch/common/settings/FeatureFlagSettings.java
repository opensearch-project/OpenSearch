/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.FeatureFlags;

import java.util.Set;

/**
 * Encapsulates all valid feature flag level settings.
 *
 * @opensearch.internal
 */
public class FeatureFlagSettings extends AbstractScopedSettings {

    protected FeatureFlagSettings(
        Settings settings,
        Set<Setting<?>> settingsSet,
        Set<SettingUpgrader<?>> settingUpgraders,
        Property scope
    ) {
        super(settings, settingsSet, settingUpgraders, scope);
    }

    public static final Set<Setting<?>> BUILT_IN_FEATURE_FLAGS = Set.of(
        FeatureFlags.EXTENSIONS_SETTING,
        FeatureFlags.TELEMETRY_SETTING,
        FeatureFlags.DATETIME_FORMATTER_CACHING_SETTING,
        FeatureFlags.WRITABLE_WARM_INDEX_SETTING,
        FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING,
        FeatureFlags.STAR_TREE_INDEX_SETTING,
        FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING,
        FeatureFlags.TERM_VERSION_PRECOMMIT_ENABLE_SETTING,
        FeatureFlags.ARROW_STREAMS_SETTING,
        FeatureFlags.MERGED_SEGMENT_WARMER_EXPERIMENTAL_SETTING
    );
}
