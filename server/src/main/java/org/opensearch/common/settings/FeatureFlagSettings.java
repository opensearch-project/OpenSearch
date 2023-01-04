/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.FeatureFlags;

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

    public static final Set<Setting<?>> BUILT_IN_FEATURE_FLAGS = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                FeatureFlags.REPLICATION_TYPE_SETTING,
                FeatureFlags.REMOTE_STORE_SETTING,
                FeatureFlags.SEARCHABLE_SNAPSHOT_SETTING,
                FeatureFlags.EXTENSIONS_SETTING
            )
        )
    );
}
