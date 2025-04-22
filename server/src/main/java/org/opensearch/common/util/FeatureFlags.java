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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Feature flags are used to gate the visibility/availability of incomplete features. For more information, see
 * https://featureflags.io/feature-flag-introduction/
 * Due to their use case, feature flag settings have several additional properties enforced by convention and code:
 * - Feature flags are boolean settings.
 * - Feature flags are static settings.
 * - Feature flags are globally available.
 * - Feature flags are configurable by JVM system properties with setting key.
 * @opensearch.internal
 */
public class FeatureFlags {
    // Prefixes public for testing
    private static final String OS_EXPERIMENTAL_PREFIX = "opensearch.experimental.";
    static final String FEATURE_FLAG_PREFIX = OS_EXPERIMENTAL_PREFIX + "feature.";

    /**
     * Gates the visibility of the remote store to docrep migration.
     */
    public static final String REMOTE_STORE_MIGRATION_EXPERIMENTAL = FEATURE_FLAG_PREFIX + "remote_store.migration.enabled";

    /**
     * Gates the ability for Searchable Snapshots to read snapshots that are older than the
     * guaranteed backward compatibility for OpenSearch (one prior major version) on a best effort basis.
     */
    public static final String SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY = FEATURE_FLAG_PREFIX
        + "searchable_snapshot.extended_compatibility.enabled";
    public static final Setting<Boolean> SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_SETTING = Setting.boolSetting(
        SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY,
        false,
        Property.NodeScope
    );

    /**
     * Gates the functionality of extensions.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String EXTENSIONS = FEATURE_FLAG_PREFIX + "extensions.enabled";

    /**
     * Gates the functionality of telemetry framework.
     */
    public static final String TELEMETRY = FEATURE_FLAG_PREFIX + "telemetry.enabled";

    /**
     * Gates the optimization of datetime formatters caching along with change in default datetime formatter.
     */
    public static final String DATETIME_FORMATTER_CACHING = OS_EXPERIMENTAL_PREFIX + "optimization.datetime_formatter_caching.enabled";

    /**
     * Gates the functionality of warm index having the capability to store data remotely.
     * Once the feature is ready for release, this feature flag can be removed.
     */
    public static final String WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG = "opensearch.experimental.feature.writable_warm_index.enabled";

    /**
     * Gates the functionality of background task execution.
     */
    public static final String BACKGROUND_TASK_EXECUTION_EXPERIMENTAL = FEATURE_FLAG_PREFIX + "task.background.enabled";

    /**
     * Gates the functionality of merged segment warmer in local/remote segment replication.
     * Once the feature is ready for release, this feature flag can be removed.
     */
    public static final String MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG = "opensearch.experimental.feature.merged_segment_warmer.enabled";

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

    public static final Setting<Boolean> WRITABLE_WARM_INDEX_SETTING = Setting.boolSetting(
        WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG,
        false,
        Property.NodeScope
    );

    public static final Setting<Boolean> MERGED_SEGMENT_WARMER_EXPERIMENTAL_SETTING = Setting.boolSetting(
        MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG,
        false,
        Property.NodeScope
    );

    /**
     * Gates the functionality of star tree index, which improves the performance of search
     * aggregations.
     */
    public static final String STAR_TREE_INDEX = FEATURE_FLAG_PREFIX + "composite_index.star_tree.enabled";
    public static final Setting<Boolean> STAR_TREE_INDEX_SETTING = Setting.boolSetting(STAR_TREE_INDEX, false, Property.NodeScope);

    /**
     * Gates the functionality of application based configuration templates.
     */
    public static final String APPLICATION_BASED_CONFIGURATION_TEMPLATES = FEATURE_FLAG_PREFIX + "application_templates.enabled";
    public static final Setting<Boolean> APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING = Setting.boolSetting(
        APPLICATION_BASED_CONFIGURATION_TEMPLATES,
        false,
        Property.NodeScope
    );

    public static final String TERM_VERSION_PRECOMMIT_ENABLE = OS_EXPERIMENTAL_PREFIX + "optimization.termversion.precommit.enabled";
    public static final Setting<Boolean> TERM_VERSION_PRECOMMIT_ENABLE_SETTING = Setting.boolSetting(
        TERM_VERSION_PRECOMMIT_ENABLE,
        false,
        Property.NodeScope
    );

    public static final String ARROW_STREAMS = FEATURE_FLAG_PREFIX + "arrow.streams.enabled";
    public static final Setting<Boolean> ARROW_STREAMS_SETTING = Setting.boolSetting(ARROW_STREAMS, false, Property.NodeScope);

    /**
     * Underlying implementation for feature flags.
     * All settable feature flags are tracked here in FeatureFlagsImpl.featureFlags.
     * Contains all functionality across test and server use cases.
     */
    static class FeatureFlagsImpl {
        // Add an evergreen test feature flag and hide it in private scope
        private static final String TEST_FLAG = "test.flag.enabled";
        private static final Setting<Boolean> TEST_FLAG_SETTING = Setting.boolSetting(TEST_FLAG, false, Property.NodeScope);

        private final ConcurrentHashMap<Setting<Boolean>, Boolean> featureFlags = new ConcurrentHashMap<>() {
            {
                put(TEST_FLAG_SETTING, TEST_FLAG_SETTING.get(Settings.EMPTY));
                put(REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING, REMOTE_STORE_MIGRATION_EXPERIMENTAL_SETTING.getDefault(Settings.EMPTY));
                put(EXTENSIONS_SETTING, EXTENSIONS_SETTING.getDefault(Settings.EMPTY));
                put(TELEMETRY_SETTING, TELEMETRY_SETTING.getDefault(Settings.EMPTY));
                put(DATETIME_FORMATTER_CACHING_SETTING, DATETIME_FORMATTER_CACHING_SETTING.getDefault(Settings.EMPTY));
                put(WRITABLE_WARM_INDEX_SETTING, WRITABLE_WARM_INDEX_SETTING.getDefault(Settings.EMPTY));
                put(STAR_TREE_INDEX_SETTING, STAR_TREE_INDEX_SETTING.getDefault(Settings.EMPTY));
                put(
                    APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING,
                    APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING.getDefault(Settings.EMPTY)
                );
                put(TERM_VERSION_PRECOMMIT_ENABLE_SETTING, TERM_VERSION_PRECOMMIT_ENABLE_SETTING.getDefault(Settings.EMPTY));
                put(ARROW_STREAMS_SETTING, ARROW_STREAMS_SETTING.getDefault(Settings.EMPTY));
                put(
                    SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_SETTING,
                    SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_SETTING.getDefault(Settings.EMPTY)
                );
                put(MERGED_SEGMENT_WARMER_EXPERIMENTAL_SETTING, MERGED_SEGMENT_WARMER_EXPERIMENTAL_SETTING.getDefault(Settings.EMPTY));
            }
        };

        /**
         * Initialize feature flags map from the following sources:
         * (Each source overwrites previous feature flags)
         * - Set from setting default
         * - Set from JVM system property if flag exists
         */
        FeatureFlagsImpl() {
            initFromDefaults();
            initFromSysProperties();
        }

        /**
         * Initialize feature flags map from the following sources:
         * (Each source overwrites previous feature flags)
         * - Set from setting default
         * - Set from JVM system property if flag exists
         * - Set from provided settings if flag exists
         * @param openSearchSettings The settings stored in opensearch.yml.
         */
        void initializeFeatureFlags(Settings openSearchSettings) {
            initFromDefaults();
            initFromSysProperties();
            initFromSettings(openSearchSettings);
        }

        /**
         * Set all feature flags according to setting defaults.
         * Overwrites existing entries in feature flags map.
         * Skips flags which are write locked according to TestUtils.FlagLock.
         */
        private void initFromDefaults() {
            for (Setting<Boolean> ff : featureFlags.keySet()) {
                if (TestUtils.FlagWriteLock.isLocked(ff.getKey())) continue;
                featureFlags.put(ff, ff.getDefault(Settings.EMPTY));
            }
        }

        /**
         * Update feature flags according to JVM system properties.
         * Feature flags are true if system property is set as "true" (case-insensitive). Else feature set to false.
         * Overwrites existing value if system property exists.
         * Skips flags which are write locked according to TestUtils.FlagLock.
         */
        private void initFromSysProperties() {
            for (Setting<Boolean> ff : featureFlags.keySet()) {
                if (TestUtils.FlagWriteLock.isLocked(ff.getKey())) continue;
                String prop = System.getProperty(ff.getKey());
                if (prop != null) {
                    featureFlags.put(ff, Boolean.valueOf(prop));
                }
            }
        }

        /**
         * Update feature flags in ALL_FEATURE_FLAG_SETTINGS according to provided settings.
         * Overwrites existing entries in feature flags map.
         * Skips flags which are write locked according to TestUtils.FlagLock.
         * @param settings settings to update feature flags from
         */
        private void initFromSettings(Settings settings) {
            for (Setting<Boolean> ff : featureFlags.keySet()) {
                if (settings.hasValue(ff.getKey())) {
                    if (TestUtils.FlagWriteLock.isLocked(ff.getKey())) continue;
                    featureFlags.put(ff, settings.getAsBoolean(ff.getKey(), ff.getDefault(settings)));
                }
            }
        }

        /**
         * @param ff feature flag setting
         * @return true if feature enabled - else false
         */
        boolean isEnabled(Setting<Boolean> ff) {
            return featureFlags.getOrDefault(ff, false);
        }

        /**
         * @param featureFlagName feature flag setting key
         * @return true if feature enabled - else false
         */
        boolean isEnabled(String featureFlagName) {
            for (Setting<Boolean> ff : featureFlags.keySet()) {
                if (ff.getKey().equals(featureFlagName)) return featureFlags.get(ff);
            }
            return false;
        }

        /**
         * @param featureFlagName feature flag to set
         * @param value value for flag
         */
        void set(String featureFlagName, Boolean value) {
            for (Setting<Boolean> ff : featureFlags.keySet()) {
                if (ff.getKey().equals(featureFlagName)) featureFlags.put(ff, value);
            }
        }
    }

    private static final FeatureFlagsImpl featureFlagsImpl = new FeatureFlagsImpl();

    /**
     * Server module public API.
     */
    public static void initializeFeatureFlags(Settings openSearchSettings) {
        featureFlagsImpl.initializeFeatureFlags(openSearchSettings);
    }

    public static boolean isEnabled(String featureFlagName) {
        return featureFlagsImpl.isEnabled(featureFlagName);
    }

    public static boolean isEnabled(Setting<Boolean> featureFlag) {
        return featureFlagsImpl.isEnabled(featureFlag);
    }

    /**
     * Provides feature flag write access for test use cases.
     * To enable a feature flag for a single test case see @LockFeatureFlag annotation.
     * For more fine grain control us TestUtils.with() or explicitly construct a new FlagLock().
     * Note: JUnit will not run test cases concurrently within a suite by default.
     * Similarly test suites are forked and run in a separate JVM environment.
     * As such these utility methods do not provide any thread safety.
     */
    public static class TestUtils {
        /**
         * AutoCloseable helper which sets a feature flag and makes it immutable for the lifetime of the lock.
         * Throws an exception if two locks exist for the same flag as we should never reach this state.
         * Initializing two write locks for the same flag throws a RuntimeException.
         */
        public static class FlagWriteLock implements AutoCloseable {
            private static final Set<String> writeLocks = new HashSet<>();
            private final String flag;
            private final Boolean prev;

            public static boolean isLocked(String flag) {
                return writeLocks.contains(flag);
            }

            public FlagWriteLock(String flag) {
                this(flag, true);
            }

            public FlagWriteLock(String flag, Boolean value) {
                if (writeLocks.contains(flag)) {
                    throw new RuntimeException("Cannot initialize second write lock for feature flag: " + flag);
                }
                this.flag = flag;
                this.prev = featureFlagsImpl.isEnabled(flag);
                writeLocks.add(flag);
                featureFlagsImpl.set(flag, value);
            }

            public void unlock() {
                featureFlagsImpl.set(flag, prev);
                writeLocks.remove(flag);
            }

            @Override
            public void close() {
                featureFlagsImpl.set(flag, prev);
                writeLocks.remove(flag);
            }
        }

        /**
         * For critical sections run as lambdas which may throw exceptions.
         */
        @FunctionalInterface
        public interface ThrowingRunnable {
            void run() throws Exception;
        }

        /**
         * Executes runnable test action with the provided feature flag enabled.
         * Returns feature flag to previous value.
         * @param flag feature flag setting.
         * @param action critical section to run while feature flag is set.
         */
        public static void with(String flag, ThrowingRunnable action) throws Exception {
            try (FlagWriteLock ignored = new FlagWriteLock(flag)) {
                action.run();
            }
        }

        public static void with(String flag, Boolean value, ThrowingRunnable action) throws Exception {
            try (FlagWriteLock ignored = new FlagWriteLock(flag, value)) {
                action.run();
            }
        }
    }
}
