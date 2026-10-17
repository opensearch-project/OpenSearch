/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.ClusterSettingsValidatorRegistry;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.script.ScriptMetadata;
import org.opensearch.script.ScriptService;
import org.opensearch.script.StoredScriptSource;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.opensearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class SettingsUpdaterTests extends OpenSearchTestCase {

    public void testUpdateSetting() {
        AtomicReference<Float> index = new AtomicReference<>();
        AtomicReference<Float> shard = new AtomicReference<>();
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("foo"));
        ClusterSettings settingsService = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, index::set);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, shard::set);
        SettingsUpdater updater = createSettingsUpdater(settingsService);
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5)
                    .build()
            )
            .transientSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5)
                    .build()
            );
        ClusterState build = builder.metadata(metadata).build();
        ClusterState clusterState = updater.updateSettings(
            build,
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5).build(),
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.4).build(),
            logger
        );
        assertNotSame(clusterState, build);
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 0.4, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 2.5, 0.1);
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().transientSettings()), 0.5, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metadata().transientSettings()), 4.5, 0.1);

        clusterState = updater.updateSettings(
            clusterState,
            Settings.builder().putNull("cluster.routing.*").build(),
            Settings.EMPTY,
            logger
        );
        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 0.4, 0.1);
        assertEquals(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 2.5, 0.1);
        assertFalse(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));

        clusterState = updater.updateSettings(
            clusterState,
            Settings.EMPTY,
            Settings.builder()
                .putNull("cluster.routing.*")
                .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 10.0)
                .build(),
            logger
        );

        assertEquals(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.get(clusterState.metadata().persistentSettings()), 10.0, 0.1);
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().persistentSettings()));
        assertFalse(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));
        assertFalse(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.exists(clusterState.metadata().transientSettings()));
        assertNull("updater only does a dryRun", index.get());
        assertNull("updater only does a dryRun", shard.get());
    }

    public void testAllOrNothing() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("foo"));
        ClusterSettings settingsService = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AtomicReference<Float> index = new AtomicReference<>();
        AtomicReference<Float> shard = new AtomicReference<>();
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, index::set);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, shard::set);
        SettingsUpdater updater = createSettingsUpdater(settingsService);
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5)
                    .build()
            )
            .transientSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5)
                    .build()
            );
        ClusterState build = builder.metadata(metadata).build();

        try {
            updater.updateSettings(
                build,
                Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "not a float").build(),
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "not a float")
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                    .build(),
                logger
            );
            fail("all or nothing");
        } catch (IllegalArgumentException ex) {
            logger.info("", ex);
            assertEquals("Failed to parse value [not a float] for setting [cluster.routing.allocation.balance.index]", ex.getMessage());
        }
        assertNull("updater only does a dryRun", index.get());
        assertNull("updater only does a dryRun", shard.get());
    }

    public void testClusterBlock() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("foo"));
        ClusterSettings settingsService = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AtomicReference<Float> index = new AtomicReference<>();
        AtomicReference<Float> shard = new AtomicReference<>();
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING, index::set);
        settingsService.addSettingsUpdateConsumer(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING, shard::set);
        SettingsUpdater updater = createSettingsUpdater(settingsService);
        Metadata.Builder metadata = Metadata.builder()
            .persistentSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 2.5)
                    .build()
            )
            .transientSettings(
                Settings.builder()
                    .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 3.5)
                    .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 4.5)
                    .build()
            );
        ClusterState build = builder.metadata(metadata).build();

        ClusterState clusterState = updater.updateSettings(
            build,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build(),
            Settings.builder()
                .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.6)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                .build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 1);
        assertEquals(clusterState.blocks().global().iterator().next(), Metadata.CLUSTER_READ_ONLY_BLOCK);

        clusterState = updater.updateSettings(
            build,
            Settings.EMPTY,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), false).build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 0);

        clusterState = updater.updateSettings(
            build,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build(),
            Settings.builder()
                .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 1.6)
                .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                .build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 1);
        assertEquals(clusterState.blocks().global().iterator().next(), Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
        clusterState = updater.updateSettings(
            build,
            Settings.EMPTY,
            Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false).build(),
            logger
        );
        assertEquals(clusterState.blocks().global().size(), 0);

    }

    public void testDeprecationLogging() {
        Setting<String> deprecatedSetting = Setting.simpleString(
            "deprecated.setting",
            Property.Dynamic,
            Property.NodeScope,
            Property.Deprecated
        );
        final Settings settings = Settings.builder().put("deprecated.setting", "foo").build();
        final Set<Setting<?>> settingsSet = Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(deprecatedSetting))
            .collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        clusterSettings.addSettingsUpdateConsumer(deprecatedSetting, s -> {});
        final SettingsUpdater settingsUpdater = createSettingsUpdater(clusterSettings);
        final ClusterState clusterState = ClusterState.builder(new ClusterName("foo"))
            .metadata(Metadata.builder().persistentSettings(settings).build())
            .build();

        final Settings toApplyDebug = Settings.builder().put("logger.org.opensearch", "debug").build();
        final ClusterState afterDebug = settingsUpdater.updateSettings(clusterState, toApplyDebug, Settings.EMPTY, logger);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { deprecatedSetting });

        final Settings toApplyUnset = Settings.builder().putNull("logger.org.opensearch").build();
        final ClusterState afterUnset = settingsUpdater.updateSettings(afterDebug, toApplyUnset, Settings.EMPTY, logger);
        assertNoDeprecationWarnings();

        // we also check that if no settings are changed, deprecation logging still occurs
        settingsUpdater.updateSettings(afterUnset, toApplyUnset, Settings.EMPTY, logger);
        assertNoDeprecationWarnings();
    }

    public void testUpdateWithUnknownAndSettings() {
        // we will randomly apply some new dynamic persistent and transient settings
        final int numberOfDynamicSettings = randomIntBetween(1, 8);
        final List<Setting<String>> dynamicSettings = new ArrayList<>(numberOfDynamicSettings);
        for (int i = 0; i < numberOfDynamicSettings; i++) {
            final Setting<String> dynamicSetting = Setting.simpleString("dynamic.setting" + i, Property.Dynamic, Property.NodeScope);
            dynamicSettings.add(dynamicSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingPersistentSettings = Settings.builder();
        final Settings.Builder existingTransientSettings = Settings.builder();

        for (final Setting<String> dynamicSetting : dynamicSettings) {
            switch (randomIntBetween(0, 2)) {
                case 0:
                    existingPersistentSettings.put(dynamicSetting.getKey(), "existing_value");
                    break;
                case 1:
                    existingTransientSettings.put(dynamicSetting.getKey(), "existing_value");
                    break;
                case 2:
                    break;
            }
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(invalidSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(invalidSetting.getKey(), "value");
            }
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(unknownSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(unknownSetting.getKey(), "value");
            }
        }

        // register all the known settings (note that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.concat(dynamicSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, knownSettings);
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            clusterSettings.addSettingsUpdateConsumer(dynamicSetting, s -> {});
        }
        final SettingsUpdater settingsUpdater = createSettingsUpdater(clusterSettings);
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .persistentSettings(existingPersistentSettings.build())
            .transientSettings(existingTransientSettings.build());
        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).metadata(metadataBuilder).build();

        // prepare the dynamic settings update
        final Settings.Builder persistentToApply = Settings.builder();
        final Settings.Builder transientToApply = Settings.builder();
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            switch (randomIntBetween(0, 2)) {
                case 0:
                    persistentToApply.put(dynamicSetting.getKey(), "new_value");
                    break;
                case 1:
                    transientToApply.put(dynamicSetting.getKey(), "new_value");
                    break;
                case 2:
                    break;
            }
        }

        if (transientToApply.keys().isEmpty() && persistentToApply.keys().isEmpty()) {
            // force a settings update otherwise our assertions below will fail
            if (randomBoolean()) {
                persistentToApply.put(dynamicSettings.get(0).getKey(), "new_value");
            } else {
                transientToApply.put(dynamicSettings.get(0).getKey(), "new_value");
            }
        }

        final ClusterState clusterStateAfterUpdate = settingsUpdater.updateSettings(
            clusterState,
            transientToApply.build(),
            persistentToApply.build(),
            logger
        );

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            if (existingPersistentSettings.keys().contains(invalidSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(invalidSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            if (existingPersistentSettings.keys().contains(unknownSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(unknownSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(unknownSetting.getKey())));
        }

        // the dynamic settings should be applied
        for (final Setting<String> dynamicSetting : dynamicSettings) {
            if (persistentToApply.keys().contains(dynamicSetting.getKey())) {
                assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), hasItem(dynamicSetting.getKey()));
                assertThat(clusterStateAfterUpdate.metadata().persistentSettings().get(dynamicSetting.getKey()), equalTo("new_value"));
            } else if (transientToApply.keys().contains(dynamicSetting.getKey())) {
                assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), hasItem(dynamicSetting.getKey()));
                assertThat(clusterStateAfterUpdate.metadata().transientSettings().get(dynamicSetting.getKey()), equalTo("new_value"));
            } else {
                if (existingPersistentSettings.keys().contains(dynamicSetting.getKey())) {
                    assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), hasItem(dynamicSetting.getKey()));
                    assertThat(
                        clusterStateAfterUpdate.metadata().persistentSettings().get(dynamicSetting.getKey()),
                        equalTo("existing_value")
                    );
                } else if (existingTransientSettings.keys().contains(dynamicSetting.getKey())) {
                    assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), hasItem(dynamicSetting.getKey()));
                    assertThat(
                        clusterStateAfterUpdate.metadata().transientSettings().get(dynamicSetting.getKey()),
                        equalTo("existing_value")
                    );
                } else {
                    assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(dynamicSetting.getKey())));
                    assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(dynamicSetting.getKey())));
                }
            }
        }
    }

    public void testRemovingArchivedSettingsDoesNotRemoveNonArchivedInvalidOrUnknownSettings() {
        // these are settings that are archived in the cluster state as either persistent or transient settings
        final int numberOfArchivedSettings = randomIntBetween(1, 8);
        final List<Setting<String>> archivedSettings = new ArrayList<>(numberOfArchivedSettings);
        for (int i = 0; i < numberOfArchivedSettings; i++) {
            final Setting<String> archivedSetting = Setting.simpleString("setting", Property.NodeScope);
            archivedSettings.add(archivedSetting);
        }

        // these are invalid settings that exist as either persistent or transient settings
        final int numberOfInvalidSettings = randomIntBetween(0, 7);
        final List<Setting<String>> invalidSettings = invalidSettings(numberOfInvalidSettings);

        // these are unknown settings that exist as either persistent or transient settings
        final int numberOfUnknownSettings = randomIntBetween(0, 7);
        final List<Setting<String>> unknownSettings = unknownSettings(numberOfUnknownSettings);

        final Settings.Builder existingPersistentSettings = Settings.builder();
        final Settings.Builder existingTransientSettings = Settings.builder();

        for (final Setting<String> archivedSetting : archivedSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey(), "value");
            }
        }

        for (final Setting<String> invalidSetting : invalidSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(invalidSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(invalidSetting.getKey(), "value");
            }
        }

        for (final Setting<String> unknownSetting : unknownSettings) {
            if (randomBoolean()) {
                existingPersistentSettings.put(unknownSetting.getKey(), "value");
            } else {
                existingTransientSettings.put(unknownSetting.getKey(), "value");
            }
        }

        // register all the known settings (not that we do not register the unknown settings)
        final Set<Setting<?>> knownSettings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.concat(archivedSettings.stream(), invalidSettings.stream())
        ).collect(Collectors.toSet());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, knownSettings);
        final SettingsUpdater settingsUpdater = createSettingsUpdater(clusterSettings);
        final Metadata.Builder metadataBuilder = Metadata.builder()
            .persistentSettings(existingPersistentSettings.build())
            .transientSettings(existingTransientSettings.build());
        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).metadata(metadataBuilder).build();

        final Settings.Builder persistentToApply = Settings.builder().put("archived.*", (String) null);
        final Settings.Builder transientToApply = Settings.builder().put("archived.*", (String) null);

        final ClusterState clusterStateAfterUpdate = settingsUpdater.updateSettings(
            clusterState,
            transientToApply.build(),
            persistentToApply.build(),
            logger
        );

        // existing archived settings are removed
        for (final Setting<String> archivedSetting : archivedSettings) {
            if (existingPersistentSettings.keys().contains(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    not(hasItem(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey()))
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    not(hasItem(ARCHIVED_SETTINGS_PREFIX + archivedSetting.getKey()))
                );
            }
        }

        // the invalid settings should be archived and not present in non-archived form
        for (final Setting<String> invalidSetting : invalidSettings) {
            if (existingPersistentSettings.keys().contains(invalidSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + invalidSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(invalidSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(invalidSetting.getKey())));
        }

        // the unknown settings should be archived and not present in non-archived form
        for (final Setting<String> unknownSetting : unknownSettings) {
            if (existingPersistentSettings.keys().contains(unknownSetting.getKey())) {
                assertThat(
                    clusterStateAfterUpdate.metadata().persistentSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            } else {
                assertThat(
                    clusterStateAfterUpdate.metadata().transientSettings().keySet(),
                    hasItem(ARCHIVED_SETTINGS_PREFIX + unknownSetting.getKey())
                );
            }
            assertThat(clusterStateAfterUpdate.metadata().persistentSettings().keySet(), not(hasItem(unknownSetting.getKey())));
            assertThat(clusterStateAfterUpdate.metadata().transientSettings().keySet(), not(hasItem(unknownSetting.getKey())));
        }
    }

    private static List<Setting<String>> unknownSettings(int numberOfUnknownSettings) {
        final List<Setting<String>> unknownSettings = new ArrayList<>(numberOfUnknownSettings);
        for (int i = 0; i < numberOfUnknownSettings; i++) {
            unknownSettings.add(Setting.simpleString("unknown.setting" + i, Property.NodeScope));
        }
        return unknownSettings;
    }

    private static List<Setting<String>> invalidSettings(int numberOfInvalidSettings) {
        final List<Setting<String>> invalidSettings = new ArrayList<>(numberOfInvalidSettings);
        for (int i = 0; i < numberOfInvalidSettings; i++) {
            invalidSettings.add(randomBoolean() ? invalidInIsolationSetting(i) : invalidWithDependenciesSetting(i));
        }
        return invalidSettings;
    }

    private static Setting<String> invalidInIsolationSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<String>() {

            @Override
            public void validate(final String value) {
                throw new IllegalArgumentException("Invalid in isolation setting");
            }

            @Override
            public void validate(final String value, final Map<Setting<?>, Object> settings) {

            }

        }, Property.NodeScope);
    }

    private static Setting<String> invalidWithDependenciesSetting(int index) {
        return Setting.simpleString("invalid.setting" + index, new Setting.Validator<String>() {

            @Override
            public void validate(final String value) {}

            @Override
            public void validate(final String value, final Map<Setting<?>, Object> settings) {
                throw new IllegalArgumentException("Invalid with dependencies setting");
            }

        }, Property.NodeScope);
    }

    private static class FooLowSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {}

        @Override
        public void validate(final Integer low, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_HIGH) && low > (int) settings.get(SETTING_FOO_HIGH)) {
                throw new IllegalArgumentException("[low]=" + low + " is higher than [high]=" + settings.get(SETTING_FOO_HIGH));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Collections.singletonList(SETTING_FOO_HIGH);
            return settings.iterator();
        }

    }

    private static class FooHighSettingValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(final Integer value) {

        }

        @Override
        public void validate(final Integer high, final Map<Setting<?>, Object> settings) {
            if (settings.containsKey(SETTING_FOO_LOW) && high < (int) settings.get(SETTING_FOO_LOW)) {
                throw new IllegalArgumentException("[high]=" + high + " is lower than [low]=" + settings.get(SETTING_FOO_LOW));
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Collections.singletonList(SETTING_FOO_LOW);
            return settings.iterator();
        }

    }

    private static final Setting<Integer> SETTING_FOO_LOW = new Setting<>(
        "foo.low",
        "10",
        Integer::valueOf,
        new FooLowSettingValidator(),
        Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Setting<Integer> SETTING_FOO_HIGH = new Setting<>(
        "foo.high",
        "100",
        Integer::valueOf,
        new FooHighSettingValidator(),
        Property.Dynamic,
        Setting.Property.NodeScope
    );

    public void testUpdateOfValidationDependentSettings() {
        final ClusterSettings settings = new ClusterSettings(Settings.EMPTY, new HashSet<>(asList(SETTING_FOO_LOW, SETTING_FOO_HIGH)));
        final SettingsUpdater updater = createSettingsUpdater(settings);
        final Metadata.Builder metadata = Metadata.builder().persistentSettings(Settings.EMPTY).transientSettings(Settings.EMPTY);

        ClusterState cluster = ClusterState.builder(new ClusterName("cluster")).metadata(metadata).build();

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_LOW.getKey(), 20).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("20"));

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_HIGH.getKey(), 40).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("20"));
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_LOW.getKey(), 5).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_HIGH.getKey()), equalTo("40"));

        cluster = updater.updateSettings(cluster, Settings.builder().put(SETTING_FOO_HIGH.getKey(), 8).build(), Settings.EMPTY, logger);
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_LOW.getKey()), equalTo("5"));
        assertThat(cluster.getMetadata().settings().get(SETTING_FOO_HIGH.getKey()), equalTo("8"));

        final ClusterState finalCluster = cluster;
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(finalCluster, Settings.builder().put(SETTING_FOO_HIGH.getKey(), 2).build(), Settings.EMPTY, logger)
        );

        assertThat(exception.getMessage(), equalTo("[high]=2 is lower than [low]=5"));
    }

    public void testRejectSensitiveSettingInTransient() {
        Setting<String> sensitiveSetting = Setting.simpleString(
            "sensitive.setting",
            Property.Dynamic,
            Property.NodeScope,
            Property.Sensitive
        );
        final Set<Setting<?>> settingsSet = Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(sensitiveSetting))
            .collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        SettingsUpdater updater = createSettingsUpdater(clusterSettings);
        ClusterState state = ClusterState.builder(new ClusterName("foo")).metadata(Metadata.builder().build()).build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(state, Settings.builder().put("sensitive.setting", "value").build(), Settings.EMPTY, logger)
        );
        assertThat(ex.getMessage(), equalTo("sensitive setting [sensitive.setting] must be updated using persistent settings"));
    }

    public void testRejectPersistentScriptLimitBelowStoredScriptSize() {
        assertScriptLimitRejected(Settings.EMPTY, scriptLimit(2));
    }

    public void testRejectTransientScriptLimitBelowStoredScriptSize() {
        assertScriptLimitRejected(scriptLimit(2), Settings.EMPTY);
    }

    private void assertScriptLimitRejected(Settings transientUpdates, Settings persistentUpdates) {
        ClusterState state = scriptState("abc", scriptLimit(4), Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(state, transientUpdates, persistentUpdates, logger)
        );
        assertEquals(
            "script.max_size_in_bytes cannot be set to [2], stored script [test] exceeds the new value with a size of [3]",
            e.getMessage()
        );
        assertEquals(4, (int) ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(state.metadata().persistentSettings()));
    }

    public void testScriptLimitUsesUtf8Size() {
        ClusterState state = scriptState("é", scriptLimit(2), Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Collections.emptyMap());
        expectThrows(IllegalArgumentException.class, () -> updater.updateSettings(state, Settings.EMPTY, scriptLimit(1), logger));
        assertSame(state, updater.updateSettings(state, Settings.EMPTY, scriptLimit(2), logger));
    }

    public void testScriptLimitUsesPostUpdateTransientOverride() {
        ClusterState state = scriptState("abc", scriptLimit(4), Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(scriptLimit(1), Collections.emptyMap());
        ClusterState updatedState = updater.updateSettings(state, scriptLimit(3), scriptLimit(2), logger);
        assertEquals(3, (int) ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(updatedState.metadata().settings()));
    }

    public void testResetTransientScriptLimitValidatesPersistentValue() {
        ClusterState state = scriptState("abc", scriptLimit(2), scriptLimit(4));
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Collections.emptyMap());
        expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(
                state,
                Settings.builder().putNull(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey()).build(),
                Settings.EMPTY,
                logger
            )
        );
    }

    public void testResetPersistentScriptLimitPreservesTransientOverride() {
        ClusterState state = scriptState("abc", scriptLimit(4), scriptLimit(3));
        SettingsUpdater updater = createSettingsUpdater(scriptLimit(1), Collections.emptyMap());
        ClusterState updatedState = updater.updateSettings(
            state,
            Settings.EMPTY,
            Settings.builder().putNull(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey()).build(),
            logger
        );
        assertEquals(3, (int) ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(updatedState.metadata().settings()));
    }

    public void testResetScriptLimitValidatesNodeValue() {
        ClusterState state = scriptState("abc", scriptLimit(4), Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(scriptLimit(2), Collections.emptyMap());
        expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(
                state,
                Settings.EMPTY,
                Settings.builder().putNull(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey()).build(),
                logger
            )
        );
    }

    public void testResetMissingScriptLimitStillValidates() {
        ClusterState state = scriptState("abc", Settings.EMPTY, Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(scriptLimit(2), Collections.emptyMap());
        expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(
                state,
                Settings.EMPTY,
                Settings.builder().putNull(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey()).build(),
                logger
            )
        );
    }

    public void testResetScriptLimitRejectsDefaultBelowStoredScriptSize() {
        assertScriptLimitResetRejected(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey());
    }

    public void testWildcardScriptLimitResetRejectsDefaultBelowStoredScriptSize() {
        assertScriptLimitResetRejected("script.*");
    }

    private void assertScriptLimitResetRejected(String resetKey) {
        int scriptSize = ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(Settings.EMPTY) + 1;
        ClusterState state = scriptState("a".repeat(scriptSize), scriptLimit(scriptSize), Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Collections.emptyMap());
        expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(state, Settings.EMPTY, Settings.builder().putNull(resetKey).build(), logger)
        );
    }

    public void testResetScriptLimitResolvesNodeValueAboveDefault() {
        int scriptSize = ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(Settings.EMPTY) + 1;
        ClusterState state = scriptState("a".repeat(scriptSize), scriptLimit(scriptSize + 1), Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(scriptLimit(scriptSize), Collections.emptyMap());
        ClusterState updatedState = updater.updateSettings(
            state,
            Settings.EMPTY,
            Settings.builder().putNull(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey()).build(),
            logger
        );
        assertFalse(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.exists(updatedState.metadata().settings()));
    }

    public void testUnrelatedUpdateDoesNotValidateStoredScriptSize() {
        ClusterState state = scriptState("abc", scriptLimit(2), Settings.EMPTY);
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Collections.emptyMap());
        ClusterState updatedState = updater.updateSettings(
            state,
            Settings.EMPTY,
            Settings.builder().put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5).build(),
            logger
        );
        assertEquals(2, (int) ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(updatedState.metadata().settings()));
    }

    public void testPluginValidatorRejectsProposedStateBeforeReturningIt() {
        Setting<Boolean> allowScripts = Setting.boolSetting("plugin.allow_scripts", true, Property.Dynamic, Property.NodeScope);
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Map.of(allowScripts, (effectiveSettings, proposedState) -> {
            assertFalse(allowScripts.get(effectiveSettings));
            assertFalse(allowScripts.get(proposedState.metadata().settings()));
            ScriptMetadata scripts = proposedState.metadata().custom(ScriptMetadata.TYPE);
            if (scripts != null && allowScripts.get(effectiveSettings) == false) {
                throw new IllegalArgumentException("stored scripts are present");
            }
        }));
        ClusterState state = scriptState("abc", Settings.builder().put(allowScripts.getKey(), true).build(), Settings.EMPTY);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(state, Settings.EMPTY, Settings.builder().put(allowScripts.getKey(), false).build(), logger)
        );
        assertEquals("stored scripts are present", e.getMessage());
        assertTrue(allowScripts.get(state.metadata().settings()));
    }

    public void testValidatorRunsOnceWhenBothScopesTouchSameSetting() {
        Setting<Integer> setting = Setting.intSetting("plugin.limit", 2, Property.Dynamic, Property.NodeScope);
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Map.of(setting, (effectiveSettings, proposedState) -> {
            assertEquals(3, (int) setting.get(effectiveSettings));
            validations.incrementAndGet();
        }));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        updater.updateSettings(
            state,
            Settings.builder().put(setting.getKey(), 3).build(),
            Settings.builder().put(setting.getKey(), 1).build(),
            logger
        );
        assertEquals(1, validations.get());
    }

    public void testUnchangedRequestedSettingStillValidated() {
        Setting<Integer> setting = Setting.intSetting("plugin.limit", 2, Property.Dynamic, Property.NodeScope);
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createSettingsUpdater(
            Settings.EMPTY,
            Map.of(setting, (effectiveSettings, proposedState) -> validations.incrementAndGet())
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().persistentSettings(Settings.builder().put(setting.getKey(), 2).build()))
            .build();
        assertSame(state, updater.updateSettings(state, Settings.EMPTY, Settings.builder().put(setting.getKey(), 2).build(), logger));
        assertEquals(1, validations.get());
    }

    public void testWildcardResetValidatesAffixSetting() {
        Setting.AffixSetting<Integer> setting = Setting.affixKeySetting(
            "plugin.",
            "limit",
            key -> Setting.intSetting(key, 2, Property.Dynamic, Property.NodeScope)
        );
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createSettingsUpdater(Settings.EMPTY, Map.of(setting, (effectiveSettings, proposedState) -> {
            assertEquals(2, (int) setting.getConcreteSetting("plugin.group.limit").get(effectiveSettings));
            validations.incrementAndGet();
        }));
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().persistentSettings(Settings.builder().put("plugin.group.limit", 3).build()))
            .build();
        ClusterState updatedState = updater.updateSettings(
            state,
            Settings.EMPTY,
            Settings.builder().putNull("plugin.group.*").build(),
            logger
        );
        assertFalse(updatedState.metadata().persistentSettings().hasValue("plugin.group.limit"));
        assertEquals(1, validations.get());
    }

    private static SettingsUpdater createSettingsUpdater(ClusterSettings clusterSettings) {
        return new SettingsUpdater(clusterSettings, new ClusterSettingsValidatorRegistry(Settings.EMPTY, Collections.emptyMap()));
    }

    private static SettingsUpdater createSettingsUpdater(
        Settings nodeSettings,
        Map<Setting<?>, BiConsumer<Settings, ClusterState>> validators
    ) {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.addAll(validators.keySet());
        return new SettingsUpdater(
            new ClusterSettings(nodeSettings, settings),
            new ClusterSettingsValidatorRegistry(nodeSettings, validators)
        );
    }

    private static Settings scriptLimit(int value) {
        return Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), value).build();
    }

    private static ClusterState scriptState(String source, Settings persistentSettings, Settings transientSettings) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .persistentSettings(persistentSettings)
                    .transientSettings(transientSettings)
                    .putCustom(
                        ScriptMetadata.TYPE,
                        new ScriptMetadata.Builder(null).storeScript("test", new StoredScriptSource("test", source, Collections.emptyMap()))
                            .build()
                    )
            )
            .build();
    }

}
