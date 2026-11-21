/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.ClusterSettingsValidatorRegistry;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.AffixSetting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SettingsUpdaterSettingTypesTests extends OpenSearchTestCase {
    private static final String INDEX = "validation-index";
    private static final String LIMIT_ERROR = "limit is lower than the existing index count";
    private static final String LIST_ERROR = "an existing index is not allowed";
    private static final Setting<Integer> BASE_LIMIT = Setting.intSetting("plugin.base_limit", 2, 0, Property.Dynamic, Property.NodeScope);
    private static final Setting<List<String>> ALLOWED_INDICES = Setting.listSetting(
        "plugin.allowed_indices",
        List.of(INDEX),
        Function.identity(),
        Property.Dynamic,
        Property.NodeScope
    );
    private static final Setting<Settings> GROUP = Setting.groupSetting("plugin.group.", Property.Dynamic, Property.NodeScope);

    public void testListValidatorRejectsRemovingExistingIndex() {
        SettingsUpdater updater = listUpdater(Settings.EMPTY, ALLOWED_INDICES);
        ClusterState state = indexState(Settings.EMPTY, Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putList(ALLOWED_INDICES.getKey(), "another-index").build(), LIST_ERROR);
    }

    public void testListResetValidatesNodeSettings() {
        SettingsUpdater updater = listUpdater(
            Settings.builder().putList(ALLOWED_INDICES.getKey(), "another-index").build(),
            ALLOWED_INDICES
        );
        ClusterState state = indexState(Settings.builder().putList(ALLOWED_INDICES.getKey(), INDEX).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull(ALLOWED_INDICES.getKey()).build(), LIST_ERROR);
    }

    public void testWildcardListResetValidatesNodeSettings() {
        SettingsUpdater updater = listUpdater(
            Settings.builder().putList(ALLOWED_INDICES.getKey(), "another-index").build(),
            ALLOWED_INDICES
        );
        ClusterState state = indexState(Settings.builder().putList(ALLOWED_INDICES.getKey(), INDEX).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull("plugin.allowed_*").build(), LIST_ERROR);
    }

    public void testListFallbackUpdateRejectsInvalidEffectiveValue() {
        Setting<List<String>> allowedIndices = Setting.listSetting(
            "plugin.indices",
            ALLOWED_INDICES,
            Function.identity(),
            Property.Dynamic,
            Property.NodeScope
        );
        SettingsUpdater updater = listUpdater(Settings.EMPTY, allowedIndices, ALLOWED_INDICES);
        ClusterState state = indexState(Settings.EMPTY, Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putList(ALLOWED_INDICES.getKey(), "another-index").build(), LIST_ERROR);
    }

    public void testGroupMemberUpdateRejectsInvalidEffectiveValue() {
        SettingsUpdater updater = groupUpdater(Settings.EMPTY, GROUP);
        ClusterState state = indexState(Settings.EMPTY, Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().put("plugin.group.limit", 0).build(), LIMIT_ERROR);
    }

    public void testWildcardGroupMemberResetValidatesNodeSettings() {
        SettingsUpdater updater = groupUpdater(Settings.builder().put("plugin.group.limit", 0).build(), GROUP);
        ClusterState state = indexState(Settings.builder().put("plugin.group.limit", 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull("plugin.group.limit*").build(), LIMIT_ERROR);
    }

    public void testGroupValidatorUsesTransientMemberOverride() {
        AtomicInteger validations = new AtomicInteger();
        SettingsUpdater updater = createUpdater(Settings.EMPTY, Map.of(GROUP, (settings, state) -> {
            assertEquals(2, (int) GROUP.get(settings).getAsInt("limit", 0));
            assertEquals("updated", GROUP.get(settings).get("label"));
            validations.incrementAndGet();
        }));
        ClusterState state = indexState(
            Settings.builder().put("plugin.group.limit", 0).build(),
            Settings.builder().put("plugin.group.limit", 2).build()
        );
        ClusterState updatedState = updater.updateSettings(
            state,
            Settings.EMPTY,
            Settings.builder().put("plugin.group.label", "updated").build(),
            logger
        );
        assertEquals(0, (int) GROUP.get(updatedState.metadata().persistentSettings()).getAsInt("limit", -1));
        assertEquals(1, validations.get());
    }

    public void testPrefixSettingUpdateRejectsInvalidEffectiveValue() {
        AffixSetting<Integer> setting = Setting.prefixKeySetting(
            "plugin.limit.",
            key -> Setting.intSetting(key, 2, 0, Property.Dynamic, Property.NodeScope)
        );
        Setting<Integer> concrete = setting.getConcreteSettingForNamespace(INDEX);
        SettingsUpdater updater = createUpdater(Settings.EMPTY, Map.of(setting, limitValidator(concrete)));
        assertRejected(
            updater,
            indexState(Settings.EMPTY, Settings.EMPTY),
            Settings.builder().put(concrete.getKey(), 0).build(),
            LIMIT_ERROR
        );
    }

    public void testPrefixWildcardResetValidatesNodeSettings() {
        AffixSetting<Integer> setting = Setting.prefixKeySetting(
            "plugin.limit.",
            key -> Setting.intSetting(key, 2, 0, Property.Dynamic, Property.NodeScope)
        );
        Setting<Integer> concrete = setting.getConcreteSettingForNamespace(INDEX);
        SettingsUpdater updater = createUpdater(
            Settings.builder().put(concrete.getKey(), 0).build(),
            Map.of(setting, limitValidator(concrete))
        );
        ClusterState state = indexState(Settings.builder().put(concrete.getKey(), 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull("plugin.limit.validation*").build(), LIMIT_ERROR);
    }

    public void testAffixGroupWildcardResetValidatesNodeSettings() {
        AffixSetting<Settings> setting = Setting.affixKeySetting(
            "plugin.",
            "config",
            key -> Setting.groupSetting(key + ".", Property.Dynamic, Property.NodeScope)
        );
        String key = "plugin.example.config.limit";
        SettingsUpdater updater = createUpdater(
            Settings.builder().put(key, 0).build(),
            Map.of(setting, groupValidator(setting.getConcreteSettingForNamespace("example")))
        );
        ClusterState state = indexState(Settings.builder().put(key, 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull("plugin.example.*").build(), LIMIT_ERROR);
    }

    public void testRegisteredAffixFallbackUpdateRejectsInvalidEffectiveValue() {
        SettingsUpdater updater = affixFallbackUpdater(Settings.EMPTY);
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().put(BASE_LIMIT.getKey(), 0).build(), LIMIT_ERROR);
    }

    public void testRegisteredAffixFallbackResetValidatesNodeSettings() {
        SettingsUpdater updater = affixFallbackUpdater(Settings.builder().put(BASE_LIMIT.getKey(), 0).build());
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull(BASE_LIMIT.getKey()).build(), LIMIT_ERROR);
    }

    public void testRegisteredAffixFallbackWildcardResetValidatesNodeSettings() {
        SettingsUpdater updater = affixFallbackUpdater(Settings.builder().put(BASE_LIMIT.getKey(), 0).build());
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull("plugin.base_*").build(), LIMIT_ERROR);
    }

    public void testRegisteredNamespaceFallbackUpdateRejectsInvalidEffectiveValue() {
        String key = "plugin.defaults." + INDEX + ".limit";
        SettingsUpdater updater = namespaceFallbackUpdater(Settings.EMPTY);
        ClusterState state = indexState(Settings.builder().put(key, 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().put(key, 0).build(), LIMIT_ERROR);
    }

    public void testRegisteredNamespaceFallbackWildcardResetValidatesNodeSettings() {
        String key = "plugin.defaults." + INDEX + ".limit";
        SettingsUpdater updater = namespaceFallbackUpdater(Settings.builder().put(key, 0).build());
        ClusterState state = indexState(Settings.builder().put(key, 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().putNull("plugin.defaults.validation*").build(), LIMIT_ERROR);
    }

    public void testRegisteredDefaultFunctionDependencyRejectsInvalidEffectiveValue() {
        Setting<Integer> limit = new Setting<>(
            "plugin.derived_limit",
            settings -> Integer.toString(BASE_LIMIT.get(settings)),
            Integer::parseInt,
            Property.Dynamic,
            Property.NodeScope
        );
        BiConsumer<Settings, ClusterState> validator = limitValidator(limit);
        SettingsUpdater updater = createUpdater(Settings.EMPTY, Map.of(limit, validator, BASE_LIMIT, validator));
        ClusterState state = indexState(Settings.builder().put(BASE_LIMIT.getKey(), 2).build(), Settings.EMPTY);
        assertRejected(updater, state, Settings.builder().put(BASE_LIMIT.getKey(), 0).build(), LIMIT_ERROR);
    }

    private static SettingsUpdater affixFallbackUpdater(Settings nodeSettings) {
        AffixSetting<Integer> setting = Setting.affixKeySetting(
            "plugin.namespaces.",
            "limit",
            key -> Setting.intSetting(key, BASE_LIMIT, 0, Property.Dynamic, Property.NodeScope)
        );
        BiConsumer<Settings, ClusterState> validator = limitValidator(setting.getConcreteSettingForNamespace(INDEX));
        return createUpdater(nodeSettings, Map.of(setting, validator, BASE_LIMIT, validator));
    }

    private static SettingsUpdater namespaceFallbackUpdater(Settings nodeSettings) {
        AffixSetting<Integer> defaults = Setting.affixKeySetting(
            "plugin.defaults.",
            "limit",
            key -> Setting.intSetting(key, 2, 0, Property.Dynamic, Property.NodeScope)
        );
        AffixSetting<Integer> setting = Setting.affixKeySetting(
            "plugin.namespaces.",
            "limit",
            (namespace, key) -> Setting.intSetting(
                key,
                defaults.getConcreteSettingForNamespace(namespace),
                0,
                Property.Dynamic,
                Property.NodeScope
            )
        );
        BiConsumer<Settings, ClusterState> validator = limitValidator(setting.getConcreteSettingForNamespace(INDEX));
        return createUpdater(nodeSettings, Map.of(setting, validator, defaults, validator));
    }

    private SettingsUpdater listUpdater(Settings nodeSettings, Setting<List<String>> setting, Setting<?>... additionalSettings) {
        return createUpdater(nodeSettings, Map.of(setting, (settings, state) -> {
            if (setting.get(settings).containsAll(state.metadata().indices().keySet()) == false) {
                throw new IllegalArgumentException(LIST_ERROR);
            }
        }), additionalSettings);
    }

    private SettingsUpdater groupUpdater(Settings nodeSettings, Setting<Settings> setting) {
        return createUpdater(nodeSettings, Map.of(setting, groupValidator(setting)));
    }

    private static BiConsumer<Settings, ClusterState> groupValidator(Setting<Settings> setting) {
        return (settings, state) -> {
            if (setting.get(settings).getAsInt("limit", 2) < state.metadata().indices().size()) {
                throw new IllegalArgumentException(LIMIT_ERROR);
            }
        };
    }

    private static BiConsumer<Settings, ClusterState> limitValidator(Setting<Integer> setting) {
        return (settings, state) -> {
            if (setting.get(settings) < state.metadata().indices().size()) {
                throw new IllegalArgumentException(LIMIT_ERROR);
            }
        };
    }

    private static SettingsUpdater createUpdater(
        Settings nodeSettings,
        Map<Setting<?>, BiConsumer<Settings, ClusterState>> validators,
        Setting<?>... additionalSettings
    ) {
        Set<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.addAll(validators.keySet());
        Collections.addAll(allSettings, additionalSettings);
        return new SettingsUpdater(
            new ClusterSettings(nodeSettings, allSettings),
            new ClusterSettingsValidatorRegistry(nodeSettings, validators)
        );
    }

    private void assertRejected(SettingsUpdater updater, ClusterState state, Settings persistentUpdates, String message) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> updater.updateSettings(state, Settings.EMPTY, persistentUpdates, logger)
        );
        assertEquals(message, e.getMessage());
    }

    private static ClusterState indexState(Settings persistentSettings, Settings transientSettings) {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder().put(indexMetadata, false).persistentSettings(persistentSettings).transientSettings(transientSettings)
            )
            .build();
    }
}
