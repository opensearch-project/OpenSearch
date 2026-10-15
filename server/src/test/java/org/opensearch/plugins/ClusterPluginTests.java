/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.AffixSetting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ClusterPluginTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(DummyClusterPlugin.class, DummyClusterPlugin2.class);
    }

    public void testOnNodeStarted_shouldContainLocalNodeInfo() {

        DiscoveryNode localNode = DummyClusterPlugin.getLocalNode();

        assertTrue(localNode != null);
        // TODO Figure out if there is a way to check ephemeralId
        assertTrue(localNode.getId().equals(node().getNodeEnvironment().nodeId()));
    }

    public void testOnNodeStarted_shouldCallDeprecatedMethod() {
        DummyClusterPlugin2 dummyClusterPlugin2 = mock(DummyClusterPlugin2.class);
        dummyClusterPlugin2.onNodeStarted();
        verify(dummyClusterPlugin2, times(1)).onNodeStarted();

        DiscoveryNode localNode = DummyClusterPlugin2.getLocalNode();
        assertTrue(localNode != null);
    }

    public void testPluginSettingsValidatorRejectsBeforePublication() {
        createIndex("test");
        String key = DummyClusterPlugin.INDEX_LIMIT_SETTING.getKey();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(key, 0)).get()
        );
        assertThat(e.getMessage(), containsString("current index count"));
        assertFalse(client().admin().cluster().prepareState().get().getState().metadata().persistentSettings().hasValue(key));

        assertTrue(
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(key, 1)).get().isAcknowledged()
        );
        assertEquals("1", client().admin().cluster().prepareState().get().getState().metadata().persistentSettings().get(key));

        assertTrue(
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull(key)).get().isAcknowledged()
        );
        assertFalse(client().admin().cluster().prepareState().get().getState().metadata().persistentSettings().hasValue(key));
    }

    public void testRegisteredNamespaceFallbackValidatorRejectsBeforePublication() {
        createIndex("test");
        String fallbackKey = DummyClusterPlugin.DEFAULT_INDEX_LIMIT_SETTING.getKey();
        String namespaceKey = DummyClusterPlugin.NAMESPACED_INDEX_LIMIT_SETTING.getConcreteSettingForNamespace("test").getKey();
        try {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().put(fallbackKey, 0)).get()
            );
            assertThat(e.getMessage(), containsString("current index count"));
            assertFalse(client().admin().cluster().prepareState().get().getState().metadata().persistentSettings().hasValue(fallbackKey));

            assertTrue(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(fallbackKey, 0))
                    .setTransientSettings(Settings.builder().put(namespaceKey, 1))
                    .get()
                    .isAcknowledged()
            );

            IllegalArgumentException resetError = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(namespaceKey))
                    .get()
            );
            assertThat(resetError.getMessage(), containsString("current index count"));
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertEquals("0", state.metadata().persistentSettings().get(fallbackKey));
            assertEquals("1", state.metadata().transientSettings().get(namespaceKey));

            assertTrue(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(fallbackKey, 1))
                    .setTransientSettings(Settings.builder().putNull(namespaceKey))
                    .get()
                    .isAcknowledged()
            );
            ClusterState updatedState = client().admin().cluster().prepareState().get().getState();
            assertEquals("1", updatedState.metadata().persistentSettings().get(fallbackKey));
            assertFalse(updatedState.metadata().transientSettings().hasValue(namespaceKey));
        } finally {
            Settings resets = Settings.builder().putNull(fallbackKey).putNull(namespaceKey).build();
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(resets).setTransientSettings(resets).get();
        }
    }

}

final class DummyClusterPlugin extends Plugin implements ClusterPlugin {

    static final Setting<Integer> INDEX_LIMIT_SETTING = Setting.intSetting(
        "plugin.index_limit",
        10,
        0,
        Property.Dynamic,
        Property.NodeScope
    );
    static final Setting<Integer> DEFAULT_INDEX_LIMIT_SETTING = Setting.intSetting(
        "plugin.default_index_limit",
        10,
        0,
        Property.Dynamic,
        Property.NodeScope
    );
    static final AffixSetting<Integer> NAMESPACED_INDEX_LIMIT_SETTING = Setting.affixKeySetting(
        "plugin.index_limits.",
        "limit",
        key -> Setting.intSetting(key, DEFAULT_INDEX_LIMIT_SETTING, 0, Property.Dynamic, Property.NodeScope)
    );
    private static volatile DiscoveryNode localNode;

    public DummyClusterPlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(INDEX_LIMIT_SETTING, DEFAULT_INDEX_LIMIT_SETTING, NAMESPACED_INDEX_LIMIT_SETTING);
    }

    @Override
    public Map<Setting<?>, BiConsumer<Settings, ClusterState>> getClusterSettingsValidators() {
        BiConsumer<Settings, ClusterState> namespaceValidator = (settings, state) -> validateIndexLimit(
            NAMESPACED_INDEX_LIMIT_SETTING.getConcreteSettingForNamespace("test").get(settings),
            state
        );
        return Map.of(
            INDEX_LIMIT_SETTING,
            (settings, state) -> validateIndexLimit(INDEX_LIMIT_SETTING.get(settings), state),
            DEFAULT_INDEX_LIMIT_SETTING,
            namespaceValidator,
            NAMESPACED_INDEX_LIMIT_SETTING,
            namespaceValidator
        );
    }

    private static void validateIndexLimit(int limit, ClusterState state) {
        int indices = state.metadata().indices().size();
        if (indices > limit) {
            throw new IllegalArgumentException("index limit [" + limit + "] is below current index count [" + indices + "]");
        }
    }

    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        DummyClusterPlugin.localNode = localNode;
    }

    public static DiscoveryNode getLocalNode() {
        return localNode;
    }
}

class DummyClusterPlugin2 extends Plugin implements ClusterPlugin {

    private static volatile DiscoveryNode localNode;

    public DummyClusterPlugin2() {}

    @Override
    public void onNodeStarted() {
        localNode = mock(DiscoveryNode.class);
    }

    public static DiscoveryNode getLocalNode() {
        return localNode;
    }

}
