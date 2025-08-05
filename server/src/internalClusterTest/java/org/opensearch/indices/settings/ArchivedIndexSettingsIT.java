/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.startsWith;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class ArchivedIndexSettingsIT extends OpenSearchIntegTestCase {
    private volatile boolean installPlugin;

    public void testArchiveSettings() throws Exception {
        installPlugin = true;
        // Set up the cluster with an index containing dummy setting(owned by dummy plugin)
        String oldClusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String oldDataNode = internalCluster().startDataOnlyNode();
        assertEquals(2, internalCluster().numDataAndClusterManagerNodes());
        createIndex("test");
        ensureYellow();
        // Add a dummy setting
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.dummy", "foobar").put("index.dummy2", "foobar"))
            .execute()
            .actionGet();

        // Remove dummy plugin and replace the cluster manager node so that the stale plugin setting moves to "archived".
        installPlugin = false;
        String newClusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(oldClusterManagerNode));
        internalCluster().restartNode(newClusterManagerNode);

        // Verify that archived settings exists.
        assertBusy(() -> {
            // Verify that cluster state is in recovered state.
            assertFalse(client().admin().cluster().prepareState().get().getState().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
            assertTrue(
                client().admin()
                    .indices()
                    .prepareGetSettings("test")
                    .get()
                    .getIndexToSettings()
                    .get("test")
                    .hasValue("archived.index.dummy")
            );
            assertTrue(
                client().admin()
                    .indices()
                    .prepareGetSettings("test")
                    .get()
                    .getIndexToSettings()
                    .get("test")
                    .hasValue("archived.index.dummy2")
            );
        }, 30, TimeUnit.SECONDS);

        // Archived setting update should fail on open index.
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull("archived.index.dummy"))
                .execute()
                .actionGet()
        );
        assertThat(
            exception.getMessage(),
            startsWith("Can't update non dynamic settings [[archived.index.dummy]] for open indices [[test")
        );

        // Verify that a random unrelated setting can be updated when archived settings are present.
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.max_terms_count", 1024).build())
            .execute()
            .actionGet();

        // close the index.
        client().admin().indices().prepareClose("test").get();

        // Remove archived.index.dummy explicitly.
        assertTrue(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull("archived.index.dummy"))
                .execute()
                .actionGet()
                .isAcknowledged()
        );

        // Remove archived.index.dummy2 using wildcard.
        assertTrue(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull("archived.*"))
                .execute()
                .actionGet()
                .isAcknowledged()
        );

        // Verify that archived settings are cleaned up successfully.
        assertFalse(
            client().admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test").hasValue("archived.index.dummy")
        );
        assertFalse(
            client().admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test").hasValue("archived.index.dummy2")
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return installPlugin ? Arrays.asList(DummySettingPlugin.class) : Collections.emptyList();
    }

    public static class DummySettingPlugin extends Plugin {
        public static final Setting<String> DUMMY_SETTING = Setting.simpleString(
            "index.dummy",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        );
        public static final Setting<String> DUMMY_SETTING2 = Setting.simpleString(
            "index.dummy2",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        );

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(DUMMY_SETTING, DUMMY_SETTING2);
        }
    }
}
