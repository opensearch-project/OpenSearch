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

package org.opensearch.script;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class StoredScriptsIT extends OpenSearchIntegTestCase {

    private static final int SCRIPT_MAX_SIZE_IN_BYTES = 64;
    private static final String LANG = MockScriptEngine.NAME;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), SCRIPT_MAX_SIZE_IN_BYTES)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class);
    }

    public void testBasics() {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("foobar")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"" + LANG + "\", \"source\": \"1\"} }"), MediaTypeRegistry.JSON)
        );
        String script = client().admin().cluster().prepareGetStoredScript("foobar").get().getSource().getSource();
        assertNotNull(script);
        assertEquals("1", script);

        assertAcked(client().admin().cluster().prepareDeleteStoredScript().setId("foobar"));
        StoredScriptSource source = client().admin().cluster().prepareGetStoredScript("foobar").get().getSource();
        assertNull(source);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("id#")
                .setContent(new BytesArray("{}"), MediaTypeRegistry.JSON)
                .get()
        );
        assertEquals("Validation Failed: 1: id cannot contain '#' for stored script;", e.getMessage());
    }

    public void testMaxScriptSize() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("foobar")
                .setContent(
                    new BytesArray("{\"script\": { \"lang\": \"" + LANG + "\"," + " \"source\":\"0123456789abcdef\"} }"),
                    MediaTypeRegistry.JSON
                )
                .get()
        );
        assertEquals("exceeded max allowed stored script size in bytes [64] with size [65] for script [foobar]", e.getMessage());
    }

    public void testRejectPersistentMaxScriptSizeBelowStoredScript() throws Exception {
        putTwoByteStoredScript();
        String clusterManager = internalCluster().getClusterManagerName();
        try {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 1))
                    .get()
            );
            assertScriptSizeRejected(e);
            assertMaxScriptSize(null, null, SCRIPT_MAX_SIZE_IN_BYTES);

            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 2))
            );
            assertMaxScriptSize(2, null, 2);
            assertEquals(clusterManager, internalCluster().getClusterManagerName());
        } finally {
            deleteScriptAndResetMaxScriptSize();
        }
    }

    public void testRejectTransientMaxScriptSizeBelowStoredScript() throws Exception {
        putTwoByteStoredScript();
        String clusterManager = internalCluster().getClusterManagerName();
        try {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 1))
                    .get()
            );
            assertScriptSizeRejected(e);
            assertMaxScriptSize(null, null, SCRIPT_MAX_SIZE_IN_BYTES);

            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 2))
            );
            assertMaxScriptSize(null, 2, 2);
            assertEquals(clusterManager, internalCluster().getClusterManagerName());
        } finally {
            deleteScriptAndResetMaxScriptSize();
        }
    }

    public void testRejectResetRevealingMaxScriptSizeBelowStoredScript() throws Exception {
        putTwoByteStoredScript();
        String clusterManager = internalCluster().getClusterManagerName();
        try {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 2))
                    .setPersistentSettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 1))
            );
            assertMaxScriptSize(1, 2, 2);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey()))
                    .get()
            );
            assertScriptSizeRejected(e);
            assertMaxScriptSize(1, 2, 2);

            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey(), 2))
                    .setTransientSettings(Settings.builder().putNull(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey()))
            );
            assertMaxScriptSize(2, null, 2);
            assertEquals(clusterManager, internalCluster().getClusterManagerName());
        } finally {
            deleteScriptAndResetMaxScriptSize();
        }
    }

    private void putTwoByteStoredScript() {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("two-bytes")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"" + LANG + "\", \"source\": \"22\"}}"), MediaTypeRegistry.JSON)
        );
    }

    private void assertScriptSizeRejected(IllegalArgumentException e) {
        assertEquals(
            "script.max_size_in_bytes cannot be set to [1], stored script [two-bytes] exceeds the new value with a size of [2]",
            e.getMessage()
        );
    }

    private void assertMaxScriptSize(Integer persistentValue, Integer transientValue, int effectiveValue) throws Exception {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String key = ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey();
        assertEquals(persistentValue == null ? null : persistentValue.toString(), state.metadata().persistentSettings().get(key));
        assertEquals(transientValue == null ? null : transientValue.toString(), state.metadata().transientSettings().get(key));
        assertBusy(() -> {
            for (ClusterService service : internalCluster().getInstances(ClusterService.class)) {
                assertEquals(effectiveValue, (int) service.getClusterSettings().get(ScriptService.SCRIPT_MAX_SIZE_IN_BYTES));
            }
        });
    }

    private void deleteScriptAndResetMaxScriptSize() {
        assertAcked(client().admin().cluster().prepareDeleteStoredScript().setId("two-bytes"));
        String key = ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.getKey();
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(key))
                .setTransientSettings(Settings.builder().putNull(key))
        );
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of("1", script -> "1", "22", script -> "22");
        }
    }
}
