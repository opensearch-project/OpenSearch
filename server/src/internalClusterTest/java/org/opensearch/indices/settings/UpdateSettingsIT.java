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

package org.opensearch.indices.settings;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.applicationtemplates.ClusterStateSystemTemplateLoader;
import org.opensearch.cluster.applicationtemplates.SystemTemplate;
import org.opensearch.cluster.applicationtemplates.SystemTemplateMetadata;
import org.opensearch.cluster.applicationtemplates.TemplateRepositoryMetadata;
import org.opensearch.cluster.metadata.Context;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSettingsIT extends OpenSearchIntegTestCase {
    public void testInvalidUpdateOnClosedIndex() {
        createIndex("test");
        assertAcked(client().admin().indices().prepareClose("test").get());
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.analysis.char_filter.invalid_char.type", "invalid"))
                .get()
        );
        assertEquals(exception.getMessage(), "Unknown char_filter type [invalid] for [invalid_char]");
    }

    public void testInvalidDynamicUpdate() {
        createIndex("test");
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.dummy", "boom"))
                .execute()
                .actionGet()
        );
        assertEquals(exception.getCause().getMessage(), "this setting goes boom");
        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertNotEquals(indexMetadata.getSettings().get("index.dummy"), "invalid dynamic value");
    }

    public void testDynamicUpdateWithContextSettingOverlap() throws IOException {
        String templateContent = "{\n"
            + "  \"template\": {\n"
            + "    \"settings\": {\n"
            + "      \"index.merge.policy\": \"log_byte_size\"\n"
            + "    }\n"
            + "  },\n"
            + "  \"_meta\": {\n"
            + "    \"_type\": \"@abc_template\",\n"
            + "    \"_version\": 1\n"
            + "  },\n"
            + "  \"version\": 1\n"
            + "}\n";

        ClusterStateSystemTemplateLoader loader = new ClusterStateSystemTemplateLoader(
            internalCluster().clusterManagerClient(),
            () -> internalCluster().getInstance(ClusterService.class).state()
        );
        loader.loadTemplate(
            new SystemTemplate(
                BytesReference.fromByteBuffer(ByteBuffer.wrap(templateContent.getBytes(StandardCharsets.UTF_8))),
                SystemTemplateMetadata.fromComponentTemplateInfo("testcontext", 1L),
                new TemplateRepositoryMetadata(UUID.randomUUID().toString(), 1L)
            )
        );

        createIndex("test", new Context("testcontext"));

        IllegalArgumentException validationException = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.merge.policy", "tiered"))
                .execute()
                .actionGet()
        );
        assertTrue(
            validationException.getMessage()
                .contains("Cannot apply context template as user provide settings have overlap with the included context template")
        );

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.refresh_interval", "60s"))
                .execute()
                .actionGet()
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DummySettingPlugin.class, FinalSettingPlugin.class);
    }

    public static class DummySettingPlugin extends Plugin {
        public static final Setting<String> DUMMY_SETTING = Setting.simpleString(
            "index.dummy",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        );

        public static final Setting.AffixSetting<String> DUMMY_ACCOUNT_USER = Setting.affixKeySetting(
            "index.acc.",
            "user",
            k -> Setting.simpleString(k, Setting.Property.IndexScope, Setting.Property.Dynamic)
        );
        public static final Setting<String> DUMMY_ACCOUNT_PW = Setting.affixKeySetting(
            "index.acc.",
            "pw",
            k -> Setting.simpleString(k, Setting.Property.IndexScope, Setting.Property.Dynamic),
            () -> DUMMY_ACCOUNT_USER
        );

        public static final Setting.AffixSetting<String> DUMMY_ACCOUNT_USER_CLUSTER = Setting.affixKeySetting(
            "cluster.acc.",
            "user",
            k -> Setting.simpleString(k, Setting.Property.NodeScope, Setting.Property.Dynamic)
        );
        public static final Setting<String> DUMMY_ACCOUNT_PW_CLUSTER = Setting.affixKeySetting(
            "cluster.acc.",
            "pw",
            k -> Setting.simpleString(k, Setting.Property.NodeScope, Setting.Property.Dynamic),
            () -> DUMMY_ACCOUNT_USER_CLUSTER
        );

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSettingsUpdateConsumer(DUMMY_SETTING, (s) -> {}, (s) -> {
                if (s.equals("boom")) throw new IllegalArgumentException("this setting goes boom");
            });
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(DUMMY_SETTING, DUMMY_ACCOUNT_PW, DUMMY_ACCOUNT_USER, DUMMY_ACCOUNT_PW_CLUSTER, DUMMY_ACCOUNT_USER_CLUSTER);
        }
    }

    public static class FinalSettingPlugin extends Plugin {
        public static final Setting<String> FINAL_SETTING = Setting.simpleString(
            "index.final",
            Setting.Property.IndexScope,
            Setting.Property.Final
        );

        @Override
        public void onIndexModule(IndexModule indexModule) {}

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(FINAL_SETTING);
        }
    }

    /**
     * Needed by {@link UpdateSettingsIT#testEngineGCDeletesSetting()}
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("thread_pool.estimated_time_interval", 0).build();
    }

    public void testUpdateDependentClusterSettings() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("cluster.acc.test.pw", "asdf"))
                .get()
        );
        assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.acc.test.pw", "asdf"))
                .get()
        );
        assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.acc.test.pw", "asdf"))
                .setPersistentSettings(Settings.builder().put("cluster.acc.test.user", "asdf"))
                .get()
        );
        assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

        if (randomBoolean()) {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.acc.test.pw", "asdf").put("cluster.acc.test.user", "asdf"))
                .get();
            iae = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull("cluster.acc.test.user"))
                    .get()
            );
            assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull("cluster.acc.test.pw").putNull("cluster.acc.test.user"))
                .get();
        } else {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("cluster.acc.test.pw", "asdf").put("cluster.acc.test.user", "asdf"))
                .get();

            iae = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().putNull("cluster.acc.test.user"))
                    .get()
            );
            assertEquals("missing required setting [cluster.acc.test.user] for setting [cluster.acc.test.pw]", iae.getMessage());

            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("cluster.acc.test.pw").putNull("cluster.acc.test.user"))
                .get();
        }

    }

    public void testUpdateDependentIndexSettings() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test", Settings.builder().put("index.acc.test.pw", "asdf")).get()
        );
        assertEquals("missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

        createIndex("test");
        for (int i = 0; i < 2; i++) {
            if (i == 1) {
                // now do it on a closed index
                client().admin().indices().prepareClose("test").get();
            }

            iae = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.acc.test.pw", "asdf"))
                    .execute()
                    .actionGet()
            );
            assertEquals("missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

            // user has no dependency
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.acc.test.user", "asdf"))
                .execute()
                .actionGet();

            // now we are consistent
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.acc.test.pw", "test"))
                .execute()
                .actionGet();

            // now try to remove it and make sure it fails
            iae = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().putNull("index.acc.test.user"))
                    .execute()
                    .actionGet()
            );
            assertEquals("missing required setting [index.acc.test.user] for setting [index.acc.test.pw]", iae.getMessage());

            // now we are consistent
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull("index.acc.test.pw").putNull("index.acc.test.user"))
                .execute()
                .actionGet();
        }
    }

    public void testResetDefaultWithWildcard() {
        createIndex("test");

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.refresh_interval", -1))
            .execute()
            .actionGet();
        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals(indexMetadata.getSettings().get("index.refresh_interval"), "-1");
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), -1);
            }
        }
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().putNull("index.ref*"))
            .execute()
            .actionGet();
        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertNull(indexMetadata.getSettings().get("index.refresh_interval"));
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), 1000);
            }
        }
    }

    public void testResetDefault() {
        createIndex("test");
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put("index.refresh_interval", -1)
                    .put("index.translog.flush_threshold_size", "1024b")
                    .put("index.translog.generation_threshold_size", "4096b")
            )
            .execute()
            .actionGet();
        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertEquals(indexMetadata.getSettings().get("index.refresh_interval"), "-1");
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), -1);
                assertEquals(indexService.getIndexSettings().getFlushThresholdSize().getBytes(), 1024);
                assertEquals(indexService.getIndexSettings().getGenerationThresholdSize().getBytes(), 4096);
            }
        }
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().putNull("index.refresh_interval"))
            .execute()
            .actionGet();
        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertNull(indexMetadata.getSettings().get("index.refresh_interval"));
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(resolveIndex("test"));
            if (indexService != null) {
                assertEquals(indexService.getIndexSettings().getRefreshInterval().millis(), 1000);
                assertEquals(indexService.getIndexSettings().getFlushThresholdSize().getBytes(), 1024);
                assertEquals(indexService.getIndexSettings().getGenerationThresholdSize().getBytes(), 4096);
            }
        }
    }

    public void testOpenCloseUpdateSettings() throws Exception {
        createIndex("test");
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1) // this one can change
                        .put("index.fielddata.cache", "none")
                ) // this one can't
                .execute()
                .actionGet()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1) // this one can change
                        .put("index.final", "no")
                ) // this one can't
                .execute()
                .actionGet()
        );
        IndexMetadata indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), nullValue());
        assertThat(indexMetadata.getSettings().get("index.fielddata.cache"), nullValue());
        assertThat(indexMetadata.getSettings().get("index.final"), nullValue());

        // Now verify via dedicated get settings api:
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.fielddata.cache"), nullValue());
        assertThat(getSettingsResponse.getSetting("test", "index.final"), nullValue());

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.refresh_interval", -1)) // this one can change
            .execute()
            .actionGet();

        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), equalTo("-1"));
        // Now verify via dedicated get settings api:
        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("-1"));

        // now close the index, change the non dynamic setting, and see that it applies

        // Wait for the index to turn green before attempting to close it
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setTimeout("30s")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .execute()
            .actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        client().admin().indices().prepareClose("test").execute().actionGet();

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .execute()
            .actionGet();

        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getNumberOfReplicas(), equalTo(1));

        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put("index.refresh_interval", "1s") // this one can change
                    .put("index.fielddata.cache", "none")
            ) // this one can't
            .execute()
            .actionGet();

        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetadata.getSettings().get("index.fielddata.cache"), equalTo("none"));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.refresh_interval", -1) // this one can change
                        .put("index.final", "no")
                ) // this one really can't
                .execute()
                .actionGet()
        );
        assertThat(ex.getMessage(), containsString("final test setting [index.final], not updateable"));
        indexMetadata = client().admin().cluster().prepareState().execute().actionGet().getState().metadata().index("test");
        assertThat(indexMetadata.getSettings().get("index.refresh_interval"), equalTo("1s"));
        assertThat(indexMetadata.getSettings().get("index.final"), nullValue());

        // Now verify via dedicated get settings api:
        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        assertThat(getSettingsResponse.getSetting("test", "index.refresh_interval"), equalTo("1s"));
        assertThat(getSettingsResponse.getSetting("test", "index.final"), nullValue());
    }

    public void testEngineGCDeletesSetting() throws Exception {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource("f", 1).setVersionType(VersionType.EXTERNAL).setVersion(1).get();
        client().prepareDelete("test", "1").setVersionType(VersionType.EXTERNAL).setVersion(2).get();
        // delete is still in cache this should fail
        assertRequestBuilderThrows(
            client().prepareIndex("test").setId("1").setSource("f", 3).setVersionType(VersionType.EXTERNAL).setVersion(1),
            VersionConflictEngineException.class
        );
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.gc_deletes", 0)));

        client().prepareDelete("test", "1").setVersionType(VersionType.EXTERNAL).setVersion(4).get();

        // Make sure the time has advanced for InternalEngine#resolveDocVersion()
        for (ThreadPool threadPool : internalCluster().getInstances(ThreadPool.class)) {
            long startTime = threadPool.relativeTimeInMillis();
            assertBusy(() -> assertThat(threadPool.relativeTimeInMillis(), greaterThan(startTime)));
        }

        // delete should not be in cache
        client().prepareIndex("test").setId("1").setSource("f", 2).setVersionType(VersionType.EXTERNAL).setVersion(1);
    }

    public void testUpdateSettingsWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        Settings.Builder builder = Settings.builder().put("index.refresh_interval", -1);

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(builder));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Closing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareUpdateSettings("test").setSettings(builder));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testSettingsVersion() {
        createIndex("test");
        ensureGreen("test");

        {
            final long settingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.refresh_interval", "500ms"))
                    .get()
            );
            final long newSettingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(1 + settingsVersion));
        }

        {
            final boolean block = randomBoolean();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.blocks.read_only", block))
                    .get()
            );
            final long settingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.blocks.read_only", block == false))
                    .get()
            );
            final long newSettingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(1 + settingsVersion));

            // if the read-only block is present, remove it
            if (block == false) {
                assertAcked(
                    client().admin()
                        .indices()
                        .prepareUpdateSettings("test")
                        .setSettings(Settings.builder().put("index.blocks.read_only", false))
                        .get()
                );
            }
        }
    }

    public void testSettingsVersionUnchanged() {
        createIndex("test");
        ensureGreen("test");

        {
            final long settingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            final String refreshInterval = client().admin()
                .indices()
                .prepareGetSettings("test")
                .get()
                .getSetting("test", "index.refresh_interval");
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.refresh_interval", refreshInterval))
                    .get()
            );
            final long newSettingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(settingsVersion));
        }

        {
            final boolean block = randomBoolean();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.blocks.read_only", block))
                    .get()
            );
            // now put the same block again
            final long settingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.blocks.read_only", block))
                    .get()
            );
            final long newSettingsVersion = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .index("test")
                .getSettingsVersion();
            assertThat(newSettingsVersion, equalTo(settingsVersion));

            // if the read-only block is present, remove it
            if (block) {
                assertAcked(
                    client().admin()
                        .indices()
                        .prepareUpdateSettings("test")
                        .setSettings(Settings.builder().put("index.blocks.read_only", false))
                        .get()
                );
            }
        }
    }

    /**
     * The setting {@link IndexMetadata#SETTING_NUMBER_OF_REPLICAS} is special due to handling in
     * {@link IndexMetadata.Builder#numberOfReplicas(int)}. Therefore we have a dedicated test that this setting is handled properly with
     * respect to settings version when applying a settings change that does not change the number of replicas.
     */
    public void testNumberOfReplicasSettingsVersionUnchanged() {
        createIndex("test");

        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        final int numberOfReplicas = Integer.valueOf(
            client().admin().indices().prepareGetSettings("test").get().getSetting("test", "index.number_of_replicas")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", numberOfReplicas))
                .get()
        );
        final long newSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(newSettingsVersion, equalTo(settingsVersion));
    }

    /**
     * The setting {@link IndexMetadata#SETTING_NUMBER_OF_REPLICAS} is special due to handling in
     * {@link IndexMetadata.Builder#numberOfReplicas(int)}. Therefore we have a dedicated test that this setting is handled properly with
     * respect to settings version when changing the number of replicas.
     */
    public void testNumberOfReplicasSettingsVersion() {
        createIndex("test");

        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        final int numberOfReplicas = Integer.valueOf(
            client().admin().indices().prepareGetSettings("test").get().getSetting("test", "index.number_of_replicas")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", 1 + numberOfReplicas))
                .get()
        );
        final long newSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(newSettingsVersion, equalTo(1 + settingsVersion));
    }

    /*
     * Test that we are able to set the setting index.number_of_replicas to the default.
     */
    public void testDefaultNumberOfReplicasOnOpenIndices() {
        runTestDefaultNumberOfReplicasTest(false);
    }

    public void testDefaultNumberOfReplicasOnClosedIndices() {
        runTestDefaultNumberOfReplicasTest(true);
    }

    private void runTestDefaultNumberOfReplicasTest(final boolean closeIndex) {
        if (randomBoolean()) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate("test")
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, 8)))
            );
        } else {
            assertAcked(client().admin().indices().prepareCreate("test"));
        }

        if (closeIndex) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }

        /*
         * Previous versions of Elasticsearch would throw an exception that the number of replicas had to have a value, and could not be
         * null. In the update settings logic, we ensure this by providing an explicit default value if the setting is set to null.
         */
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_NUMBER_OF_REPLICAS))
        );

        final GetSettingsResponse response = client().admin().indices().prepareGetSettings("test").get();

        // we removed the setting but it should still have an explicit value since index metadata requires this
        assertTrue(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(response.getIndexToSettings().get("test")));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(response.getIndexToSettings().get("test")), equalTo(1));
    }

    public void testNullReplicaUpdate() {
        internalCluster().ensureAtLeastNumDataNodes(2);

        // cluster setting
        String defaultNumberOfReplica = "3";
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put("cluster.default_number_of_replicas", defaultNumberOfReplica))
                .get()
        );
        // create index with number of replicas will ignore default value
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "2"))
        );

        String numberOfReplicas = client().admin()
            .indices()
            .prepareGetSettings("test")
            .get()
            .getSetting("test", IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
        assertEquals("2", numberOfReplicas);
        // update setting with null replica will use cluster setting of replica number
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_NUMBER_OF_REPLICAS))
        );

        numberOfReplicas = client().admin()
            .indices()
            .prepareGetSettings("test")
            .get()
            .getSetting("test", IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
        assertEquals(defaultNumberOfReplica, numberOfReplicas);

        // Clean up cluster setting, then update setting with null replica should pick up default value of 1
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("cluster.default_number_of_replicas"))
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_NUMBER_OF_REPLICAS))
        );

        numberOfReplicas = client().admin()
            .indices()
            .prepareGetSettings("test")
            .get()
            .getSetting("test", IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
        assertEquals("1", numberOfReplicas);
    }

    public void testNoopUpdate() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final ClusterService clusterService = internalCluster().getClusterManagerNodeInstance(ClusterService.class);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
        );

        ClusterState currentState = clusterService.state();
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertNotSame(currentState, clusterService.state());
        client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForGreenStatus()
            .setWaitForNoInitializingShards(true)
            .setWaitForEvents(Priority.LANGUID)
            .setTimeout(TimeValue.MAX_VALUE)
            .get();
        currentState = clusterService.state();

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertSame(clusterService.state(), currentState);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_NUMBER_OF_REPLICAS))
        );
        assertSame(clusterService.state(), currentState);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull(SETTING_BLOCKS_READ).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertSame(currentState, clusterService.state());

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(SETTING_BLOCKS_READ, true).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        assertNotSame(currentState, clusterService.state());
        currentState = clusterService.state();

        assertAcked(
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(SETTING_BLOCKS_READ, true))
        );
        assertSame(currentState, clusterService.state());

        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().putNull(SETTING_BLOCKS_READ)));
        assertNotSame(currentState, clusterService.state());
    }

}
