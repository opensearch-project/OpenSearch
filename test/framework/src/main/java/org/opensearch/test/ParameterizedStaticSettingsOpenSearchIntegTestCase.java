/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.search.lookup.LeafFieldsLookup;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Base class for running the tests with parameterization with static settings: the cluster will be pre-created with the settings at startup, the method
 * {@link #hasSameParametersAs(ParameterizedOpenSearchIntegTestCase)} is being used by the test scaffolding to detect when the test suite is instantiated with
 * the new parameters and the test cluster has to be recreated.
 * <p>
 * Here is the simple illustration on of the execution flow per parameters combination:
 * <ul>
 *   <li><b>suite scope</b>: create cluster -&gt; for each test method { run test method } -&gt; shutdown cluster</li>
 *   <li><b>test scope</b>: for each test method { create cluster -&gt; run test method -&gt; shutdown cluster }</li>
 * </ul>
 */
public abstract class ParameterizedStaticSettingsOpenSearchIntegTestCase extends ParameterizedOpenSearchIntegTestCase {

    protected static final String REMOTE_STORE_REPOSITORY_NAME = "test-remote-store-repo";
    private Path remoteStoreRepositoryPath;
    public static final List<Object[]> replicationSettings = Arrays.asList(
        new Object[] { Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build() },
        new Object[] { Settings.builder().put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build() }
    );

    public ParameterizedStaticSettingsOpenSearchIntegTestCase(Settings nodeSettings) {
        super(nodeSettings);
    }

    public static final List<Object[]> remoteStoreSettings = Arrays.asList(
        new Object[] { Settings.builder().put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build() },
        new Object[] { Settings.builder().put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), false).build() }
    );

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder();
        if (REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings)) {
            if (remoteStoreRepositoryPath == null) {
                remoteStoreRepositoryPath = randomRepoPath().toAbsolutePath();
            }
            builder.put(remoteStoreClusterSettings(REMOTE_STORE_REPOSITORY_NAME, remoteStoreRepositoryPath));
        }
        return builder.put(super.nodeSettings(nodeOrdinal)).put(settings).build();
    }

    @Override
    boolean hasSameParametersAs(ParameterizedOpenSearchIntegTestCase obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        final ParameterizedStaticSettingsOpenSearchIntegTestCase other = (ParameterizedStaticSettingsOpenSearchIntegTestCase) obj;
        return Objects.equals(settings, other.settings);
    }

    protected static void indexTestData(Client client) {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client.prepareIndex("test").setId(Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        public static final String SCRIPT_NAME = "search_block";

        public final AtomicInteger hits = new AtomicInteger();

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(ScriptedBlockPlugin.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    assertBusy(() -> assertFalse(shouldBlock.get()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    protected void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        int numberOfShards = getNumShards("test").numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        });
    }

    protected void disableBlocks(List<ScriptedBlockPlugin> plugins) throws Exception {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }
}
