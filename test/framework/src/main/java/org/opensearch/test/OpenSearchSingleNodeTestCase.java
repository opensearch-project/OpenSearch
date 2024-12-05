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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.FeatureFlagSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.node.MockNode;
import org.opensearch.node.Node;
import org.opensearch.node.NodeValidationException;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptService;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.telemetry.MockTelemetryPlugin;
import org.opensearch.test.telemetry.tracing.StrictCheckSpanProcessor;
import org.opensearch.transport.TransportSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.coordination.ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */
public abstract class OpenSearchSingleNodeTestCase extends OpenSearchTestCase {

    private static Node NODE = null;

    protected void startNode(long seed) throws Exception {
        assert NODE == null;
        NODE = RandomizedContext.current().runWithPrivateRandomness(seed, this::newNode);
        // we must wait for the node to actually be up and running. otherwise the node might have started,
        // elected itself cluster-manager but might not yet have removed the
        // SERVICE_UNAVAILABLE/1/state not recovered / initialized block
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
        assertFalse(clusterHealthResponse.isTimedOut());
        client().admin()
            .indices()
            .preparePutTemplate("one_shard_index_template")
            .setPatterns(Collections.singletonList("*"))
            .setOrder(0)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .get();
        client().admin()
            .indices()
            .preparePutTemplate("random-soft-deletes-template")
            .setPatterns(Collections.singletonList("*"))
            .setOrder(0)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000)))
            .get();
    }

    private static void stopNode() throws IOException, InterruptedException {
        Node node = NODE;
        NODE = null;
        IOUtils.close(node);
        if (node != null && node.awaitClose(10, TimeUnit.SECONDS) == false) {
            throw new AssertionError("Node couldn't close within 10 seconds.");
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // the seed has to be created regardless of whether it will be used or not, for repeatability
        long seed = random().nextLong();
        // Create the node lazily, on the first test. This is ok because we do not randomize any settings,
        // only the cluster name. This allows us to have overridden properties for plugins and the version to use.
        if (NODE == null) {
            startNode(seed);
        }
    }

    @Override
    public void tearDown() throws Exception {
        logger.trace("[{}#{}]: cleaning up after test", getTestClass().getSimpleName(), getTestName());
        super.tearDown();
        assertAcked(
            client().admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN).get()
        );
        Metadata metadata = client().admin().cluster().prepareState().get().getState().getMetadata();
        assertThat(
            "test leaves persistent cluster metadata behind: " + metadata.persistentSettings().keySet(),
            metadata.persistentSettings().size(),
            equalTo(0)
        );
        assertThat(
            "test leaves transient cluster metadata behind: " + metadata.transientSettings().keySet(),
            metadata.transientSettings().size(),
            equalTo(0)
        );
        GetIndexResponse indices = client().admin()
            .indices()
            .prepareGetIndex()
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .addIndices("*")
            .get();
        assertThat(
            "test leaves indices that were not deleted: " + Strings.arrayToCommaDelimitedString(indices.indices()),
            indices.indices(),
            equalTo(Strings.EMPTY_ARRAY)
        );
        if (resetNodeAfterTest()) {
            assert NODE != null;
            stopNode();
            // the seed can be created within this if as it will either be executed before every test method or will never be.
            startNode(random().nextLong());
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        stopNode();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        stopNode();
        StrictCheckSpanProcessor.validateTracingStateOnShutdown();
    }

    /**
     * This method returns <code>true</code> if the node that is used in the background should be reset
     * after each test. This is useful if the test changes the cluster state metadata etc. The default is
     * <code>false</code>.
     */
    protected boolean resetNodeAfterTest() {
        return false;
    }

    /** The plugin classes that should be added to the node. */
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.emptyList();
    }

    /** Helper method to create list of plugins without specifying generic types. */
    @SafeVarargs
    @SuppressWarnings("varargs") // due to type erasure, the varargs type is non-reifiable, which causes this warning
    protected final Collection<Class<? extends Plugin>> pluginList(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }

    /** Additional settings to add when creating the node. Also allows overriding the default settings. */
    protected Settings nodeSettings() {
        return Settings.EMPTY;
    }

    /** True if a dummy http transport should be used, or false if the real http transport should be used. */
    protected boolean addMockHttpTransport() {
        return true;
    }

    private Node newNode() {
        final Path tempDir = createTempDir();
        final String nodeName = nodeSettings().get(Node.NODE_NAME_SETTING.getKey(), "node_s_0");

        final Settings featureFlagSettings = featureFlagSettings();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", random().nextLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo"))
            // TODO: use a consistent data path for custom paths
            // This needs to tie into the OpenSearchIntegTestCase#indexSettings() method
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), createTempDir().getParent())
            .put(Node.NODE_NAME_SETTING.getKey(), nodeName)
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 1) // limit the number of threads created
            .put("transport.type", getTestTransportType())
            .put(TransportSettings.PORT.getKey(), getPortRange())
            .put(dataNode())
            .put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), random().nextLong())
            // default the watermarks low values to prevent tests from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // turning on the real memory circuit breaker leads to spurious test failures. As have no full control over heap usage, we
            // turn it off for these tests.
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey(), nodeName)
            .put(FeatureFlags.TELEMETRY_SETTING.getKey(), true)
            .put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true)
            .put(TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING.getKey(), true)
            // By default, for tests we will put the target slice count of 2. This will increase the probability of having multiple slices
            // when tests are run with concurrent segment search enabled
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_KEY, 2)
            .put(nodeSettings()) // allow test cases to provide their own settings or override these
            .put(featureFlagSettings);

        Collection<Class<? extends Plugin>> plugins = getPlugins();
        if (plugins.contains(getTestTransportPlugin()) == false) {
            plugins = new ArrayList<>(plugins);
            plugins.add(getTestTransportPlugin());
        }
        if (addMockHttpTransport()) {
            plugins.add(MockHttpTransport.TestPlugin.class);
        }
        plugins.add(MockScriptService.TestPlugin.class);

        plugins.add(MockTelemetryPlugin.class);
        Node node = new MockNode(settingsBuilder.build(), plugins, forbidPrivateIndexSettings());
        try {
            node.start();
        } catch (NodeValidationException e) {
            throw new RuntimeException(e);
        }
        return node;
    }

    /**
     * Returns a client to the single-node cluster.
     */
    public Client client() {
        return wrapClient(NODE.client());
    }

    public Client wrapClient(final Client client) {
        return client;
    }

    /**
     * Return a reference to the singleton node.
     */
    protected Node node() {
        return NODE;
    }

    /**
     * Get an instance for a particular class using the injector of the singleton node.
     */
    protected <T> T getInstanceFromNode(Class<T> clazz) {
        return NODE.injector().getInstance(clazz);
    }

    /**
     * Create a new index on the singleton node with empty index settings.
     */
    protected IndexService createIndex(String index) {
        return createIndex(index, Settings.EMPTY);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     */
    protected IndexService createIndex(String index, Settings settings) {
        return createIndex(index, settings, null, (XContentBuilder) null);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     * @deprecated types are being removed
     */
    @Deprecated
    protected IndexService createIndex(String index, Settings settings, String type, XContentBuilder mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (type != null && mappings != null) {
            createIndexRequestBuilder.setMapping(mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    /**
     * Create a new index on the singleton node with the provided index settings.
     * @deprecated types are being removed
     */
    @Deprecated
    protected IndexService createIndex(String index, Settings settings, String type, String... mappings) {
        CreateIndexRequestBuilder createIndexRequestBuilder = client().admin().indices().prepareCreate(index).setSettings(settings);
        if (mappings != null) {
            createIndexRequestBuilder.setMapping(mappings);
        }
        return createIndex(index, createIndexRequestBuilder);
    }

    protected IndexService createIndex(String index, CreateIndexRequestBuilder createIndexRequestBuilder) {
        assertAcked(createIndexRequestBuilder.get());
        // Wait for the index to be allocated so that cluster state updates don't override
        // changes that would have been done locally
        ClusterHealthResponse health = client().admin()
            .cluster()
            .health(
                Requests.clusterHealthRequest(index).waitForYellowStatus().waitForEvents(Priority.LANGUID).waitForNoRelocatingShards(true)
            )
            .actionGet();
        assertThat(health.getStatus(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW));
        assertThat("Cluster must be a single node cluster", health.getNumberOfDataNodes(), equalTo(1));
        IndicesService instanceFromNode = getInstanceFromNode(IndicesService.class);
        return instanceFromNode.indexServiceSafe(resolveIndex(index));
    }

    public Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    /**
     * Create a new search context.
     */
    protected SearchContext createSearchContext(IndexService indexService) {
        BigArrays bigArrays = indexService.getBigArrays();
        return new TestSearchContext(bigArrays, indexService);
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(30), indices);
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     *
     * @param timeout time out value to set on {@link ClusterHealthRequest}
     */
    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        ClusterHealthResponse actionGet = client().admin()
            .cluster()
            .health(
                Requests.clusterHealthRequest(indices)
                    .timeout(timeout)
                    .waitForGreenStatus()
                    .waitForEvents(Priority.LANGUID)
                    .waitForNoRelocatingShards(true)
            )
            .actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "ensureGreen timed out, cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get()
            );
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return getInstanceFromNode(NamedXContentRegistry.class);
    }

    protected boolean forbidPrivateIndexSettings() {
        return true;
    }

    /**
     * Setting all feature flag settings at base IT, which can be overridden later by individual
     * IT classes.
     *
     * @return Feature flag settings.
     */
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        for (Setting builtInFlag : FeatureFlagSettings.BUILT_IN_FEATURE_FLAGS) {
            featureSettings.put(builtInFlag.getKey(), builtInFlag.getDefaultRaw(Settings.EMPTY));
        }
        featureSettings.put(FeatureFlags.TELEMETRY_SETTING.getKey(), true);
        featureSettings.put(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING.getKey(), true);
        return featureSettings.build();
    }

}
