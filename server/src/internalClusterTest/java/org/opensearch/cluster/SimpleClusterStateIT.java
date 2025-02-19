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

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;
import org.opensearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertIndexTemplateExists;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

/**
 * Checking simple filtering capabilities of the cluster state
 *
 */
public class SimpleClusterStateIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(PrivateCustomPlugin.class);
    }

    @Override
    protected boolean useRandomReplicationStrategy() {
        return true;
    }

    @Before
    public void indexData() throws Exception {
        index("foo", "bar", "1", XContentFactory.jsonBuilder().startObject().field("foo", "foo").endObject());
        index("fuu", "buu", "1", XContentFactory.jsonBuilder().startObject().field("fuu", "fuu").endObject());
        index("baz", "baz", "1", XContentFactory.jsonBuilder().startObject().field("baz", "baz").endObject());
        refreshAndWaitForReplication();
    }

    public void testRoutingTable() throws Exception {
        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().clear().setRoutingTable(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("foo"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("fuu"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("baz"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("non-existent"), is(false));

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("foo"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("fuu"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("baz"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("non-existent"), is(false));
    }

    public void testNodes() throws Exception {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        assertThat(clusterStateResponse.getState().nodes().getNodes().size(), is(cluster().size()));

        ClusterStateResponse clusterStateResponseFiltered = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponseFiltered.getState().nodes().getNodes().size(), is(0));
    }

    public void testMetadata() throws Exception {
        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().clear().setMetadata(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().metadata().indices().size(), is(3));

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(0));
    }

    public void testMetadataVersion() {
        createIndex("index-1");
        createIndex("index-2");
        long baselineVersion = client().admin().cluster().prepareState().get().getState().metadata().version();
        assertThat(baselineVersion, greaterThan(0L));
        assertThat(
            client().admin().cluster().prepareState().setIndices("index-1").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(
            client().admin().cluster().prepareState().setIndices("index-2").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(
            client().admin().cluster().prepareState().setIndices("*").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(
            client().admin().cluster().prepareState().setIndices("not-found").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(client().admin().cluster().prepareState().clear().setMetadata(false).get().getState().metadata().version(), equalTo(0L));
    }

    public void testIndexTemplates() throws Exception {
        client().admin()
            .indices()
            .preparePutTemplate("foo_template")
            .setPatterns(Collections.singletonList("te*"))
            .setOrder(0)
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "text")
                    .field("store", true)
                    .endObject()
                    .startObject("field2")
                    .field("type", "keyword")
                    .field("store", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        client().admin()
            .indices()
            .preparePutTemplate("fuu_template")
            .setPatterns(Collections.singletonList("test*"))
            .setOrder(1)
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("field2")
                    .field("type", "text")
                    .field("store", false)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponseUnfiltered.getState().metadata().templates().size(), is(greaterThanOrEqualTo(2)));

        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates("foo_template").get();
        assertIndexTemplateExists(getIndexTemplatesResponse, "foo_template");
    }

    public void testThatFilteringByIndexWorksForMetadataAndRoutingTable() throws Exception {
        testFilteringByIndexWorks(new String[] { "foo", "fuu", "non-existent" }, new String[] { "foo", "fuu" });
        testFilteringByIndexWorks(new String[] { "baz" }, new String[] { "baz" });
        testFilteringByIndexWorks(new String[] { "f*" }, new String[] { "foo", "fuu" });
        testFilteringByIndexWorks(new String[] { "b*" }, new String[] { "baz" });
        testFilteringByIndexWorks(new String[] { "*u" }, new String[] { "fuu" });

        String[] randomIndices = randomFrom(
            new String[] { "*" },
            new String[] { Metadata.ALL },
            Strings.EMPTY_ARRAY,
            new String[] { "f*", "b*" }
        );
        testFilteringByIndexWorks(randomIndices, new String[] { "foo", "fuu", "baz" });
    }

    /**
     * Retrieves the cluster state for the given indices and then checks
     * that the cluster state returns coherent data for both routing table and metadata.
     */
    private void testFilteringByIndexWorks(String[] indices, String[] expected) {
        ClusterStateResponse clusterState = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .setRoutingTable(true)
            .setIndices(indices)
            .get();

        final Map<String, IndexMetadata> metadata = clusterState.getState().getMetadata().indices();
        assertThat(metadata.size(), is(expected.length));

        RoutingTable routingTable = clusterState.getState().getRoutingTable();
        assertThat(routingTable.indicesRouting().size(), is(expected.length));

        for (final String expectedIndex : expected) {
            assertThat(metadata, hasKey(expectedIndex));
            assertThat(routingTable.hasIndex(expectedIndex), is(true));
        }
    }

    public void testLargeClusterStatePublishing() throws Exception {
        int estimatedBytesSize = scaledRandomIntBetween(
            ByteSizeValue.parseBytesSizeValue("10k", "estimatedBytesSize").bytesAsInt(),
            ByteSizeValue.parseBytesSizeValue("256k", "estimatedBytesSize").bytesAsInt()
        );
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        int counter = 0;
        int numberOfFields = 0;
        while (true) {
            mapping.startObject(UUIDs.randomBase64UUID()).field("type", "text").endObject();
            counter += 10; // each field is about 10 bytes, assuming compression in place
            numberOfFields++;
            if (counter > estimatedBytesSize) {
                break;
            }
        }
        logger.info("number of fields [{}], estimated bytes [{}]", numberOfFields, estimatedBytesSize);
        mapping.endObject().endObject();

        int numberOfShards = scaledRandomIntBetween(1, cluster().numDataNodes());
        // if the create index is ack'ed, then all nodes have successfully processed the cluster state
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
                )
                .setMapping(mapping)
                .setTimeout("60s")
                .get()
        );
        ensureGreen(); // wait for green state, so its both green, and there are no more pending events
        MappingMetadata clusterManagerMappingMetadata = client().admin()
            .indices()
            .prepareGetMappings("test")
            .get()
            .getMappings()
            .get("test");
        for (Client client : clients()) {
            MappingMetadata mappingMetadata = client.admin()
                .indices()
                .prepareGetMappings("test")
                .setLocal(true)
                .get()
                .getMappings()
                .get("test");
            assertThat(mappingMetadata.source().string(), equalTo(clusterManagerMappingMetadata.source().string()));
            assertThat(mappingMetadata, equalTo(clusterManagerMappingMetadata));
        }
    }

    public void testIndicesOptions() throws Exception {
        ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .setIndices("f*")
            .get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(2));
        ensureGreen("fuu");

        // close one index
        client().admin().indices().close(Requests.closeIndexRequest("fuu")).get();
        clusterStateResponse = client().admin().cluster().prepareState().clear().setMetadata(true).setIndices("f*").get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(1));
        assertThat(clusterStateResponse.getState().metadata().index("foo").getState(), equalTo(IndexMetadata.State.OPEN));

        // expand_wildcards_closed should toggle return only closed index fuu
        IndicesOptions expandCloseOptions = IndicesOptions.fromOptions(false, true, false, true);
        clusterStateResponse = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .setIndices("f*")
            .setIndicesOptions(expandCloseOptions)
            .get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(1));
        assertThat(clusterStateResponse.getState().metadata().index("fuu").getState(), equalTo(IndexMetadata.State.CLOSE));

        // ignore_unavailable set to true should not raise exception on fzzbzz
        IndicesOptions ignoreUnavailabe = IndicesOptions.fromOptions(true, true, true, false);
        clusterStateResponse = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .setIndices("fzzbzz")
            .setIndicesOptions(ignoreUnavailabe)
            .get();
        assertThat(clusterStateResponse.getState().metadata().indices().isEmpty(), is(true));

        // empty wildcard expansion result should work when allowNoIndices is
        // turned on
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, true, true, false);
        clusterStateResponse = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .setIndices("a*")
            .setIndicesOptions(allowNoIndices)
            .get();
        assertThat(clusterStateResponse.getState().metadata().indices().isEmpty(), is(true));
    }

    public void testIndicesOptionsOnAllowNoIndicesFalse() throws Exception {
        // empty wildcard expansion throws exception when allowNoIndices is turned off
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, false, true, false);
        try {
            client().admin().cluster().prepareState().clear().setMetadata(true).setIndices("a*").setIndicesOptions(allowNoIndices).get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index [a*]"));
        }
    }

    public void testIndicesIgnoreUnavailableFalse() throws Exception {
        // ignore_unavailable set to false throws exception when allowNoIndices is turned off
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, true, true, false);
        try {
            client().admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setIndices("fzzbzz")
                .setIndicesOptions(allowNoIndices)
                .get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index [fzzbzz]"));
        }
    }

    public void testPrivateCustomsAreExcluded() throws Exception {
        // ensure that the custom is injected into the cluster state
        assertBusy(() -> assertTrue(clusterService().state().customs().containsKey("test")));
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().setCustoms(true).get();
        assertFalse(clusterStateResponse.getState().customs().containsKey("test"));
    }

    private static class TestCustom extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

        private final int value;

        TestCustom(int value) {
            this.value = value;
        }

        TestCustom(StreamInput in) throws IOException {
            this.value = in.readInt();
        }

        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(ClusterState.Custom.class, "test", in);
        }

        @Override
        public boolean isPrivate() {
            return true;
        }
    }

    public static class PrivateCustomPlugin extends Plugin implements ClusterPlugin {

        public PrivateCustomPlugin() {}

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, "test", TestCustom::new));
            entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, "test", TestCustom::readDiffFrom));
            return entries;
        }

        private final AtomicBoolean installed = new AtomicBoolean();

        @Override
        public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry,
            final IndexNameExpressionResolver expressionResolver,
            final Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            clusterService.addListener(event -> {
                final ClusterState state = event.state();
                if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                    return;
                }

                if (state.nodes().isLocalNodeElectedClusterManager()) {
                    if (state.custom("test") == null) {
                        if (installed.compareAndSet(false, true)) {
                            clusterService.submitStateUpdateTask("install-metadata-custom", new ClusterStateUpdateTask(Priority.URGENT) {

                                @Override
                                public ClusterState execute(ClusterState currentState) {
                                    if (currentState.custom("test") == null) {
                                        final ClusterState.Builder builder = ClusterState.builder(currentState);
                                        builder.putCustom("test", new TestCustom(42));
                                        return builder.build();
                                    } else {
                                        return currentState;
                                    }
                                }

                                @Override
                                public void onFailure(String source, Exception e) {
                                    throw new AssertionError(e);
                                }

                            });
                        }
                    }
                }

            });
            return Collections.emptyList();
        }
    }
}
