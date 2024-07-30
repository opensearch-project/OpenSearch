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

package org.opensearch.gateway;

import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ElectionSchedulerFactory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.TransportNodesGatewayStartedShardHelper.GatewayStartedShard;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.store.ShardAttributes;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadataHelper;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.InternalTestCluster.RestartCallback;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.store.MockFSIndexStore;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.cluster.coordination.ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING;
import static org.opensearch.cluster.health.ClusterHealthStatus.GREEN;
import static org.opensearch.cluster.health.ClusterHealthStatus.RED;
import static org.opensearch.cluster.health.ClusterHealthStatus.YELLOW;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.gateway.GatewayRecoveryTestUtils.corruptShard;
import static org.opensearch.gateway.GatewayRecoveryTestUtils.getDiscoveryNodes;
import static org.opensearch.gateway.GatewayRecoveryTestUtils.prepareRequestMap;
import static org.opensearch.gateway.GatewayService.RECOVER_AFTER_NODES_SETTING;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(numDataNodes = 0, scope = Scope.TEST)
public class RecoveryFromGatewayIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockFSIndexStore.TestPlugin.class, InternalSettingsPlugin.class);
    }

    public void testOneNodeRecoverFromGateway() throws Exception {

        internalCluster().startNode();

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("appAccountIds")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertAcked(prepareCreate("test").setMapping(mapping));

        client().prepareIndex("test")
            .setId("10990239")
            .setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).value(179).endArray().endObject())
            .execute()
            .actionGet();
        client().prepareIndex("test")
            .setId("10990473")
            .setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).endArray().endObject())
            .execute()
            .actionGet();
        client().prepareIndex("test")
            .setId("10990513")
            .setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).value(179).endArray().endObject())
            .execute()
            .actionGet();
        client().prepareIndex("test")
            .setId("10990695")
            .setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).endArray().endObject())
            .execute()
            .actionGet();
        client().prepareIndex("test")
            .setId("11026351")
            .setSource(jsonBuilder().startObject().startArray("appAccountIds").value(14).endArray().endObject())
            .execute()
            .actionGet();

        refresh();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
        ensureYellow("test"); // wait for primary allocations here otherwise if we have a lot of shards we might have a
        // shard that is still in post recovery when we restart and the ensureYellow() below will timeout

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);
        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        client().admin().indices().prepareRefresh().execute().actionGet();
        assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("appAccountIds", 179)).execute().actionGet(), 2);
    }

    private Map<String, long[]> assertAndCapturePrimaryTerms(Map<String, long[]> previousTerms) {
        if (previousTerms == null) {
            previousTerms = new HashMap<>();
        }
        final Map<String, long[]> result = new HashMap<>();
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        for (final IndexMetadata indexMetadata : state.metadata().indices().values()) {
            final String index = indexMetadata.getIndex().getName();
            final long[] previous = previousTerms.get(index);
            final long[] current = IntStream.range(0, indexMetadata.getNumberOfShards()).mapToLong(indexMetadata::primaryTerm).toArray();
            if (previous == null) {
                result.put(index, current);
            } else {
                assertThat("number of terms changed for index [" + index + "]", current.length, equalTo(previous.length));
                for (int shard = 0; shard < current.length; shard++) {
                    assertThat(
                        "primary term didn't increase for [" + index + "][" + shard + "]",
                        current[shard],
                        greaterThan(previous[shard])
                    );
                }
                result.put(index, current);
            }
        }

        return result;
    }

    public void testSingleNodeNoFlush() throws Exception {
        internalCluster().startNode();

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .endObject()
            .startObject("num")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        // note: default replica settings are tied to #data nodes-1 which is 0 here. We can do with 1 in this test.
        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards()).put(SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 1))
            ).setMapping(mapping)
        );

        int value1Docs;
        int value2Docs;
        boolean indexToAllShards = randomBoolean();

        if (indexToAllShards) {
            // insert enough docs so all shards will have a doc
            value1Docs = randomIntBetween(numberOfShards * 10, numberOfShards * 20);
            value2Docs = randomIntBetween(numberOfShards * 10, numberOfShards * 20);

        } else {
            // insert a two docs, some shards will not have anything
            value1Docs = 1;
            value2Docs = 1;
        }

        for (int i = 0; i < 1 + randomInt(100); i++) {
            for (int id = 0; id < Math.max(value1Docs, value2Docs); id++) {
                if (id < value1Docs) {
                    index(
                        "test",
                        "type1",
                        "1_" + id,
                        jsonBuilder().startObject().field("field", "value1").startArray("num").value(14).value(179).endArray().endObject()
                    );
                }
                if (id < value2Docs) {
                    index(
                        "test",
                        "type1",
                        "2_" + id,
                        jsonBuilder().startObject().field("field", "value2").startArray("num").value(14).endArray().endObject()
                    );
                }
            }

        }

        refresh();

        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }
        if (!indexToAllShards) {
            // we have to verify primaries are started for them to be restored
            logger.info("Ensure all primaries have been started");
            ensureYellow();
        }

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i <= randomInt(10); i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), value1Docs + value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value1")).get(), value1Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("field", "value2")).get(), value2Docs);
            assertHitCount(client().prepareSearch().setSize(0).setQuery(termQuery("num", 179)).get(), value1Docs);
        }
    }

    public void testSingleNodeWithFlush() throws Exception {
        internalCluster().startNode();
        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("field", "value1").endObject())
            .execute()
            .actionGet();
        flush();
        client().prepareIndex("test")
            .setId("2")
            .setSource(jsonBuilder().startObject().field("field", "value2").endObject())
            .execute()
            .actionGet();
        refresh();

        assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);

        ensureYellow("test"); // wait for primary allocations here otherwise if we have a lot of shards we might have a
        // shard that is still in post recovery when we restart and the ensureYellow() below will timeout

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        internalCluster().fullRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }
    }

    public void testTwoNodeFirstNodeCleared() throws Exception {
        final String firstNode = internalCluster().startNode();
        internalCluster().startNode();

        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("field", "value1").endObject())
            .execute()
            .actionGet();
        flush();
        client().prepareIndex("test")
            .setId("2")
            .setSource(jsonBuilder().startObject().field("field", "value2").endObject())
            .execute()
            .actionGet();
        refresh();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        Map<String, long[]> primaryTerms = assertAndCapturePrimaryTerms(null);

        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(firstNode)).get();

        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .put(RECOVER_AFTER_NODES_SETTING.getKey(), 2)
                    .putList(INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey()) // disable bootstrapping
                    .build();
            }

            @Override
            public boolean clearData(String nodeName) {
                return firstNode.equals(nodeName);
            }

        });

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureGreen();
        primaryTerms = assertAndCapturePrimaryTerms(primaryTerms);

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        client().execute(ClearVotingConfigExclusionsAction.INSTANCE, new ClearVotingConfigExclusionsRequest()).get();
    }

    public void testLatestVersionLoaded() throws Exception {
        // clean two nodes
        List<String> nodes = internalCluster().startNodes(2, Settings.builder().put("gateway.recover_after_nodes", 2).build());
        Settings node1DataPathSettings = internalCluster().dataPathSettings(nodes.get(0));
        Settings node2DataPathSettings = internalCluster().dataPathSettings(nodes.get(1));

        assertAcked(client().admin().indices().prepareCreate("test"));
        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("field", "value1").endObject())
            .execute()
            .actionGet();
        client().admin().indices().prepareFlush().execute().actionGet();
        client().prepareIndex("test")
            .setId("2")
            .setSource(jsonBuilder().startObject().field("field", "value2").endObject())
            .execute()
            .actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 2);
        }

        String metadataUuid = client().admin().cluster().prepareState().execute().get().getState().getMetadata().clusterUUID();
        assertThat(metadataUuid, not(equalTo("_na_")));

        logger.info("--> closing first node, and indexing more data to the second node");
        internalCluster().stopRandomDataNode();

        logger.info("--> one node is closed - start indexing data into the second one");
        client().prepareIndex("test")
            .setId("3")
            .setSource(jsonBuilder().startObject().field("field", "value3").endObject())
            .execute()
            .actionGet();
        // TODO: remove once refresh doesn't fail immediately if there a cluster-manager block:
        // https://github.com/elastic/elasticsearch/issues/9997
        // client().admin().cluster().prepareHealth("test").setWaitForYellowStatus().get();
        logger.info("--> refreshing all indices after indexing is complete");
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> checking if documents exist, there should be 3");
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        logger.info("--> add some metadata and additional template");
        client().admin()
            .indices()
            .preparePutTemplate("template_1")
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
            .execute()
            .actionGet();
        client().admin()
            .indices()
            .prepareAliases()
            .addAlias("test", "test_alias", QueryBuilders.termQuery("field", "value"))
            .execute()
            .actionGet();

        logger.info("--> stopping the second node");
        internalCluster().stopRandomDataNode();

        logger.info("--> starting the two nodes back");

        internalCluster().startNodes(
            Settings.builder().put(node1DataPathSettings).put("gateway.recover_after_nodes", 2).build(),
            Settings.builder().put(node2DataPathSettings).put("gateway.recover_after_nodes", 2).build()
        );

        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        assertThat(client().admin().cluster().prepareState().execute().get().getState().getMetadata().clusterUUID(), equalTo(metadataUuid));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).execute().actionGet(), 3);
        }

        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.metadata().templates().get("template_1").patterns(), equalTo(Collections.singletonList("te*")));
        assertThat(state.metadata().index("test").getAliases().get("test_alias"), notNullValue());
        assertThat(state.metadata().index("test").getAliases().get("test_alias").filter(), notNullValue());
    }

    public void testReuseInFileBasedPeerRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode(nodeSettings(0));

        // create the index with our mapping
        client(primaryNode).admin()
            .indices()
            .prepareCreate("test")
            .setSettings(
                Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", 1)

                    // disable merges to keep segments the same
                    .put(MergePolicyProvider.INDEX_MERGE_ENABLED, false)

                    // expire retention leases quickly
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            )
            .get();

        logger.info("--> indexing docs");
        int numDocs = randomIntBetween(1, 1024);
        for (int i = 0; i < numDocs; i++) {
            client(primaryNode).prepareIndex("test").setSource("field", "value").execute().actionGet();
        }

        client(primaryNode).admin().indices().prepareFlush("test").setForce(true).get();

        // start the replica node; we do this after indexing so a file-based recovery is triggered to ensure the files are identical
        final String replicaNode = internalCluster().startDataOnlyNode(nodeSettings(1));
        ensureGreen();

        final RecoveryResponse initialRecoveryReponse = client().admin().indices().prepareRecoveries("test").get();
        final Set<String> files = new HashSet<>();
        for (final RecoveryState recoveryState : initialRecoveryReponse.shardRecoveryStates().get("test")) {
            if (recoveryState.getTargetNode().getName().equals(replicaNode)) {
                for (final ReplicationLuceneIndex.FileMetadata file : recoveryState.getIndex().fileDetails()) {
                    files.add(file.name());
                }
                break;
            }
        }

        logger.info("--> restart replica node");
        boolean softDeleteEnabled = internalCluster().getInstance(IndicesService.class, primaryNode)
            .indexServiceSafe(resolveIndex("test"))
            .getShard(0)
            .indexSettings()
            .isSoftDeleteEnabled();

        int moreDocs = randomIntBetween(1, 1024);
        internalCluster().restartNode(replicaNode, new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // index some more documents; we expect to reuse the files that already exist on the replica
                for (int i = 0; i < moreDocs; i++) {
                    client(primaryNode).prepareIndex("test").setSource("field", "value").execute().actionGet();
                }

                // prevent a sequence-number-based recovery from being possible
                client(primaryNode).admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(
                        Settings.builder()
                            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), "-1")
                            .put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), "-1")
                            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)
                            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), "0s")
                    )
                    .get();
                assertBusy(
                    () -> assertThat(
                        client().admin().indices().prepareStats("test").get().getShards()[0].getRetentionLeaseStats()
                            .retentionLeases()
                            .leases()
                            .size(),
                        equalTo(1)
                    )
                );
                client().admin().indices().prepareFlush("test").setForce(true).get();
                if (softDeleteEnabled) { // We need an extra flush to advance the min_retained_seqno of the SoftDeletesPolicy
                    client().admin().indices().prepareFlush("test").setForce(true).get();
                }
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen();

        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
        for (final RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            long recovered = 0;
            long reused = 0;
            int filesRecovered = 0;
            int filesReused = 0;
            for (final ReplicationLuceneIndex.FileMetadata file : recoveryState.getIndex().fileDetails()) {
                if (files.contains(file.name()) == false) {
                    recovered += file.length();
                    filesRecovered++;
                } else {
                    reused += file.length();
                    filesReused++;
                }
            }
            if (recoveryState.getPrimary()) {
                assertThat(recoveryState.getIndex().recoveredBytes(), equalTo(0L));
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes()));
                assertThat(recoveryState.getIndex().recoveredFileCount(), equalTo(0));
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount()));
            } else {
                logger.info(
                    "--> replica shard {} recovered from {} to {}, recovered {}, reuse {}",
                    recoveryState.getShardId().getId(),
                    recoveryState.getSourceNode().getName(),
                    recoveryState.getTargetNode().getName(),
                    recoveryState.getIndex().recoveredBytes(),
                    recoveryState.getIndex().reusedBytes()
                );
                assertThat("bytes should have been recovered", recoveryState.getIndex().recoveredBytes(), equalTo(recovered));
                assertThat("data should have been reused", recoveryState.getIndex().reusedBytes(), greaterThan(0L));
                // we have to recover the segments file since we commit the translog ID on engine startup
                assertThat(
                    "all existing files should be reused, byte count mismatch",
                    recoveryState.getIndex().reusedBytes(),
                    equalTo(reused)
                );
                assertThat(recoveryState.getIndex().reusedBytes(), equalTo(recoveryState.getIndex().totalBytes() - recovered));
                assertThat(
                    "the segment from the last round of indexing should be recovered",
                    recoveryState.getIndex().recoveredFileCount(),
                    equalTo(filesRecovered)
                );
                assertThat(
                    "all existing files should be reused, file count mismatch",
                    recoveryState.getIndex().reusedFileCount(),
                    equalTo(filesReused)
                );
                assertThat(recoveryState.getIndex().reusedFileCount(), equalTo(recoveryState.getIndex().totalFileCount() - filesRecovered));
                assertThat("> 0 files should be reused", recoveryState.getIndex().reusedFileCount(), greaterThan(0));
                assertThat("no translog ops should be recovered", recoveryState.getTranslog().recoveredOperations(), equalTo(0));
            }
        }
    }

    public void assertSyncIdsNotNull() {
        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    public void testStartedShardFoundIfStateNotYetProcessed() throws Exception {
        // nodes may need to report the shards they processed the initial recovered cluster state from the cluster-manager
        final String nodeName = internalCluster().startNode();
        createIndex("test", Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).build());
        final String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(
            client().admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test")
        );
        final Index index = resolveIndex("test");
        final ShardId shardId = new ShardId(index, 0);
        index("test", "type", "1");
        flush("test");

        final boolean corrupt = randomBoolean();

        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // make sure state is not recovered
                return Settings.builder().put(RECOVER_AFTER_NODES_SETTING.getKey(), 2).build();
            }
        });

        if (corrupt) {
            for (Path path : internalCluster().getInstance(NodeEnvironment.class, nodeName).availableShardPaths(shardId)) {
                final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
                if (Files.exists(indexPath)) { // multi data path might only have one path in use
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                        for (Path item : stream) {
                            if (item.getFileName().toString().startsWith("segments_")) {
                                logger.debug("--> deleting [{}]", item);
                                Files.delete(item);
                            }
                        }
                    }
                }

            }
        }

        DiscoveryNode node = internalCluster().getInstance(ClusterService.class, nodeName).localNode();

        TransportNodesListGatewayStartedShards.NodesGatewayStartedShards response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShards.class),
            new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, new DiscoveryNode[] { node })
        );

        assertThat(response.getNodes(), hasSize(1));
        assertThat(response.getNodes().get(0).getGatewayShardStarted().allocationId(), notNullValue());
        if (corrupt) {
            assertThat(response.getNodes().get(0).getGatewayShardStarted().storeException(), notNullValue());
        } else {
            assertThat(response.getNodes().get(0).getGatewayShardStarted().storeException(), nullValue());
        }

        // start another node so cluster consistency checks won't time out due to the lack of state
        internalCluster().startNode();
    }

    public void testMessyElectionsStillMakeClusterGoGreen() throws Exception {
        internalCluster().startNodes(
            3,
            Settings.builder().put(ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING.getKey(), "2ms").build()
        );
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .build()
        );
        ensureGreen("test");
        internalCluster().fullRestart();
        ensureGreen("test");
    }

    public void testBatchModeEnabledWithoutTimeout() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true).build()
        );
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(2);
        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureGreen("test");
        Settings node0DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(0));
        Settings node1DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(1));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(1)));
        ensureRed("test");
        ensureStableCluster(1);

        logger.info("--> Now do a protective reroute");
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        ShardsBatchGatewayAllocator gatewayAllocator = internalCluster().getInstance(
            ShardsBatchGatewayAllocator.class,
            internalCluster().getClusterManagerName()
        );
        assertTrue(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(internalCluster().clusterService().getSettings()));
        assertEquals(1, gatewayAllocator.getNumberOfStartedShardBatches());
        // Replica shard would be marked ineligible since there are no data nodes.
        // It would then be removed from any batch and batches would get deleted, so we would have 0 replica batches
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());

        // Now start one data node
        logger.info("--> restarting the first stopped node");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(0)).put(node0DataPathSettings).build());
        ensureStableCluster(2);
        ensureYellow("test");
        assertEquals(0, gatewayAllocator.getNumberOfStartedShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfInFlightFetches());

        // calling reroute and asserting on reroute response
        logger.info("--> calling reroute while cluster is yellow");
        clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        // Now start last data node and ensure batch mode is working and cluster goes green
        logger.info("--> restarting the second stopped node");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(1)).put(node1DataPathSettings).build());
        ensureStableCluster(3);
        ensureGreen("test");
        assertEquals(0, gatewayAllocator.getNumberOfStartedShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfInFlightFetches());
    }

    public void testBatchModeEnabledWithSufficientTimeoutAndClusterGreen() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder()
                .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true)
                .put(ShardsBatchGatewayAllocator.PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING.getKey(), "20s")
                .put(ShardsBatchGatewayAllocator.REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING.getKey(), "20s")
                .build()
        );
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(2);
        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        ensureGreen("test");
        Settings node0DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(0));
        Settings node1DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(1));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(1)));
        ensureRed("test");
        ensureStableCluster(1);

        logger.info("--> Now do a protective reroute");
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        ShardsBatchGatewayAllocator gatewayAllocator = internalCluster().getInstance(
            ShardsBatchGatewayAllocator.class,
            internalCluster().getClusterManagerName()
        );
        assertTrue(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(internalCluster().clusterService().getSettings()));
        assertEquals(1, gatewayAllocator.getNumberOfStartedShardBatches());
        // Replica shard would be marked ineligible since there are no data nodes.
        // It would then be removed from any batch and batches would get deleted, so we would have 0 replica batches
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());

        // Now start one data nodes and ensure batch mode is working
        logger.info("--> restarting the first stopped node");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(0)).put(node0DataPathSettings).build());
        ensureStableCluster(2);
        ensureYellow("test");
        assertEquals(0, gatewayAllocator.getNumberOfStartedShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfInFlightFetches());

        // calling reroute and asserting on reroute response
        logger.info("--> calling reroute while cluster is yellow");
        clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        // Now start last data node and ensure batch mode is working and cluster goes green
        logger.info("--> restarting the second stopped node");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(1)).put(node1DataPathSettings).build());
        ensureStableCluster(3);
        ensureGreen("test");
        assertEquals(0, gatewayAllocator.getNumberOfStartedShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfInFlightFetches());
    }

    public void testBatchModeEnabledWithInSufficientTimeoutButClusterGreen() throws Exception {

        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true).build()
        );
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(2);
        createNIndices(50, "test"); // this will create 50p, 50r shards
        ensureStableCluster(3);
        IndicesStatsResponse indicesStats = dataNodeClient().admin().indices().prepareStats().get();
        assertThat(indicesStats.getSuccessfulShards(), equalTo(100));
        ClusterHealthResponse health = client().admin()
            .cluster()
            .health(Requests.clusterHealthRequest().waitForGreenStatus().timeout("1m"))
            .actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(GREEN, health.getStatus());

        String clusterManagerName = internalCluster().getClusterManagerName();
        Settings clusterManagerDataPathSettings = internalCluster().dataPathSettings(clusterManagerName);
        Settings node0DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(0));
        Settings node1DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(1));

        internalCluster().stopCurrentClusterManagerNode();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(1)));

        // Now start cluster manager node and post that verify batches created
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder()
                .put("node.name", clusterManagerName)
                .put(clusterManagerDataPathSettings)
                .put(ShardsBatchGatewayAllocator.GATEWAY_ALLOCATOR_BATCH_SIZE.getKey(), 5)
                .put(ShardsBatchGatewayAllocator.PRIMARY_BATCH_ALLOCATOR_TIMEOUT_SETTING.getKey(), "10ms")
                .put(ShardsBatchGatewayAllocator.REPLICA_BATCH_ALLOCATOR_TIMEOUT_SETTING.getKey(), "10ms")
                .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true)
                .build()
        );
        ensureStableCluster(1);

        logger.info("--> Now do a protective reroute"); // to avoid any race condition in test
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        ShardsBatchGatewayAllocator gatewayAllocator = internalCluster().getInstance(
            ShardsBatchGatewayAllocator.class,
            internalCluster().getClusterManagerName()
        );

        assertTrue(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(internalCluster().clusterService().getSettings()));
        assertEquals(10, gatewayAllocator.getNumberOfStartedShardBatches());
        // All replica shards would be marked ineligible since there are no data nodes.
        // They would then be removed from any batch and batches would get deleted, so we would have 0 replica batches
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
        health = client(internalCluster().getClusterManagerName()).admin().cluster().health(Requests.clusterHealthRequest()).actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(RED, health.getStatus());
        assertEquals(100, health.getUnassignedShards());
        assertEquals(0, health.getInitializingShards());
        assertEquals(0, health.getActiveShards());
        assertEquals(0, health.getRelocatingShards());
        assertEquals(0, health.getNumberOfDataNodes());

        // Now start both data nodes and ensure batch mode is working
        logger.info("--> restarting the stopped nodes");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(0)).put(node0DataPathSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(1)).put(node1DataPathSettings).build());
        ensureStableCluster(3);

        // wait for cluster to turn green
        health = client().admin().cluster().health(Requests.clusterHealthRequest().waitForGreenStatus().timeout("5m")).actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(GREEN, health.getStatus());
        assertEquals(0, health.getUnassignedShards());
        assertEquals(0, health.getInitializingShards());
        assertEquals(100, health.getActiveShards());
        assertEquals(0, health.getRelocatingShards());
        assertEquals(2, health.getNumberOfDataNodes());
        assertEquals(0, gatewayAllocator.getNumberOfStartedShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
    }

    public void testBatchModeDisabled() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), false).build()
        );
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(2);
        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        ensureGreen("test");
        Settings node0DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(0));
        Settings node1DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(1));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(1)));
        ensureStableCluster(1);

        logger.info("--> Now do a protective reroute");
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        ShardsBatchGatewayAllocator gatewayAllocator = internalCluster().getInstance(
            ShardsBatchGatewayAllocator.class,
            internalCluster().getClusterManagerName()
        );
        ensureRed("test");

        assertFalse(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(internalCluster().clusterService().getSettings()));

        // assert no batches created
        assertEquals(0, gatewayAllocator.getNumberOfStartedShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());

        logger.info("--> restarting the stopped nodes");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(0)).put(node0DataPathSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(1)).put(node1DataPathSettings).build());
        ensureStableCluster(3);
        ensureGreen("test");
    }

    public void testMultipleReplicaShardAssignmentWithDelayedAllocationAndDifferentNodeStartTimeInBatchMode() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true).build()
        );
        internalCluster().startDataOnlyNodes(6);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3)
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "60m")
                .build()
        );
        ensureGreen("test");

        List<String> nodesWithReplicaShards = findNodesWithShard(false);
        Settings replicaNode0DataPathSettings = internalCluster().dataPathSettings(nodesWithReplicaShards.get(0));
        Settings replicaNode1DataPathSettings = internalCluster().dataPathSettings(nodesWithReplicaShards.get(1));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodesWithReplicaShards.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodesWithReplicaShards.get(1)));

        ensureStableCluster(5);

        logger.info("--> explicitly triggering reroute");
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        ClusterHealthResponse health = client().admin().cluster().health(Requests.clusterHealthRequest().timeout("5m")).actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(YELLOW, health.getStatus());
        assertEquals(2, health.getUnassignedShards());
        // shard should be unassigned because of Allocation_Delayed
        BooleanSupplier delayedShardAllocationStatusVerificationSupplier = () -> AllocationDecision.ALLOCATION_DELAYED.equals(
            client().admin()
                .cluster()
                .prepareAllocationExplain()
                .setIndex("test")
                .setShard(0)
                .setPrimary(false)
                .get()
                .getExplanation()
                .getShardAllocationDecision()
                .getAllocateDecision()
                .getAllocationDecision()
        );
        waitUntil(delayedShardAllocationStatusVerificationSupplier, 2, TimeUnit.MINUTES);

        logger.info("--> restarting the node 1");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.name", nodesWithReplicaShards.get(0)).put(replicaNode0DataPathSettings).build()
        );
        clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());
        ensureStableCluster(6);
        waitUntil(
            () -> client().admin().cluster().health(Requests.clusterHealthRequest().timeout("5m")).actionGet().getActiveShards() == 3,
            2,
            TimeUnit.MINUTES
        );
        health = client().admin().cluster().health(Requests.clusterHealthRequest().timeout("5m")).actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(YELLOW, health.getStatus());
        assertEquals(1, health.getUnassignedShards());
        assertEquals(1, health.getDelayedUnassignedShards());
        waitUntil(delayedShardAllocationStatusVerificationSupplier, 2, TimeUnit.MINUTES);
        logger.info("--> restarting the node 0");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.name", nodesWithReplicaShards.get(1)).put(replicaNode1DataPathSettings).build()
        );
        ensureStableCluster(7);
        ensureGreen("test");
    }

    public void testAllocationExplainReturnsNoWhenExtraReplicaShardInNonBatchMode() throws Exception {
        // Non batch mode - This test is to validate that we don't return AWAITING_INFO in allocation explain API when the deciders are
        // returning NO
        this.allocationExplainReturnsNoWhenExtraReplicaShard(false);
    }

    public void testAllocationExplainReturnsNoWhenExtraReplicaShardInBatchMode() throws Exception {
        // Batch mode - This test is to validate that we don't return AWAITING_INFO in allocation explain API when the deciders are
        // returning NO
        this.allocationExplainReturnsNoWhenExtraReplicaShard(true);
    }

    public void testNBatchesCreationAndAssignment() throws Exception {
        // we will reduce batch size to 5 to make sure we have enough batches to test assignment
        // Total number of primary shards = 50 (50 indices*1)
        // Total number of replica shards = 50 (50 indices*1)
        // Total batches creation for primaries and replicas will be 10 each

        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true).build()
        );
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(2);
        createNIndices(50, "test");
        ensureStableCluster(3);
        IndicesStatsResponse indicesStats = dataNodeClient().admin().indices().prepareStats().get();
        assertThat(indicesStats.getSuccessfulShards(), equalTo(100));
        ClusterHealthResponse health = client().admin()
            .cluster()
            .health(Requests.clusterHealthRequest().waitForGreenStatus().timeout("1m"))
            .actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(GREEN, health.getStatus());

        String clusterManagerName = internalCluster().getClusterManagerName();
        Settings clusterManagerDataPathSettings = internalCluster().dataPathSettings(clusterManagerName);
        Settings node0DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(0));
        Settings node1DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(1));

        internalCluster().stopCurrentClusterManagerNode();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(1)));

        // Now start cluster manager node and post that verify batches created
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder()
                .put("node.name", clusterManagerName)
                .put(clusterManagerDataPathSettings)
                .put(ShardsBatchGatewayAllocator.GATEWAY_ALLOCATOR_BATCH_SIZE.getKey(), 5)
                .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true)
                .build()
        );
        ensureStableCluster(1);

        logger.info("--> Now do a protective reroute"); // to avoid any race condition in test
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        ShardsBatchGatewayAllocator gatewayAllocator = internalCluster().getInstance(
            ShardsBatchGatewayAllocator.class,
            internalCluster().getClusterManagerName()
        );
        assertTrue(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(internalCluster().clusterService().getSettings()));
        assertEquals(10, gatewayAllocator.getNumberOfStartedShardBatches());
        // All replica shards would be marked ineligible since there are no data nodes.
        // They would then be removed from any batch and batches would get deleted, so we would have 0 replica batches
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
        health = client(internalCluster().getClusterManagerName()).admin().cluster().health(Requests.clusterHealthRequest()).actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(RED, health.getStatus());
        assertEquals(100, health.getUnassignedShards());
        assertEquals(0, health.getInitializingShards());
        assertEquals(0, health.getActiveShards());
        assertEquals(0, health.getRelocatingShards());
        assertEquals(0, health.getNumberOfDataNodes());

        // Now start both data nodes and ensure batch mode is working
        logger.info("--> restarting the stopped nodes");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(0)).put(node0DataPathSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(1)).put(node1DataPathSettings).build());
        ensureStableCluster(3);

        // wait for cluster to turn green
        health = client().admin().cluster().health(Requests.clusterHealthRequest().waitForGreenStatus().timeout("5m")).actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(GREEN, health.getStatus());
        assertEquals(0, health.getUnassignedShards());
        assertEquals(0, health.getInitializingShards());
        assertEquals(100, health.getActiveShards());
        assertEquals(0, health.getRelocatingShards());
        assertEquals(2, health.getNumberOfDataNodes());
        assertEquals(0, gatewayAllocator.getNumberOfStartedShardBatches());
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
    }

    public void testCulpritShardInBatch() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true).build()
        );
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(3);
        createNIndices(4, "test");
        ensureStableCluster(4);
        ClusterHealthResponse health = client().admin()
            .cluster()
            .health(Requests.clusterHealthRequest().waitForGreenStatus().timeout("5m"))
            .actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(GREEN, health.getStatus());
        assertEquals(8, health.getActiveShards());

        String culpritShardIndexName = "test0";
        Index idx = resolveIndex(culpritShardIndexName);
        for (String node : internalCluster().nodesInclude(culpritShardIndexName)) {
            IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexShards = indexServices.indexServiceSafe(idx);
            Integer shardId = 0;
            IndexShard shard = indexShards.getShard(0);
            logger.debug("--> failing shard [{}] on node [{}]", shardId, node);
            shard.failShard("test", new CorruptIndexException("test corrupted", ""));
            logger.debug("--> failed shard [{}] on node [{}]", shardId, node);
        }

        String clusterManagerName = internalCluster().getClusterManagerName();
        Settings clusterManagerDataPathSettings = internalCluster().dataPathSettings(clusterManagerName);
        Settings node0DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(0));
        Settings node1DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(1));
        Settings node2DataPathSettings = internalCluster().dataPathSettings(dataOnlyNodes.get(2));

        internalCluster().stopCurrentClusterManagerNode();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(1)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataOnlyNodes.get(2)));

        // Now start cluster manager node and post that verify batches created
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder()
                .put("node.name", clusterManagerName)
                .put(clusterManagerDataPathSettings)
                .put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true)
                .build()
        );
        ensureStableCluster(1);

        logger.info("--> Now do a protective reroute"); // to avoid any race condition in test
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        ShardsBatchGatewayAllocator gatewayAllocator = internalCluster().getInstance(
            ShardsBatchGatewayAllocator.class,
            internalCluster().getClusterManagerName()
        );
        assertTrue(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(internalCluster().clusterService().getSettings()));
        assertEquals(1, gatewayAllocator.getNumberOfStartedShardBatches());
        // Replica shard would be marked ineligible since there are no data nodes.
        // It would then be removed from any batch and batches would get deleted, so we would have 0 replica batches
        assertEquals(0, gatewayAllocator.getNumberOfStoreShardBatches());
        assertTrue(clusterRerouteResponse.isAcknowledged());
        health = client(internalCluster().getClusterManagerName()).admin().cluster().health(Requests.clusterHealthRequest()).actionGet();
        assertFalse(health.isTimedOut());
        assertEquals(RED, health.getStatus());
        assertEquals(8, health.getUnassignedShards());
        assertEquals(0, health.getInitializingShards());
        assertEquals(0, health.getActiveShards());
        assertEquals(0, health.getRelocatingShards());
        assertEquals(0, health.getNumberOfDataNodes());

        logger.info("--> restarting the stopped nodes");
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(0)).put(node0DataPathSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(1)).put(node1DataPathSettings).build());
        internalCluster().startDataOnlyNode(Settings.builder().put("node.name", dataOnlyNodes.get(2)).put(node2DataPathSettings).build());
        ensureStableCluster(4);

        health = client().admin().cluster().health(Requests.clusterHealthRequest().waitForGreenStatus().timeout("1m")).actionGet();

        assertEquals(RED, health.getStatus());
        assertTrue(health.isTimedOut());
        assertEquals(0, health.getNumberOfPendingTasks());
        assertEquals(0, health.getNumberOfInFlightFetch());
        assertEquals(6, health.getActiveShards());
        assertEquals(2, health.getUnassignedShards());
        assertEquals(0, health.getInitializingShards());
        assertEquals(0, health.getRelocatingShards());
        assertEquals(3, health.getNumberOfDataNodes());
    }

    private void createNIndices(int n, String prefix) {

        for (int i = 0; i < n; i++) {
            createIndex(
                prefix + i,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
            );
            // index doc2
            client().prepareIndex(prefix + i).setId("1").setSource("foo", "bar").get();

            // index doc 2
            client().prepareIndex(prefix + i).setId("2").setSource("foo2", "bar2").get();
            ensureGreen(prefix + i);
        }
    }

    public void testSingleShardFetchUsingBatchAction() {
        String indexName = "test";
        int numOfShards = 1;
        prepareIndex(indexName, numOfShards);
        Map<ShardId, ShardAttributes> shardIdShardAttributesMap = prepareRequestMap(new String[] { indexName }, numOfShards);

        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName).get();

        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(searchShardsResponse.getNodes(), shardIdShardAttributesMap)
        );
        final Index index = resolveIndex(indexName);
        final ShardId shardId = new ShardId(index, 0);
        GatewayStartedShard gatewayStartedShard = response.getNodesMap()
            .get(searchShardsResponse.getNodes()[0].getId())
            .getNodeGatewayStartedShardsBatch()
            .get(shardId);
        assertNodeGatewayStartedShardsHappyCase(gatewayStartedShard);
    }

    public void testShardFetchMultiNodeMultiIndexesUsingBatchAction() {
        // start node
        internalCluster().startNode();
        String indexName1 = "test1";
        String indexName2 = "test2";
        int numShards = internalCluster().numDataNodes();
        // assign one primary shard each to the data nodes
        prepareIndex(indexName1, numShards);
        prepareIndex(indexName2, numShards);
        Map<ShardId, ShardAttributes> shardIdShardAttributesMap = prepareRequestMap(new String[] { indexName1, indexName2 }, numShards);
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName1, indexName2).get();
        assertEquals(internalCluster().numDataNodes(), searchShardsResponse.getNodes().length);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(searchShardsResponse.getNodes(), shardIdShardAttributesMap)
        );
        for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
            ShardId shardId = clusterSearchShardsGroup.getShardId();
            assertEquals(1, clusterSearchShardsGroup.getShards().length);
            String nodeId = clusterSearchShardsGroup.getShards()[0].currentNodeId();
            GatewayStartedShard gatewayStartedShard = response.getNodesMap().get(nodeId).getNodeGatewayStartedShardsBatch().get(shardId);
            assertNodeGatewayStartedShardsHappyCase(gatewayStartedShard);
        }
    }

    public void testShardFetchCorruptedShardsUsingBatchAction() throws Exception {
        String indexName = "test";
        int numOfShards = 1;
        prepareIndex(indexName, numOfShards);
        Map<ShardId, ShardAttributes> shardIdShardAttributesMap = prepareRequestMap(new String[] { indexName }, numOfShards);
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName).get();
        final Index index = resolveIndex(indexName);
        final ShardId shardId = new ShardId(index, 0);
        corruptShard(searchShardsResponse.getNodes()[0].getName(), shardId);
        TransportNodesListGatewayStartedShardsBatch.NodesGatewayStartedShardsBatch response;
        internalCluster().restartNode(searchShardsResponse.getNodes()[0].getName());
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListGatewayStartedShardsBatch.class),
            new TransportNodesListGatewayStartedShardsBatch.Request(getDiscoveryNodes(), shardIdShardAttributesMap)
        );
        DiscoveryNode[] discoveryNodes = getDiscoveryNodes();
        GatewayStartedShard gatewayStartedShard = response.getNodesMap()
            .get(discoveryNodes[0].getId())
            .getNodeGatewayStartedShardsBatch()
            .get(shardId);
        assertNotNull(gatewayStartedShard.storeException());
        assertNotNull(gatewayStartedShard.allocationId());
        assertTrue(gatewayStartedShard.primary());
    }

    public void testSingleShardStoreFetchUsingBatchAction() throws ExecutionException, InterruptedException {
        String indexName = "test";
        DiscoveryNode[] nodes = getDiscoveryNodes();
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response = prepareAndSendRequest(
            new String[] { indexName },
            nodes
        );
        Index index = resolveIndex(indexName);
        ShardId shardId = new ShardId(index, 0);
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata = response.getNodesMap()
            .get(nodes[0].getId())
            .getNodeStoreFilesMetadataBatch()
            .get(shardId);
        assertNodeStoreFilesMetadataSuccessCase(nodeStoreFilesMetadata, shardId);
    }

    public void testShardStoreFetchMultiNodeMultiIndexesUsingBatchAction() throws Exception {
        internalCluster().startNodes(2);
        String indexName1 = "test1";
        String indexName2 = "test2";
        DiscoveryNode[] nodes = getDiscoveryNodes();
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response = prepareAndSendRequest(
            new String[] { indexName1, indexName2 },
            nodes
        );
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(indexName1, indexName2).get();
        for (ClusterSearchShardsGroup clusterSearchShardsGroup : searchShardsResponse.getGroups()) {
            ShardId shardId = clusterSearchShardsGroup.getShardId();
            ShardRouting[] shardRoutings = clusterSearchShardsGroup.getShards();
            assertEquals(2, shardRoutings.length);
            for (ShardRouting shardRouting : shardRoutings) {
                TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata = response.getNodesMap()
                    .get(shardRouting.currentNodeId())
                    .getNodeStoreFilesMetadataBatch()
                    .get(shardId);
                assertNodeStoreFilesMetadataSuccessCase(nodeStoreFilesMetadata, shardId);
            }
        }
    }

    public void testShardStoreFetchNodeNotConnectedUsingBatchAction() {
        DiscoveryNode nonExistingNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        String indexName = "test";
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response = prepareAndSendRequest(
            new String[] { indexName },
            new DiscoveryNode[] { nonExistingNode }
        );
        assertTrue(response.hasFailures());
        assertEquals(1, response.failures().size());
        assertEquals(nonExistingNode.getId(), response.failures().get(0).nodeId());
    }

    public void testShardStoreFetchCorruptedIndexUsingBatchAction() throws Exception {
        internalCluster().startNodes(2);
        String index1Name = "test1";
        String index2Name = "test2";
        prepareIndices(new String[] { index1Name, index2Name }, 1, 1);
        Map<ShardId, ShardAttributes> shardAttributesMap = prepareRequestMap(new String[] { index1Name, index2Name }, 1);
        Index index1 = resolveIndex(index1Name);
        ShardId shardId1 = new ShardId(index1, 0);
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards(index1Name).get();
        assertEquals(2, searchShardsResponse.getNodes().length);

        // corrupt test1 index shards
        corruptShard(searchShardsResponse.getNodes()[0].getName(), shardId1);
        corruptShard(searchShardsResponse.getNodes()[1].getName(), shardId1);
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(false).get();
        DiscoveryNode[] discoveryNodes = getDiscoveryNodes();
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response;
        response = ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListShardStoreMetadataBatch.class),
            new TransportNodesListShardStoreMetadataBatch.Request(shardAttributesMap, discoveryNodes)
        );
        Map<ShardId, TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata> nodeStoreFilesMetadata = response.getNodesMap()
            .get(discoveryNodes[0].getId())
            .getNodeStoreFilesMetadataBatch();
        // We don't store exception in case of corrupt index, rather just return an empty response
        assertNull(nodeStoreFilesMetadata.get(shardId1).getStoreFileFetchException());
        assertEquals(shardId1, nodeStoreFilesMetadata.get(shardId1).storeFilesMetadata().shardId());
        assertTrue(nodeStoreFilesMetadata.get(shardId1).storeFilesMetadata().isEmpty());

        Index index2 = resolveIndex(index2Name);
        ShardId shardId2 = new ShardId(index2, 0);
        assertNodeStoreFilesMetadataSuccessCase(nodeStoreFilesMetadata.get(shardId2), shardId2);
    }

    public void testDeleteRedIndexInBatchMode() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true).build()
        );
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(
            2,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), true).build()
        );
        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "test1",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "test2",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "testg",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );

        ensureGreen("test", "test1", "test2", "testg");
        internalCluster().stopRandomDataNode();
        ensureStableCluster(2);

        ShardsBatchGatewayAllocator gatewayAllocator = internalCluster().getInstance(
            ShardsBatchGatewayAllocator.class,
            internalCluster().getClusterManagerName()
        );
        ensureRed("test", "test1", "test2");

        assertTrue(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.get(internalCluster().clusterService().getSettings()));

        logger.info("--> Now do a reroute so batches are created"); // to avoid any race condition in test
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        assertTrue(clusterRerouteResponse.isAcknowledged());

        AcknowledgedResponse deleteIndexResponse = client().admin().indices().prepareDelete("test").get();
        assertTrue(deleteIndexResponse.isAcknowledged());

        ensureYellow("testg");
        IndicesExistsResponse indexExistResponse = client().admin().indices().prepareExists("test").get();
        assertFalse(indexExistResponse.isExists());
    }

    private void prepareIndices(String[] indices, int numberOfPrimaryShards, int numberOfReplicaShards) {
        for (String index : indices) {
            createIndex(
                index,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfPrimaryShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicaShards)
                    .build()
            );
            index(index, "type", "1", Collections.emptyMap());
            flush(index);
        }
    }

    private TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch prepareAndSendRequest(
        String[] indices,
        DiscoveryNode[] nodes
    ) {
        Map<ShardId, ShardAttributes> shardAttributesMap = null;
        prepareIndices(indices, 1, 1);
        shardAttributesMap = prepareRequestMap(indices, 1);
        TransportNodesListShardStoreMetadataBatch.NodesStoreFilesMetadataBatch response;
        return ActionTestUtils.executeBlocking(
            internalCluster().getInstance(TransportNodesListShardStoreMetadataBatch.class),
            new TransportNodesListShardStoreMetadataBatch.Request(shardAttributesMap, nodes)
        );
    }

    private void assertNodeStoreFilesMetadataSuccessCase(
        TransportNodesListShardStoreMetadataBatch.NodeStoreFilesMetadata nodeStoreFilesMetadata,
        ShardId shardId
    ) {
        assertNull(nodeStoreFilesMetadata.getStoreFileFetchException());
        TransportNodesListShardStoreMetadataHelper.StoreFilesMetadata storeFileMetadata = nodeStoreFilesMetadata.storeFilesMetadata();
        assertFalse(storeFileMetadata.isEmpty());
        assertEquals(shardId, storeFileMetadata.shardId());
        assertNotNull(storeFileMetadata.peerRecoveryRetentionLeases());
    }

    private void assertNodeGatewayStartedShardsHappyCase(GatewayStartedShard gatewayStartedShard) {
        assertNull(gatewayStartedShard.storeException());
        assertNotNull(gatewayStartedShard.allocationId());
        assertTrue(gatewayStartedShard.primary());
    }

    private void prepareIndex(String indexName, int numberOfPrimaryShards) {
        createIndex(
            indexName,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfPrimaryShards).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        index(indexName, "type", "1", Collections.emptyMap());
        flush(indexName);
    }

    private List<String> findNodesWithShard(final boolean primary) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        List<ShardRouting> startedShards = state.routingTable().shardsWithState(ShardRoutingState.STARTED);
        List<ShardRouting> requiredStartedShards = startedShards.stream()
            .filter(startedShard -> startedShard.primary() == primary)
            .collect(Collectors.toList());
        Collections.shuffle(requiredStartedShards, random());
        return requiredStartedShards.stream().map(shard -> state.nodes().get(shard.currentNodeId()).getName()).collect(Collectors.toList());
    }

    private void allocationExplainReturnsNoWhenExtraReplicaShard(boolean batchModeEnabled) throws Exception {
        internalCluster().startClusterManagerOnlyNodes(
            1,
            Settings.builder().put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_BATCH_MODE.getKey(), batchModeEnabled).build()
        );
        internalCluster().startDataOnlyNodes(5);
        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 4).build()
        );
        ensureGreen("test");
        ensureStableCluster(6);

        // Stop one of the nodes to make the cluster yellow
        // We cannot directly create an index with replica = data node count because then the whole flow will get skipped due to
        // INDEX_CREATED
        List<String> nodesWithReplicaShards = findNodesWithShard(false);
        Settings replicaNodeDataPathSettings = internalCluster().dataPathSettings(nodesWithReplicaShards.get(0));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodesWithReplicaShards.get(0)));

        ensureStableCluster(5);
        ensureYellow("test");

        logger.info("--> calling allocation explain API");
        // shard should have decision NO because there is no valid node for the extra replica to go to
        AllocateUnassignedDecision aud = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex("test")
            .setShard(0)
            .setPrimary(false)
            .get()
            .getExplanation()
            .getShardAllocationDecision()
            .getAllocateDecision();

        assertEquals(AllocationDecision.NO, aud.getAllocationDecision());
        assertEquals("cannot allocate because allocation is not permitted to any of the nodes", aud.getExplanation());

        // Now creating a new index with too many replicas and trying again
        createIndex(
            "test2",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 5).build()
        );

        ensureYellowAndNoInitializingShards("test2");

        logger.info("--> calling allocation explain API again");
        // shard should have decision NO because there are 6 replicas and 4 data nodes
        aud = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex("test2")
            .setShard(0)
            .setPrimary(false)
            .get()
            .getExplanation()
            .getShardAllocationDecision()
            .getAllocateDecision();

        assertEquals(AllocationDecision.NO, aud.getAllocationDecision());
        assertEquals("cannot allocate because allocation is not permitted to any of the nodes", aud.getExplanation());

        logger.info("--> restarting the stopped node");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.name", nodesWithReplicaShards.get(0)).put(replicaNodeDataPathSettings).build()
        );

        ensureStableCluster(6);
        ensureGreen("test");

        logger.info("--> calling allocation explain API 3rd time");
        // shard should still have decision NO because there are 6 replicas and 5 data nodes
        aud = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex("test2")
            .setShard(0)
            .setPrimary(false)
            .get()
            .getExplanation()
            .getShardAllocationDecision()
            .getAllocateDecision();

        assertEquals(AllocationDecision.NO, aud.getAllocationDecision());
        assertEquals("cannot allocate because allocation is not permitted to any of the nodes", aud.getExplanation());

        internalCluster().startDataOnlyNodes(1);

        ensureStableCluster(7);
        ensureGreen("test2");
    }
}
