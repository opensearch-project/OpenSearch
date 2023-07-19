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

package org.opensearch.backwards;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class IndexingIT extends OpenSearchRestTestCase {

    protected static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));


    private int indexDocs(String index, final int idStart, final int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final int id = idStart + i;
            Request request = new Request("PUT", index + "/_doc/" + id);
            request.setJsonEntity("{\"test\": \"test_" + randomAlphaOfLength(2) + "\"}");
            assertOK(client().performRequest(request));
        }
        return numDocs;
    }

    /**
     * Indexes a document in <code>index</code> with <code>docId</code> then concurrently updates the same document
     * <code>nUpdates</code> times
     *
     * @return the document version after updates
     */
    private int indexDocWithConcurrentUpdates(String index, final int docId, int nUpdates) throws IOException, InterruptedException {
        indexDocs(index, docId, 1);
        Thread[] indexThreads = new Thread[nUpdates];
        for (int i = 0; i < nUpdates; i++) {
            indexThreads[i] = new Thread(() -> {
                try {
                    indexDocs(index, docId, 1);
                } catch (IOException e) {
                    throw new AssertionError("failed while indexing [" + e.getMessage() + "]");
                }
            });
            indexThreads[i].start();
        }
        for (Thread indexThread : indexThreads) {
            indexThread.join();
        }
        return nUpdates + 1;
    }

    private void printClusterRouting() throws IOException, ParseException {
        Request clusterStateRequest = new Request("GET", "_cluster/state/routing_nodes?pretty");
        String clusterState = EntityUtils.toString(client().performRequest(clusterStateRequest).getEntity()).trim();
        logger.info("cluster nodes: {}", clusterState);
    }

    /**
     * This test verifies that segment replication does not break when primary shards are on lower OS version. It does this
     * by verifying replica shards contains same number of documents as primary's.
     */
    public void testIndexingWithPrimaryOnBwcNodes() throws Exception {
        if (UPGRADE_FROM_VERSION.before(Version.V_2_4_0)) {
            logger.info("--> Skip test for version {} where segment replication feature is not available", UPGRADE_FROM_VERSION);
            return;
        }
        Nodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered:\n {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(Node::getNodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        // Update allocation settings so that primaries gets allocated only on nodes running on older version
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.routing.allocation.include._name", bwcNames);
        final String index = "test-index";
        createIndex(index, settings.build());
        ensureNoInitializingShards(); // wait for all other shard activity to finish

        int docCount = 200;
        try (RestClient nodeClient = buildClient(restClientSettings(),
            nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {

            logger.info("Remove allocation include settings so that shards can be allocated on current version nodes");
            updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.include._name"));
            // Add replicas so that it can be assigned on higher OS version nodes.
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 2));

            printClusterRouting();
            ensureGreen(index);

            // Index docs
            indexDocs(index, 0, docCount);

            // perform a refresh
            assertOK(client().performRequest(new Request("POST", index + "/_flush")));

            // verify replica catch up with primary
            assertSeqNoOnShards(index, nodes, docCount, nodeClient);
        }
    }


    /**
     * This test creates a cluster with primary on higher version but due to {@link org.opensearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider};
     * replica shard allocation on lower OpenSearch version is prevented. Thus, this test though cover the use case where
     * primary shard containing nodes are running on higher OS version while replicas are unassigned.
     */
    public void testIndexingWithReplicaOnBwcNodes() throws Exception {
        if (UPGRADE_FROM_VERSION.before(Version.V_2_4_0)) {
            logger.info("--> Skip test for version {} where segment replication feature is not available", UPGRADE_FROM_VERSION);
            return;
        }
        Nodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered:\n {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(Node::getNodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        // Exclude bwc nodes from allocation so that primaries gets allocated on current/higher version
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.routing.allocation.exclude._name", bwcNames);
        final String index = "test-index";
        createIndex(index, settings.build());
        ensureNoInitializingShards(); // wait for all other shard activity to finish
        printClusterRouting();

        int docCount = 200;
        try (RestClient nodeClient = buildClient(restClientSettings(),
            nodes.values().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {

            logger.info("allowing replica shards assignment on bwc nodes");
            updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.exclude._name"));
            // Add replicas so that it can be assigned on lower OS version nodes, but it doesn't work as called out in test overview
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 2));
            printClusterRouting();

            // Index docs
            indexDocs(index, 0, docCount);

            // perform a refresh
            assertOK(client().performRequest(new Request("POST", index + "/_flush")));

            // verify replica catch up with primary
            assertSeqNoOnShards(index, nodes, docCount, nodeClient);
        }
    }

    public void testIndexVersionPropagation() throws Exception {
        Nodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(Node::getNodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
                .put("index.routing.allocation.include._name", bwcNames);
        final String index = "indexversionprop";
        final int minUpdates = 5;
        final int maxUpdates = 10;
        createIndex(index, settings.build());
        try (RestClient newNodeClient = buildClient(restClientSettings(),
                nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {

            int nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates initially", nUpdates);
            final int finalVersionForDoc1 = indexDocWithConcurrentUpdates(index, 1, nUpdates);
            logger.info("allowing shards on all nodes");
            updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.include._name"));
            ensureGreen(index);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            List<Shard> shards = buildShards(index, nodes, newNodeClient);
            Shard primary = buildShards(index, nodes, newNodeClient).stream().filter(Shard::isPrimary).findFirst().get();
            logger.info("primary resolved to: " + primary.getNode().getNodeName());
            for (Shard shard : shards) {
                assertVersion(index, 1, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc1);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 1);
            }

            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates after allowing shards on all nodes", nUpdates);
            final int finalVersionForDoc2 = indexDocWithConcurrentUpdates(index, 2, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            primary = shards.stream().filter(Shard::isPrimary).findFirst().get();
            logger.info("primary resolved to: " + primary.getNode().getNodeName());
            for (Shard shard : shards) {
                assertVersion(index, 2, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc2);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 2);
            }

            primary = shards.stream().filter(Shard::isPrimary).findFirst().get();
            logger.info("moving primary to new node by excluding {}", primary.getNode().getNodeName());
            updateIndexSettings(index, Settings.builder().put("index.routing.allocation.exclude._name", primary.getNode().getNodeName()));
            ensureGreen(index);
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates after moving primary", nUpdates);
            final int finalVersionForDoc3 = indexDocWithConcurrentUpdates(index, 3, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 3, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc3);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 3);
            }

            logger.info("setting number of replicas to 0");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 0));
            ensureGreen(index);
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing doc with [{}] concurrent updates after setting number of replicas to 0", nUpdates);
            final int finalVersionForDoc4 = indexDocWithConcurrentUpdates(index, 4, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 4, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc4);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 4);
            }

            logger.info("setting number of replicas to 1");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 1));
            ensureGreen(index);
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing doc with [{}] concurrent updates after setting number of replicas to 1", nUpdates);
            final int finalVersionForDoc5 = indexDocWithConcurrentUpdates(index, 5, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 5, "_only_nodes:" + shard.getNode().getNodeName(), finalVersionForDoc5);
                assertCount(index, "_only_nodes:" + shard.getNode().getNodeName(), 5);
            }
        }
    }

    public void testSeqNoCheckpoints() throws Exception {
        Nodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(Node::getNodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
            .put("index.routing.allocation.include._name", bwcNames);

        final String index = "test";
        createIndex(index, settings.build());
        try (RestClient newNodeClient = buildClient(restClientSettings(),
            nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {
            int numDocs = 0;
            final int numberOfInitialDocs = 1 + randomInt(5);
            logger.info("indexing [{}] docs initially", numberOfInitialDocs);
            numDocs += indexDocs(index, 0, numberOfInitialDocs);
            boolean compat = nodes.getBWCVersion().compareTo(LegacyESVersion.fromId(6000000)) >= 0;
            assertSeqNoOnShards(index, nodes, compat ? numDocs : 0, newNodeClient);
            logger.info("allowing shards on all nodes");
            updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.include._name"));
            ensureGreen(index);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            for (final String bwcName : bwcNamesList) {
                assertCount(index, "_only_nodes:" + bwcName, numDocs);
            }
            final int numberOfDocsAfterAllowingShardsOnAllNodes = 1 + randomInt(5);
            logger.info("indexing [{}] docs after allowing shards on all nodes", numberOfDocsAfterAllowingShardsOnAllNodes);
            numDocs += indexDocs(index, numDocs, numberOfDocsAfterAllowingShardsOnAllNodes);
            compat = nodes.getBWCVersion().compareTo(LegacyESVersion.fromId(6000000)) >= 0;
            assertSeqNoOnShards(index, nodes, compat ? numDocs : 0, newNodeClient);
            Shard primary = buildShards(index, nodes, newNodeClient).stream().filter(Shard::isPrimary).findFirst().get();
            logger.info("moving primary to new node by excluding {}", primary.getNode().getNodeName());
            updateIndexSettings(index, Settings.builder().put("index.routing.allocation.exclude._name", primary.getNode().getNodeName()));
            ensureGreen(index);
            int numDocsOnNewPrimary = 0;
            final int numberOfDocsAfterMovingPrimary = 1 + randomInt(5);
            logger.info("indexing [{}] docs after moving primary", numberOfDocsAfterMovingPrimary);
            numDocsOnNewPrimary += indexDocs(index, numDocs, numberOfDocsAfterMovingPrimary);
            numDocs += numberOfDocsAfterMovingPrimary;
            compat = nodes.getBWCVersion().compareTo(LegacyESVersion.fromId(6000000)) >= 0;
            assertSeqNoOnShards(index, nodes, compat ? numDocs : numDocsOnNewPrimary, newNodeClient);
            /*
             * Dropping the number of replicas to zero, and then increasing it to one triggers a recovery thus exercising any BWC-logic in
             * the recovery code.
             */
            logger.info("setting number of replicas to 0");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 0));
            final int numberOfDocsAfterDroppingReplicas = 1 + randomInt(5);
            logger.info("indexing [{}] docs after setting number of replicas to 0", numberOfDocsAfterDroppingReplicas);
            numDocsOnNewPrimary += indexDocs(index, numDocs, numberOfDocsAfterDroppingReplicas);
            numDocs += numberOfDocsAfterDroppingReplicas;
            logger.info("setting number of replicas to 1");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 1));
            ensureGreen(index);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));

            for (Shard shard : buildShards(index, nodes, newNodeClient)) {
                assertCount(index, "_only_nodes:" + shard.node.nodeName, numDocs);
            }
            compat = nodes.getBWCVersion().compareTo(LegacyESVersion.fromId(6000000)) >= 0;
            assertSeqNoOnShards(index, nodes, compat ? numDocs : numDocsOnNewPrimary, newNodeClient);
        }
    }

    public void testUpdateSnapshotStatus() throws Exception {
        Nodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());

        // Create the repository before taking the snapshot.
        Request request = new Request("PUT", "/_snapshot/repo");
        request.setJsonEntity(Strings
            .toString(JsonXContent.contentBuilder()
                .startObject()
                    .field("type", "fs")
                    .startObject("settings")
                        .field("compress", randomBoolean())
                        .field("location", System.getProperty("tests.path.repo"))
                    .endObject()
                .endObject()));

        assertOK(client().performRequest(request));

        String bwcNames = nodes.getBWCNodes().stream().map(Node::getNodeName).collect(Collectors.joining(","));

        // Allocating shards on the BWC nodes to makes sure that taking snapshot happens on those nodes.
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), between(5, 10))
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put("index.routing.allocation.include._name", bwcNames);

        final String index = "test-snapshot-index";
        createIndex(index, settings.build());
        indexDocs(index, 0, between(50, 100));
        ensureGreen(index);
        assertOK(client().performRequest(new Request("POST", index + "/_refresh")));

        request = new Request("PUT", "/_snapshot/repo/bwc-snapshot");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));

        // Allocating shards on all nodes, taking snapshots should happen on all nodes.
        updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.include._name"));
        ensureGreen(index);
        assertOK(client().performRequest(new Request("POST", index + "/_refresh")));

        request = new Request("PUT", "/_snapshot/repo/mixed-snapshot");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
    }

    public void testSyncedFlushTransition() throws Exception {
        Nodes nodes = buildNodeAndVersions();
        assumeTrue("bwc version is on 1.x or Legacy 7.x", nodes.getBWCVersion().before(Version.V_2_0_0));
        assumeFalse("no new node found", nodes.getNewNodes().isEmpty());
        assumeFalse("no bwc node found", nodes.getBWCNodes().isEmpty());
        // Allocate shards to new nodes then verify synced flush requests processed by old nodes/new nodes
        String newNodes = nodes.getNewNodes().stream().map(Node::getNodeName).collect(Collectors.joining(","));
        int numShards = randomIntBetween(1, 10);
        int numOfReplicas = randomIntBetween(0, nodes.getNewNodes().size() - 1);
        int totalShards = numShards * (numOfReplicas + 1);
        final String index = "test_synced_flush";
        createIndex(index, Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
            .put("index.routing.allocation.include._name", newNodes).build());
        ensureGreen(index);
        indexDocs(index, randomIntBetween(0, 100), between(1, 100));
        try (RestClient oldNodeClient = buildClient(restClientSettings(),
            nodes.getBWCNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {
            Request request = new Request("POST", index + "/_flush/synced");
            assertBusy(() -> {
                ResponseException responseException = expectThrows(ResponseException.class, () -> oldNodeClient.performRequest(request));
                assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
                assertThat(responseException.getResponse().getWarnings(),
                    contains("Synced flush is deprecated and will be removed in 3.0. Use flush at _/flush or /{index}/_flush instead."));
                Map<String, Object> result = ObjectPath.createFromResponse(responseException.getResponse()).evaluate("_shards");
                assertThat(result.get("total"), equalTo(totalShards));
                assertThat(result.get("successful"), equalTo(0));
                assertThat(result.get("failed"), equalTo(totalShards));
            });
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        }
        indexDocs(index, randomIntBetween(0, 100), between(1, 100));
        try (RestClient newNodeClient = buildClient(restClientSettings(),
            nodes.getNewNodes().stream().map(Node::getPublishAddress).toArray(HttpHost[]::new))) {
            Request request = new Request("POST", index + "/_flush/synced");
            List<String> warningMsg = Arrays.asList("Synced flush was removed and a normal flush was performed instead. " +
                "This transition will be removed in a future version.");
            RequestOptions.Builder requestOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
            requestOptionsBuilder.setWarningsHandler(warnings -> warnings.equals(warningMsg) == false);
            request.setOptions(requestOptionsBuilder);
            assertBusy(() -> {
                Map<String, Object> result = ObjectPath.createFromResponse(newNodeClient.performRequest(request)).evaluate("_shards");
                assertThat(result.get("total"), equalTo(totalShards));
                assertThat(result.get("successful"), equalTo(totalShards));
                assertThat(result.get("failed"), equalTo(0));
            });
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        }
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        Request request = new Request("GET", index + "/_count");
        request.addParameter("preference", preference);
        final Response response = client().performRequest(request);
        assertOK(response);
        final int actualCount = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("count").toString());
        assertThat(actualCount, equalTo(expectedCount));
    }

    private void assertVersion(final String index, final int docId, final String preference, final int expectedVersion) throws IOException {
        Request request = new Request("GET", index + "/_doc/" + docId);
        request.addParameter("preference", preference);

        final Response response = client().performRequest(request);
        assertOK(response);
        final int actualVersion = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("_version").toString());
        assertThat("version mismatch for doc [" + docId + "] preference [" + preference + "]", actualVersion, equalTo(expectedVersion));
    }

    private void assertSeqNoOnShards(String index, Nodes nodes, int numDocs, RestClient client)
            throws Exception {
        assertBusy(() -> {
            try {
                List<Shard> shards = buildShards(index, nodes, client);
                Shard primaryShard = shards.stream().filter(Shard::isPrimary).findFirst().get();
                assertNotNull("failed to find primary shard", primaryShard);
                final long expectedGlobalCkp = numDocs - 1;
                final long expectMaxSeqNo = numDocs - 1;
                logger.info("primary resolved to node {}", primaryShard.getNode());
                for (Shard shard : shards) {
                    final SeqNoStats seqNoStats = shard.getSeqNoStats();
                    logger.info("stats for {}, primary [{}]: [{}]", shard.getNode(), shard.isPrimary(), seqNoStats);
                    assertThat("max_seq no on " + shard.getNode() + " is wrong", seqNoStats.getMaxSeqNo(), equalTo(expectMaxSeqNo));
                    assertThat("localCheckpoint no on " + shard.getNode() + " is wrong",
                        seqNoStats.getLocalCheckpoint(), equalTo(expectMaxSeqNo));
                    assertThat("globalCheckpoint no on " + shard.getNode() + " is wrong",
                        seqNoStats.getGlobalCheckpoint(), equalTo(expectedGlobalCkp));
                }
            } catch (IOException e) {
                throw new AssertionError("unexpected io exception", e);
            }
        });
    }

    private List<Shard> buildShards(String index, Nodes nodes, RestClient client) throws IOException {
        Request request = new Request("GET", index + "/_stats");
        request.addParameter("level", "shards");
        Response response = client.performRequest(request);
        List<Object> shardStats = ObjectPath.createFromResponse(response).evaluate("indices." + index + ".shards.0");
        ArrayList<Shard> shards = new ArrayList<>();
        for (Object shard : shardStats) {
            final String nodeId = ObjectPath.evaluate(shard, "routing.node");
            final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
            final Node node = nodes.getSafe(nodeId);
            final SeqNoStats seqNoStats;
            Integer maxSeqNo = ObjectPath.evaluate(shard, "seq_no.max_seq_no");
            Integer localCheckpoint = ObjectPath.evaluate(shard, "seq_no.local_checkpoint");
            Integer globalCheckpoint = ObjectPath.evaluate(shard, "seq_no.global_checkpoint");
            seqNoStats = new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint);
            shards.add(new Shard(node, primary, seqNoStats));
        }
        logger.info("shards {}", shards);
        return shards;
    }

    private Nodes buildNodeAndVersions() throws IOException, URISyntaxException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Nodes nodes = new Nodes();
        for (String id : nodesAsMap.keySet()) {
            nodes.add(new Node(
                id,
                objectPath.evaluate("nodes." + id + ".name"),
                Version.fromString(objectPath.evaluate("nodes." + id + ".version")),
                HttpHost.create((String)objectPath.evaluate("nodes." + id + ".http.publish_address"))));
        }
        response = client().performRequest(new Request("GET", "_cluster/state"));
        nodes.setClusterManagerNodeId(ObjectPath.createFromResponse(response).evaluate("master_node"));
        return nodes;
    }

    final class Nodes extends HashMap<String, Node> {

        private String clusterManagerNodeId = null;

        public Node getClusterManager() {
            return get(clusterManagerNodeId);
        }

        public void setClusterManagerNodeId(String id) {
            if (get(id) == null) {
                throw new IllegalArgumentException("node with id [" + id + "] not found. got:" + toString());
            }
            clusterManagerNodeId = id;
        }

        public void add(Node node) {
            put(node.getId(), node);
        }

        public List<Node> getNewNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().after(bwcVersion)).collect(Collectors.toList());
        }

        public List<Node> getBWCNodes() {
            Version bwcVersion = getBWCVersion();
            return values().stream().filter(n -> n.getVersion().equals(bwcVersion)).collect(Collectors.toList());
        }

        public Version getBWCVersion() {
            if (isEmpty()) {
                throw new IllegalStateException("no nodes available");
            }
            return Version.fromId(values().stream().map(node -> node.getVersion().id).min(Integer::compareTo).get());
        }

        public Node getSafe(String id) {
            Node node = get(id);
            if (node == null) {
                throw new IllegalArgumentException("node with id [" + id + "] not found");
            }
            return node;
        }

        @Override
        public String toString() {
            return "Nodes{" +
                "masterNodeId='" + clusterManagerNodeId + "'\n" +
                values().stream().map(Node::toString).collect(Collectors.joining("\n")) +
                '}';
        }
    }

    final class Node {
        private final String id;
        private final String nodeName;
        private final Version version;
        private final HttpHost publishAddress;

        Node(String id, String nodeName, Version version, HttpHost publishAddress) {
            this.id = id;
            this.nodeName = nodeName;
            this.version = version;
            this.publishAddress = publishAddress;
        }

        public String getId() {
            return id;
        }

        public String getNodeName() {
            return nodeName;
        }

        public HttpHost getPublishAddress() {
            return publishAddress;
        }

        public Version getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "Node{" +
                "id='" + id + '\'' +
                ", nodeName='" + nodeName + '\'' +
                ", version=" + version +
                '}';
        }
    }

    final class Shard {
        private final Node node;
        private final boolean Primary;
        private final SeqNoStats seqNoStats;

        Shard(Node node, boolean primary, SeqNoStats seqNoStats) {
            this.node = node;
            Primary = primary;
            this.seqNoStats = seqNoStats;
        }

        public Node getNode() {
            return node;
        }

        public boolean isPrimary() {
            return Primary;
        }

        public SeqNoStats getSeqNoStats() {
            return seqNoStats;
        }

        @Override
        public String toString() {
            return "Shard{" +
                "node=" + node +
                ", Primary=" + Primary +
                ", seqNoStats=" + seqNoStats +
                '}';
        }
    }
}
