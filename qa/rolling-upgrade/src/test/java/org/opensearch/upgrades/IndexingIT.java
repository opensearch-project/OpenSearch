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

package org.opensearch.upgrades;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Booleans;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.rest.action.document.RestBulkAction;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;

/**
 * Basic test that indexed documents survive the rolling restart. See
 * {@link RecoveryIT} for much more in depth testing of the mechanism
 * by which they survive.
 * <p>
 * This test is an almost exact copy of <code>IndexingIT</code> in the
 * xpack rolling restart tests. We should work on a way to remove this
 * duplication but for now we have no real way to share code.
 */
public class IndexingIT extends AbstractRollingTestCase {

    private void printClusterNodes() throws IOException, ParseException, URISyntaxException {
        Request clusterStateRequest = new Request("GET", "_nodes");
        Response response = client().performRequest(clusterStateRequest);

        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        for (String id : nodesAsMap.keySet()) {
            logger.info("--> {} {} {}",
                id,
                objectPath.evaluate("nodes." + id + ".name"),
                Version.fromString(objectPath.evaluate("nodes." + id + ".version")));
        }
        response = client().performRequest(new Request("GET", "_cluster/state"));
        String cm = ObjectPath.createFromResponse(response).evaluate("master_node");
        logger.info("--> Cluster manager {}", cm);
    }

    // Verifies that for each shard copy holds same document count across all containing nodes.
    private void waitForSearchableDocs(String index, int shardCount) throws Exception {
        Map<Integer,String> primaryShardToNodeIDMap = new HashMap<>();
        Map<Integer,String> replicaShardToNodeIDMap = new HashMap<>();
        logger.info("--> _cat/shards \n{}", EntityUtils.toString(client().performRequest(new Request("GET", "/_cat/shards?v")).getEntity()));

        Request request = new Request("GET", index + "/_stats");
        request.addParameter("level", "shards");
        Response response = client().performRequest(request);
        for (int shardNumber = 0; shardNumber < shardCount; shardNumber++) {
            List<Object> shardStats = ObjectPath.createFromResponse(response).evaluate("indices." + index + ".shards." + shardNumber);
            for (Object shard : shardStats) {
                final String nodeId = ObjectPath.evaluate(shard, "routing.node");
                final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
                if (primary) {
                    primaryShardToNodeIDMap.putIfAbsent(shardNumber, nodeId);
                } else {
                    replicaShardToNodeIDMap.putIfAbsent(shardNumber, nodeId);
                }
            }
        }
        logger.info("--> primaryShardToNodeIDMap {}", primaryShardToNodeIDMap);
        logger.info("--> replicaShardToNodeIDMap {}", replicaShardToNodeIDMap);

        for (int shardNumber = 0; shardNumber < shardCount; shardNumber++) {
            logger.info("--> Verify doc count for shard number {}", shardNumber);
            Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
            searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
            searchTestIndexRequest.addParameter("filter_path", "hits.total");
            searchTestIndexRequest.addParameter("preference", "_shards:" + shardNumber + "|_only_nodes:" + primaryShardToNodeIDMap.get(shardNumber));
            Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
            final int primaryHits = ObjectPath.createFromResponse(searchTestIndexResponse).evaluate("hits.total");
            logger.info("--> primaryHits {}", primaryHits);
            final int shardNum = shardNumber;
            // Verify replica shard doc count only when available.
            if (replicaShardToNodeIDMap.get(shardNum) != null) {
                assertBusy(() -> {
                    Request replicaRequest = new Request("POST", "/" + index + "/_search");
                    replicaRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
                    replicaRequest.addParameter("filter_path", "hits.total");
                    replicaRequest.addParameter("preference", "_shards:" + shardNum + "|_only_nodes:" + replicaShardToNodeIDMap.get(shardNum));
                    Response replicaResponse = client().performRequest(replicaRequest);
                    int replicaHits = ObjectPath.createFromResponse(replicaResponse).evaluate("hits.total");
                    logger.info("--> ReplicaHits {}", replicaHits);
                    assertEquals(primaryHits, replicaHits);
                }, 1, TimeUnit.MINUTES);
            }
        }
    }

    private void waitForClusterHealthWithNoShardMigration(String indexName, String status) throws IOException {
        Request waitForStatus = new Request("GET", "/_cluster/health/" + indexName);
        waitForStatus.addParameter("wait_for_status", status);
        // wait for long enough that we give delayed unassigned shards to stop being delayed
        waitForStatus.addParameter("timeout", "70s");
        waitForStatus.addParameter("level", "shards");
        waitForStatus.addParameter("wait_for_no_initializing_shards", "true");
        waitForStatus.addParameter("wait_for_no_relocating_shards", "true");
        client().performRequest(waitForStatus);
    }

    public void testIndexing() throws IOException, ParseException {
        switch (CLUSTER_TYPE) {
        case OLD:
            break;
        case MIXED:
            Request waitForYellow = new Request("GET", "/_cluster/health");
            waitForYellow.addParameter("wait_for_nodes", "3");
            waitForYellow.addParameter("wait_for_status", "yellow");
            client().performRequest(waitForYellow);
            break;
        case UPGRADED:
            Request waitForGreen = new Request("GET", "/_cluster/health/test_index,index_with_replicas,empty_index");
            waitForGreen.addParameter("wait_for_nodes", "3");
            waitForGreen.addParameter("wait_for_status", "green");
            // wait for long enough that we give delayed unassigned shards to stop being delayed
            waitForGreen.addParameter("timeout", "70s");
            waitForGreen.addParameter("level", "shards");
            client().performRequest(waitForGreen);
            break;
        default:
            throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        if (CLUSTER_TYPE == ClusterType.OLD) {
            Request createTestIndex = new Request("PUT", "/test_index");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createTestIndex);
            client().performRequest(createTestIndex);
            allowedWarnings("index [test_index] matches multiple legacy templates [global, prevent-bwc-deprecation-template], " +
                "composable templates will only match a single template");

            String recoverQuickly = "{\"settings\": {\"index.unassigned.node_left.delayed_timeout\": \"100ms\"}}";
            Request createIndexWithReplicas = new Request("PUT", "/index_with_replicas");
            createIndexWithReplicas.setJsonEntity(recoverQuickly);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createIndexWithReplicas);
            client().performRequest(createIndexWithReplicas);

            Request createEmptyIndex = new Request("PUT", "/empty_index");
            // Ask for recovery to be quick
            createEmptyIndex.setJsonEntity(recoverQuickly);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createEmptyIndex);
            client().performRequest(createEmptyIndex);

            bulk("test_index", "_OLD", 5);
            bulk("index_with_replicas", "_OLD", 5);
        }

        int expectedCount;
        switch (CLUSTER_TYPE) {
        case OLD:
            expectedCount = 5;
            break;
        case MIXED:
            if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                expectedCount = 5;
            } else {
                expectedCount = 10;
            }
            break;
        case UPGRADED:
            expectedCount = 15;
            break;
        default:
            throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        assertCount("test_index", expectedCount);
        assertCount("index_with_replicas", 5);
        assertCount("empty_index", 0);

        if (CLUSTER_TYPE != ClusterType.OLD) {
            bulk("test_index", "_" + CLUSTER_TYPE, 5);
            Request toBeDeleted = new Request("PUT", "/test_index/_doc/to_be_deleted");
            toBeDeleted.addParameter("refresh", "true");
            toBeDeleted.setJsonEntity("{\"f1\": \"delete-me\"}");
            client().performRequest(toBeDeleted);
            assertCount("test_index", expectedCount + 6);

            Request delete = new Request("DELETE", "/test_index/_doc/to_be_deleted");
            delete.addParameter("refresh", "true");
            client().performRequest(delete);

            assertCount("test_index", expectedCount + 5);
        }
    }


    /**
     * This test verifies that during rolling upgrades the segment replication does not break when replica shards can
     * be running on older codec versions.
     *
     * @throws Exception
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/7679")
    public void testIndexingWithSegRep() throws Exception {
        final String indexName = "test-index-segrep";
        final int shardCount = 3;
        final int replicaCount = 1;
        logger.info("--> Case {}", CLUSTER_TYPE);
        printClusterNodes();
        logger.info("--> _cat/shards before test execution \n{}", EntityUtils.toString(client().performRequest(new Request("GET", "/_cat/shards?v")).getEntity()));
        switch (CLUSTER_TYPE) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), shardCount)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), replicaCount)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms");
                createIndex(indexName, settings.build());
                waitForClusterHealthWithNoShardMigration(indexName, "green");
                bulk(indexName, "_OLD", 5);
                break;
            case MIXED:
                waitForClusterHealthWithNoShardMigration(indexName, "yellow");
                break;
            case UPGRADED:
                waitForClusterHealthWithNoShardMigration(indexName, "green");
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        int expectedCount;
        switch (CLUSTER_TYPE) {
            case OLD:
                expectedCount = 5;
                break;
            case MIXED:
                if (Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
                    expectedCount = 5;
                } else {
                    expectedCount = 10;
                }
                break;
            case UPGRADED:
                expectedCount = 15;
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        waitForSearchableDocs(indexName, shardCount);
        assertCount(indexName, expectedCount);

        if (CLUSTER_TYPE != ClusterType.OLD) {
            logger.info("--> Bulk index 5 documents");
            bulk(indexName, "_" + CLUSTER_TYPE, 5);
            logger.info("--> Index one doc (to be deleted next) and verify doc count");
            Request toBeDeleted = new Request("PUT", "/" + indexName + "/_doc/to_be_deleted");
            toBeDeleted.addParameter("refresh", "true");
            toBeDeleted.setJsonEntity("{\"f1\": \"delete-me\"}");
            client().performRequest(toBeDeleted);
            waitForSearchableDocs(indexName, shardCount);
            assertCount(indexName, expectedCount + 6);

            logger.info("--> Delete previously added doc and verify doc count");
            Request delete = new Request("DELETE", "/" + indexName + "/_doc/to_be_deleted");
            delete.addParameter("refresh", "true");
            client().performRequest(delete);
            waitForSearchableDocs(indexName, shardCount);
            assertCount(indexName, expectedCount + 5);
        }
        logger.info("--> _cat/shards post execution \n{}", EntityUtils.toString(client().performRequest(new Request("GET", "/_cat/shards?v")).getEntity()));
    }

    public void testAutoIdWithOpTypeCreate() throws IOException {
        final String indexName = "auto_id_and_op_type_create_index";
        StringBuilder b = new StringBuilder();
        b.append("{\"create\": {\"_index\": \"").append(indexName).append("\"}}\n");
        b.append("{\"f1\": \"v\"}\n");
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(b.toString());

        switch (CLUSTER_TYPE) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
                createIndex(indexName, settings.build());
                break;
            case MIXED:
                Request waitForGreen = new Request("GET", "/_cluster/health");
                waitForGreen.addParameter("wait_for_nodes", "3");
                client().performRequest(waitForGreen);

                Version minNodeVersion = null;
                Map<?, ?> response = entityAsMap(client().performRequest(new Request("GET", "_nodes")));
                Map<?, ?> nodes = (Map<?, ?>) response.get("nodes");
                for (Map.Entry<?, ?> node : nodes.entrySet()) {
                    Map<?, ?> nodeInfo = (Map<?, ?>) node.getValue();
                    Version nodeVersion = Version.fromString(nodeInfo.get("version").toString());
                    if (minNodeVersion == null) {
                        minNodeVersion = nodeVersion;
                    } else if (nodeVersion.before(minNodeVersion)) {
                        minNodeVersion = nodeVersion;
                    }
                }

                client().performRequest(bulk);
                break;
            case UPGRADED:
                client().performRequest(bulk);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void bulk(String index, String valueSuffix, int count) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < count; i++) {
            b.append("{\"index\": {\"_index\": \"").append(index).append("\"}}\n");
            b.append("{\"f1\": \"v").append(i).append(valueSuffix).append("\", \"f2\": ").append(i).append("}\n");
        }
        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(b.toString());
        client().performRequest(bulk);
    }

    private void assertCount(String index, int count) throws IOException, ParseException {
        Request searchTestIndexRequest = new Request("POST", "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        searchTestIndexRequest.addParameter("filter_path", "hits.total");
        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        assertEquals("{\"hits\":{\"total\":" + count + "}}",
                EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8));
    }
}
