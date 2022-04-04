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

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Booleans;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.Strings;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.opensearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

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

    public void testIndexing() throws IOException {
        switch (CLUSTER_TYPE) {
            case OLD:
                break;
            case MIXED:
                Request waitForYellow = new Request(HttpGet.METHOD_NAME, "/_cluster/health");
                waitForYellow.addParameter("wait_for_nodes", "3");
                waitForYellow.addParameter("wait_for_status", "yellow");
                client().performRequest(waitForYellow);
                break;
            case UPGRADED:
                Request waitForGreen = new Request(HttpGet.METHOD_NAME, "/_cluster/health/test_index,index_with_replicas,empty_index");
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
            Request createTestIndex = new Request(HttpPut.METHOD_NAME, "/test_index");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_replicas\": 0}}");
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createTestIndex);
            client().performRequest(createTestIndex);
            allowedWarnings("index [test_index] matches multiple legacy templates [global, prevent-bwc-deprecation-template], " +
                "composable templates will only match a single template");

            String recoverQuickly = "{\"settings\": {\"index.unassigned.node_left.delayed_timeout\": \"100ms\"}}";
            Request createIndexWithReplicas = new Request(HttpPut.METHOD_NAME, "/index_with_replicas");
            createIndexWithReplicas.setJsonEntity(recoverQuickly);
            useIgnoreMultipleMatchingTemplatesWarningsHandler(createIndexWithReplicas);
            client().performRequest(createIndexWithReplicas);

            Request createEmptyIndex = new Request(HttpPut.METHOD_NAME, "/empty_index");
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
            Request toBeDeleted = new Request(HttpPut.METHOD_NAME, "/test_index/_doc/to_be_deleted");
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

    public void testAutoIdWithOpTypeCreate() throws IOException {
        final String indexName = "auto_id_and_op_type_create_index";
        StringBuilder b = new StringBuilder();
        b.append("{\"create\": {\"_index\": \"").append(indexName).append("\"}}\n");
        b.append("{\"f1\": \"v\"}\n");
        Request bulk = new Request(HttpPost.METHOD_NAME, "/_bulk");
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
                waitForClusterGreenStatus();
                Version minNodeVersion = getMinNodeVersion();
                if (minNodeVersion.before(LegacyESVersion.V_7_5_0)) {
                    ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(bulk));
                    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
                    assertThat(e.getMessage(),
                        // if request goes to 7.5+ node
                        either(containsString("optype create not supported for indexing requests without explicit id until"))
                            // if request goes to < 7.5 node
                            .or(containsString("an id must be provided if version type or value are set")
                            ));
                } else {
                    client().performRequest(bulk);
                }
                break;
            case UPGRADED:
                client().performRequest(bulk);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    public void testTypeRemovalIndexing() throws IOException {
        final String indexName = "test_index_with_mapping";
        final String indexWithoutTypeName = "test_index_without_mapping";
        final String templateIndexName = "template_test_index";
        final String templateName = "test_template";
        final String indexNamePattern = "template_test*";

        final String mapping = "\"properties\":{\"f1\":{\"type\":\"keyword\"},\"f2\":{\"type\":\"keyword\"}}";
        final String typeMapping = "\"_doc\":{" + mapping +"}";

        switch (CLUSTER_TYPE) {
            case OLD:
                Version minNodeVersion = getMinNodeVersion();
                if (minNodeVersion.before(Version.V_2_0_0)) {
                    Settings.Builder settings = Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
                    createIndexWithDocMappings(indexName, settings.build(), typeMapping);
                    assertIndexMapping(indexName, mapping);

                    createTemplate(templateName, indexNamePattern, typeMapping);
                    createIndex(templateIndexName, settings.build());
                    assertIndexMapping(templateIndexName, mapping);
                    bulk(indexName, CLUSTER_TYPE.name(), 1);
                    bulk(templateIndexName, CLUSTER_TYPE.name(), 1);

                    createIndex(indexWithoutTypeName, settings.build());
                    bulk(indexWithoutTypeName, CLUSTER_TYPE.name(), 1);
                }
                break;
            case MIXED:
                waitForClusterGreenStatus();
                break;
            case UPGRADED:
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
                    // Assert documents created with mapping prior to OS 2.0 are accessible.
                    assertCount(indexName, 1, 1);
                    bulk(indexName, CLUSTER_TYPE.name(), 1);
                    // Assert the newly ingested documents are accessible
                    assertCount(indexName, 2, 1);

                    // Assert documents created with mapping on index with template prior to OS 2.0 are accessible.
                    assertCount(templateIndexName, 1, 1);
                    bulk(templateIndexName, CLUSTER_TYPE.name(), 1);
                    // Assert the newly ingested documents with template are accessible
                    assertCount(templateIndexName, 2, 1);

                    // Assert documents created prior to OS 2.0 are accessible.
                    assertCount(indexWithoutTypeName, 1, 1);
                    // Test ingestion of new documents created using < OS2.0
                    bulk(indexWithoutTypeName, CLUSTER_TYPE.name(), 1);
                    assertCount(indexWithoutTypeName, 2, 1);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    public void testTypeRemovalReindexing() throws IOException {
        final String indexName = "test_reindex_with_mapping";
        final String reindexName = "test_reindex_with_mapping_v2";

        final String originalMapping = "\"properties\":{\"f1\":{\"type\":\"keyword\"},\"f2\":{\"type\":\"keyword\"}}";
        final String originalTypeMapping = "\"_doc\":{" + originalMapping +"}";

        final String newMapping = "\"properties\":{\"f1\":{\"type\":\"text\"},\"f2\":{\"type\":\"text\"}}";
        final String newTypeMapping = "\"_doc\":{" + newMapping +"}";

        switch (CLUSTER_TYPE) {
            case OLD:
                Version minNodeVersion = getMinNodeVersion();
                if (minNodeVersion.before(Version.V_2_0_0)) {
                    Settings.Builder settings = Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
                    createIndexWithDocMappings(indexName, settings.build(), originalTypeMapping);
                    assertIndexMapping(indexName, originalMapping);

                    createIndexWithDocMappings(reindexName, settings.build(), newTypeMapping);
                    assertIndexMapping(reindexName, newMapping);
                    bulk(indexName, CLUSTER_TYPE.name(), 1);
                }
                break;
            case MIXED:
                waitForClusterGreenStatus();
                break;
            case UPGRADED:
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
                    // Assert documents created with mapping prior to OS 2.0 are accessible.
                    assertCount(indexName, 1, 1);
                    bulk(indexName, CLUSTER_TYPE.name(), 1);
                    // Assert the newly ingested documents are accessible
                    assertCount(indexName, 2, 1);

                    reindex(indexName, reindexName);
                    assertCount(reindexName, 2, 1);
                    assertIndexMapping(reindexName, newMapping);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    public void testTypeRemovalSnapshots() throws IOException {
        final String indexName = "test_snapshot_index";
        final String repositoryName = "test_repository";
        final String snapshotName = "test_snapshot";

        final String mapping = "\"properties\":{\"f1\":{\"type\":\"keyword\"},\"f2\":{\"type\":\"keyword\"}}";
        final String typeMapping = "\"_doc\":{" + mapping +"}";

        switch (CLUSTER_TYPE) {
            case OLD:
                Version minNodeVersion = getMinNodeVersion();
                if (minNodeVersion.before(Version.V_2_0_0)) {
                    Settings.Builder settings = Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
                    createIndexWithDocMappings(indexName, settings.build(), typeMapping);
                    assertIndexMapping(indexName, mapping);
                    bulk(indexName, CLUSTER_TYPE.name(), 1);

                    registerRepository(repositoryName,"fs", true, Settings.builder()
                        .put("compress", true)
                        .put("location", REPOSITORY_LOCATION)
                        .build());
                    createSnapshotIfNotExists(repositoryName, snapshotName, true, indexName);
                    deleteIndex(indexName);
                }
                break;
            case MIXED:
                waitForClusterGreenStatus();
                break;
            case UPGRADED:
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
                    registerRepository(repositoryName,"fs", true, Settings.builder()
                        .put("compress", true)
                        .put("location", REPOSITORY_LOCATION)
                        .build());
                    assertFalse(indexExists(indexName));;
                    restoreSnapshot(repositoryName, snapshotName, true);
                    assertCount(indexName, 1);
                }
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
        Request bulk = new Request(HttpPost.METHOD_NAME, "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(b.toString());
        client().performRequest(bulk);
    }

    private void assertCount(String index, int count) throws IOException {
        assertCount(index, count, null);
    }

    private Version getMinNodeVersion() throws IOException {
        Version minNodeVersion = null;
        Map<?, ?> response = entityAsMap(client().performRequest(new Request(HttpGet.METHOD_NAME, "_nodes")));
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
        return minNodeVersion;
    }

    private void createIndexWithDocMappings(String index, Settings settings, String mapping) throws IOException {
        Request createIndexWithMappingsRequest = new Request(HttpPut.METHOD_NAME, "/" + index);
        String entity = "{\"settings\": " + Strings.toString(settings);
        if (mapping != null) {
            entity += ",\"mappings\" : {" + mapping + "}";
        }
        entity += "}";
        createIndexWithMappingsRequest.addParameter("include_type_name", "true");
        createIndexWithMappingsRequest.setJsonEntity(entity);
        useIgnoreTypesRemovalWarningsHandler(createIndexWithMappingsRequest);
        client().performRequest(createIndexWithMappingsRequest);
    }

    private void createTemplate(String templateName, String indexPattern, String mapping) throws IOException {
        Request templateRequest = new Request(HttpPut.METHOD_NAME, "/_template/" + templateName);
        String entity = "{\"index_patterns\": \"" + indexPattern + "\"";
        if (mapping != null) {
            entity += ",\"mappings\" : {" + mapping + "}";
        }
        entity += "}";
        templateRequest.addParameter("include_type_name", "true");
        templateRequest.setJsonEntity(entity);
        useIgnoreTypesRemovalWarningsHandler(templateRequest);
        client().performRequest(templateRequest);
    }

    private void reindex(String originalIndex, String newIndex) throws IOException {
        Request reIndexRequest = new Request(HttpPost.METHOD_NAME, "/_reindex/");
        String entity = "{ \"source\": { \"index\": \"" + originalIndex + "\" }, \"dest\": { \"index\": \"" + newIndex + "\" } }";
        reIndexRequest.setJsonEntity(entity);
        reIndexRequest.addParameter("refresh", "true");
        client().performRequest(reIndexRequest);
    }

    private void assertIndexMapping(String index, String mappings) throws IOException {
        Request testIndexMappingRequest = new Request(HttpGet.METHOD_NAME, "/" + index + "/_mapping");
        Response testIndexMappingResponse = client().performRequest(testIndexMappingRequest);
        assertEquals("{\""+index+"\":{\"mappings\":{"+mappings+"}}}",
            EntityUtils.toString(testIndexMappingResponse.getEntity(), StandardCharsets.UTF_8));
    }

    private void assertCount(String index, int count, Integer totalShards) throws IOException {
        Request searchTestIndexRequest = new Request(HttpPost.METHOD_NAME, "/" + index + "/_search");
        searchTestIndexRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        if (totalShards != null) {
            searchTestIndexRequest.addParameter("filter_path", "hits.total,_shards");
        } else {
            searchTestIndexRequest.addParameter("filter_path", "hits.total");
        }

        Response searchTestIndexResponse = client().performRequest(searchTestIndexRequest);
        String expectedResponse;
        if (totalShards != null) {
            expectedResponse = "{\"_shards\":{\"total\":" + totalShards + ",\"successful\":" + totalShards + ",\"skipped\":0,\"failed\":0},\"hits\":{\"total\":" + count + "}}";
        } else {
            expectedResponse = "{\"hits\":{\"total\":" + count + "}}";
        }
        assertEquals(expectedResponse,
            EntityUtils.toString(searchTestIndexResponse.getEntity(), StandardCharsets.UTF_8));
    }

    private final Pattern TYPE_REMOVAL_WARNING = Pattern.compile(
        "^\\[types removal\\] (.+) include_type_name (.+) is deprecated\\. The parameter will be removed in the next major version\\.$"
    );

    private void useIgnoreTypesRemovalWarningsHandler(Request request) {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.setWarningsHandler(warnings -> {
            if (warnings.size() > 0) {
                boolean matches = warnings.stream()
                    .anyMatch(
                        message -> TYPE_REMOVAL_WARNING.matcher(message).matches()
                    );
                return matches == false;
            } else {
                return false;
            }
        });
        request.setOptions(options);
    }

    private void createSnapshotIfNotExists(String repository, String snapshot, boolean waitForCompletion, String indexName) throws IOException {
        final Request getSnapshotsRequest = new Request(HttpGet.METHOD_NAME, "_cat/snapshots/" + repository);
        final Response getSnapshotsResponse = client().performRequest(getSnapshotsRequest);
        if (!EntityUtils.toString(getSnapshotsResponse.getEntity(), StandardCharsets.UTF_8).contains(snapshot)) {
            final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repository + '/' + snapshot);
            request.addParameter("wait_for_completion", Boolean.toString(waitForCompletion));
            if (indexName != null) {
                String entity = "{\"indices\" : \"" + indexName + "\"}";
                request.setJsonEntity(entity);
            }

            final Response response = client().performRequest(request);
            assertEquals(
                "Failed to create snapshot [" + snapshot + "] in repository [" + repository + "]: " + response,
                response.getStatusLine().getStatusCode(), RestStatus.OK.getStatus()
            );
        }
    }

    private void waitForClusterGreenStatus() throws IOException {
        Request waitForGreen = new Request(HttpGet.METHOD_NAME, "/_cluster/health");
        waitForGreen.addParameter("wait_for_nodes", "3");
        client().performRequest(waitForGreen);
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }
}
