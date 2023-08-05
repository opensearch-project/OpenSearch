/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import static org.opensearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

/**
 * Tests to ensure indices, snapshots, templates created using type mapping before {@link Version#V_2_0_0}
 * survive the rolling upgrade to {@link Version#V_2_0_0}. The tests include indexing, re-indexing,
 * snapshot and restore, template based index creation for type mapped indices to ensure they are accessible
 * and searchable in {@link Version#V_2_0_0}.
 */
public class MappingTypeRemovalIT extends AbstractRollingTestCase {

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
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
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
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
                    assertCount(indexName, 1, 1);
                    assertCount(templateIndexName, 1, 1);
                    assertCount(indexWithoutTypeName, 1, 1);
                }
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
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
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
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
                    assertCount(indexName, 1, 1);
                }
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
        final String snapshotName = "force_preserve_test_snapshot";

        final String mapping = "\"properties\":{\"f1\":{\"type\":\"keyword\"},\"f2\":{\"type\":\"keyword\"}}";
        final String typeMapping = "\"_doc\":{" + mapping +"}";

        switch (CLUSTER_TYPE) {
            case OLD:
                if (UPGRADE_FROM_VERSION.before(Version.V_2_0_0)) {
                    Settings.Builder settings = Settings.builder()
                        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                        .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);
                    createIndexWithDocMappings(indexName, settings.build(), typeMapping);
                    assertIndexMapping(indexName, mapping);
                    bulk(indexName, CLUSTER_TYPE.name(), 1);

                    registerRepository(repositoryName,"fs", true, Settings.builder()
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
                        .put("location", REPOSITORY_LOCATION)
                        .build());
                    assertFalse(indexExists(indexName));;
                    restoreSnapshot(repositoryName, snapshotName, true);
                    assertCount(indexName, 1, 1);
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

    private void createIndexWithDocMappings(String index, Settings settings, String mapping) throws IOException {
        Request createIndexWithMappingsRequest = new Request(HttpPut.METHOD_NAME, "/" + index);
        String entity = "{\"settings\": " + Strings.toString(MediaTypeRegistry.JSON, settings);
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
}
