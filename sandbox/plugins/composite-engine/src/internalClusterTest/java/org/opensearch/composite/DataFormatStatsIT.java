/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.composite.apis.ApiTestFixtures;
import org.opensearch.composite.apis.DataFormatApiTestUtils;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the composite-engine REST API endpoints:
 * <ul>
 *   <li>{@code GET /_plugins/dataformat_stats}</li>
 *   <li>{@code GET /_plugins/composite/{index}/_catalog_snapshot}</li>
 *   <li>{@code GET /_plugins/parquet/{index}/_analyze}</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class DataFormatStatsIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        // Real HTTP transport is required because tests use getRestClient() to hit /_plugins/* REST endpoints.
        return false;
    }

    private static final String INDEX_NAME = "test-dataformat-stats";

    // --- Stats API Tests ---

    @SuppressWarnings("unchecked")
    public void testDataFormatStatsIndexLevel() throws Exception {
        String idx = INDEX_NAME + "-index-level";
        createCompositeIndex(idx);
        indexDocs(idx, 20, 0);
        flushIndex(idx);
        refreshIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        assertThat(indices, notNullValue());
        assertTrue("Response should contain the test index", indices.containsKey(idx));

        Map<String, Object> indexStats = (Map<String, Object>) indices.get(idx);
        Map<String, Object> composite = (Map<String, Object>) indexStats.get("composite");
        assertThat(composite, notNullValue());

        Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
        assertThat(((Number) indexing.get("docs_indexed_total")).longValue(), equalTo(20L));
        assertThat(((Number) indexing.get("index_time_millis")).longValue(), greaterThan(0L));

        Map<String, Object> flush = (Map<String, Object>) composite.get("flush");
        assertThat(((Number) flush.get("flush_total")).longValue(), greaterThanOrEqualTo(1L));

        Map<String, Object> refresh = (Map<String, Object>) composite.get("refresh");
        assertThat(((Number) refresh.get("refresh_total")).longValue(), greaterThanOrEqualTo(1L));
    }

    @SuppressWarnings("unchecked")
    public void testDataFormatStatsShardLevel() throws Exception {
        String idx = INDEX_NAME + "-shard-level";
        createCompositeIndex(idx);
        indexDocs(idx, 20, 0);
        flushIndex(idx);
        refreshIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx + "?level=shards"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(idx);
        assertThat(indexStats, notNullValue());

        Map<String, Object> shards = (Map<String, Object>) indexStats.get("shards");
        assertThat("Shard-level detail should be present", shards, notNullValue());
        assertTrue("Shards should contain shard 0", shards.containsKey("0"));

        List<Map<String, Object>> shard0 = (List<Map<String, Object>>) shards.get("0");
        assertThat(shard0, notNullValue());
        assertThat(shard0.size(), greaterThan(0));

        for (Map<String, Object> shardEntry : shard0) {
            assertTrue("Shard entry should have primary field", shardEntry.containsKey("primary"));
            Map<String, Object> shardComposite = (Map<String, Object>) shardEntry.get("composite");
            assertThat(shardComposite, notNullValue());
            Map<String, Object> shardIndexing = (Map<String, Object>) shardComposite.get("indexing");
            assertThat(((Number) shardIndexing.get("docs_indexed_total")).longValue(), greaterThan(0L));
        }
    }

    @SuppressWarnings("unchecked")
    public void testDataFormatStatsWithShardFilter() throws Exception {
        String idx = INDEX_NAME + "-shard-filter";
        createCompositeIndex(idx);
        indexDocs(idx, 20, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx + "?shard=0"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        assertThat(indices, notNullValue());
        assertTrue("Response should contain the test index", indices.containsKey(idx));
    }

    @SuppressWarnings("unchecked")
    public void testDataFormatStatsWithNodeFilter() throws Exception {
        String idx = INDEX_NAME + "-node-filter";
        createCompositeIndex(idx);
        indexDocs(idx, 20, 0);
        flushIndex(idx);

        // Discover the data node hosting the primary shard. _local resolves to the coordinator node
        // which on a multi-role cluster may not host any shards — use the actual data node's ID.
        org.opensearch.cluster.routing.ShardRouting primary = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .routingTable()
            .index(idx)
            .shard(0)
            .primaryShard();
        String dataNodeId = primary.currentNodeId();
        assertThat("Primary shard should be assigned", dataNodeId, notNullValue());

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx + "?node=" + dataNodeId));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        assertThat(responseMap, notNullValue());
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        assertThat(indices, notNullValue());
        assertFalse("Response should not be empty when filtered to data node", indices.isEmpty());
        assertTrue("Response should contain the index", indices.containsKey(idx));
    }

    public void testDataFormatStatsInvalidLevel() throws Exception {
        String idx = INDEX_NAME + "-invalid-level";
        createCompositeIndex(idx);
        indexDocs(idx, 5, 0);
        flushIndex(idx);

        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx + "?level=invalid"));
            fail("Expected ResponseException for invalid level");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    public void testDataFormatStatsInvalidShard() throws Exception {
        String idx = INDEX_NAME + "-invalid-shard";
        createCompositeIndex(idx);
        indexDocs(idx, 5, 0);
        flushIndex(idx);

        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx + "?shard=99"));
            fail("Expected ResponseException for invalid shard");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    public void testDataFormatStatsNonExistentIndex() throws Exception {
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/non_existent_index"));
            fail("Expected ResponseException for non-existent index");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    // --- Catalog Snapshot Tests ---

    @SuppressWarnings("unchecked")
    public void testCatalogSnapshot() throws Exception {
        String idx = INDEX_NAME + "-catalog";
        createCompositeIndex(idx);
        indexDocs(idx, 10, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/composite/" + idx + "/_catalog_snapshot"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);

        List<Object> segments = (List<Object>) responseMap.get("segments");
        assertThat("segments array should exist", segments, notNullValue());
        assertThat("segments should have at least 1 entry", segments.size(), greaterThanOrEqualTo(1));

        Map<String, Object> summary = (Map<String, Object>) responseMap.get("summary");
        assertThat("summary should exist", summary, notNullValue());
        Map<String, Object> byFormat = (Map<String, Object>) summary.get("by_format");
        assertThat("by_format should exist", byFormat, notNullValue());
        assertTrue("by_format should contain parquet", byFormat.containsKey("parquet"));
    }

    @SuppressWarnings("unchecked")
    public void testCatalogSnapshotWithShardFilter() throws Exception {
        String idx = INDEX_NAME + "-catalog-shard";
        createCompositeIndex(idx);
        indexDocs(idx, 10, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/composite/" + idx + "/_catalog_snapshot?shard=0"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        assertThat(responseMap, notNullValue());
        List<Object> segments = (List<Object>) responseMap.get("segments");
        assertThat(segments, notNullValue());
    }

    public void testCatalogSnapshotNonExistentIndex() throws Exception {
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/composite/non_existent_index/_catalog_snapshot"));
            fail("Expected ResponseException for non-existent index");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    // --- Parquet Analyze Tests ---

    @SuppressWarnings("unchecked")
    public void testParquetAnalyze() throws Exception {
        String idx = INDEX_NAME + "-analyze";
        createCompositeIndex(idx);
        indexDocs(idx, 30, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_analyze"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);

        assertThat(((Number) responseMap.get("total_rows")).longValue(), equalTo(30L));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) responseMap.get("fields");
        assertThat("fields array should exist", fields, notNullValue());
        assertThat("fields should not be empty", fields.size(), greaterThan(0));

        for (Map<String, Object> field : fields) {
            Map<String, Object> stats = (Map<String, Object>) field.get("stats");
            assertThat("field should have stats", stats, notNullValue());
            assertTrue("stats should have min", stats.containsKey("min"));
            assertTrue("stats should have max", stats.containsKey("max"));
            assertThat("field should have num_values > 0", ((Number) field.get("num_values")).longValue(), greaterThan(0L));
            assertThat("has_bloom_filter should be boolean", field.get("has_bloom_filter"), instanceOf(Boolean.class));
        }

        assertThat("footer_size should be >= 0", ((Number) responseMap.get("footer_size")).longValue(), greaterThanOrEqualTo(0L));
    }

    @SuppressWarnings("unchecked")
    public void testParquetAnalyzeWithFileLevel() throws Exception {
        String idx = INDEX_NAME + "-analyze-file";
        createCompositeIndex(idx);
        indexDocs(idx, 10, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_analyze?file_level=true"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);

        List<Object> files = (List<Object>) responseMap.get("files");
        assertThat("files array should be present when file_level=true", files, notNullValue());
        assertThat("files should not be empty", files.size(), greaterThan(0));
    }

    public void testParquetAnalyzeNonExistentIndex() throws Exception {
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/non_existent_index/_analyze"));
            fail("Expected ResponseException for non-existent index");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    // ==== GAP-FILL TESTS: Stats ====

    @SuppressWarnings("unchecked")
    public void testStatsEmptyIndex() throws Exception {
        String idx = "stats-empty-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        refreshIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(idx);
        Map<String, Object> composite = (Map<String, Object>) indexStats.get("composite");
        Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
        assertThat(((Number) indexing.get("docs_indexed_total")).longValue(), equalTo(0L));

        Map<String, Object> flush = (Map<String, Object>) composite.get("flush");
        assertThat(((Number) flush.get("flush_total")).longValue(), greaterThanOrEqualTo(0L));

        Map<String, Object> refresh = (Map<String, Object>) composite.get("refresh");
        assertThat(((Number) refresh.get("refresh_total")).longValue(), greaterThanOrEqualTo(0L));
    }

    @SuppressWarnings("unchecked")
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "HANDOFF.md is silent on delete operation support; DataFormatAwareEngine.prepareDelete() "
        + "currently throws UnsupportedOperationException. This test documents desired behavior once delete is supported.")
    public void testStatsAfterDocumentDeletes() throws Exception {
        String idx = "stats-deletes-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        indexDocs(idx, 20, 0);
        refreshIndex(idx);

        for (int i = 0; i < 5; i++) {
            client().prepareDelete().setIndex(idx).setId(String.valueOf(i)).get();
        }
        refreshIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        Map<String, Object> composite = (Map<String, Object>) ((Map<String, Object>) indices.get(idx)).get("composite");
        Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
        assertThat(((Number) indexing.get("docs_indexed_total")).longValue(), equalTo(20L));
    }

    @SuppressWarnings("unchecked")
    public void testStatsAfterForceMerge() throws Exception {
        String idx = "stats-merge-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);

        indexDocs(idx, 10, 0);
        flushIndex(idx);
        indexDocs(idx, 10, 10);
        flushIndex(idx);
        indexDocs(idx, 10, 20);
        flushIndex(idx);

        client().admin().indices().prepareForceMerge(idx).setMaxNumSegments(1).get();

        assertBusy(() -> {
            Response resp = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx));
            Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, resp.getEntity().getContent(), true);
            Map<String, Object> indices = (Map<String, Object>) map.get("indices");
            Map<String, Object> composite = (Map<String, Object>) ((Map<String, Object>) indices.get(idx)).get("composite");
            Map<String, Object> merge = (Map<String, Object>) composite.get("merge");
            assertThat(merge, notNullValue());
            assertThat(((Number) merge.get("merges_total")).longValue(), greaterThanOrEqualTo(1L));
            assertThat(((Number) merge.get("merge_time_millis")).longValue(), greaterThan(0L));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testStatsParquetOnlyNoSecondary() throws Exception {
        String idx = "stats-parquet-only-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx, false);
        indexDocs(idx, 10, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        Map<String, Object> composite = (Map<String, Object>) ((Map<String, Object>) indices.get(idx)).get("composite");
        Map<String, Object> perFormat = (Map<String, Object>) composite.get("per_format");
        assertThat(perFormat, notNullValue());
        assertTrue("per_format should contain parquet", perFormat.containsKey("parquet"));
        assertFalse("per_format should NOT contain lucene", perFormat.containsKey("lucene"));
    }

    @SuppressWarnings("unchecked")
    public void testStatsClusterWide() throws Exception {
        // HANDOFF.md §2.1 specifies {index} as required path parameter — there is no cluster-wide
        // endpoint without an index. Use a wildcard pattern to test multi-index aggregation instead.
        String prefix = "stats-cw-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        String idx1 = prefix + "-a";
        String idx2 = prefix + "-b";
        createCompositeIndex(idx1);
        createCompositeIndex(idx2);
        indexDocs(idx1, 5, 0);
        indexDocs(idx2, 5, 0);
        flushIndex(idx1);
        flushIndex(idx2);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + prefix + "-*"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        assertThat(indices, notNullValue());
        assertTrue("Response should contain " + idx1, indices.containsKey(idx1));
        assertTrue("Response should contain " + idx2, indices.containsKey(idx2));
    }

    // ==== GAP-FILL TESTS: Catalog Snapshot ====

    @SuppressWarnings("unchecked")
    public void testCatalogSnapshotEmptyIndex() throws Exception {
        String idx = "catalog-empty-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/composite/" + idx + "/_catalog_snapshot"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        assertThat(responseMap, notNullValue());
    }

    @SuppressWarnings("unchecked")
    public void testCatalogSnapshotMultipleSegments() throws Exception {
        String idx = "catalog-multi-seg-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        indexDocs(idx, 5, 0);
        flushIndex(idx);
        indexDocs(idx, 5, 5);
        flushIndex(idx);
        indexDocs(idx, 5, 10);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/composite/" + idx + "/_catalog_snapshot"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        List<Object> segments = (List<Object>) responseMap.get("segments");
        assertThat(segments, notNullValue());
        assertThat("Should have at least 3 segments", segments.size(), greaterThanOrEqualTo(3));
    }

    @SuppressWarnings("unchecked")
    public void testCatalogSnapshotAfterMerge() throws Exception {
        String idx = "catalog-merge-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        indexDocs(idx, 5, 0);
        flushIndex(idx);
        indexDocs(idx, 5, 5);
        flushIndex(idx);
        indexDocs(idx, 5, 10);
        flushIndex(idx);

        client().admin().indices().prepareForceMerge(idx).setMaxNumSegments(1).get();

        assertBusy(() -> {
            Response resp = getRestClient().performRequest(new Request("GET", "/_plugins/composite/" + idx + "/_catalog_snapshot"));
            Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, resp.getEntity().getContent(), true);
            List<Object> segments = (List<Object>) map.get("segments");
            assertThat(segments, notNullValue());

            Map<String, Object> summary = (Map<String, Object>) map.get("summary");
            assertThat(summary, notNullValue());
            Map<String, Object> byFormat = (Map<String, Object>) summary.get("by_format");
            Map<String, Object> parquet = (Map<String, Object>) byFormat.get("parquet");
            assertThat(((Number) parquet.get("total_rows")).longValue(), equalTo(15L));
        }, 30, TimeUnit.SECONDS);
    }

    public void testCatalogSnapshotInvalidShard() throws Exception {
        String idx = "catalog-invalid-shard-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        indexDocs(idx, 5, 0);
        flushIndex(idx);

        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/composite/" + idx + "/_catalog_snapshot?shard=99"));
            fail("Expected ResponseException for invalid shard");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    // ==== GAP-FILL TESTS: Parquet Analyze ====

    @SuppressWarnings("unchecked")
    public void testAnalyzeMultipleFieldTypes() throws Exception {
        String idx = "analyze-types-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndexWithMultiTypeMapping(idx);
        ApiTestFixtures.indexMultiTypeDocs(client(), idx, 20);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_analyze"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        List<Map<String, Object>> fields = (List<Map<String, Object>>) responseMap.get("fields");
        assertThat(fields, notNullValue());
        assertThat("Should have fields for all types", fields.size(), greaterThanOrEqualTo(6));
    }

    @SuppressWarnings("unchecked")
    public void testAnalyzeNullValues() throws Exception {
        String idx = "analyze-nulls-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndexWithNullableMapping(idx);
        ApiTestFixtures.indexNullableDocs(client(), idx, 30, 0.3);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_analyze"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        List<Map<String, Object>> fields = (List<Map<String, Object>>) responseMap.get("fields");
        assertThat(fields, notNullValue());

        boolean foundNulls = false;
        for (Map<String, Object> field : fields) {
            Number nullCount = (Number) field.get("null_count");
            if (nullCount != null && nullCount.longValue() > 0) {
                foundNulls = true;
                assertThat(((Number) field.get("num_values")).longValue(), greaterThanOrEqualTo(0L));
                break;
            }
        }
        assertTrue("At least one field should have null_count > 0", foundNulls);
    }

    @SuppressWarnings("unchecked")
    public void testAnalyzeAfterMerge() throws Exception {
        String idx = "analyze-merge-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        indexDocs(idx, 5, 0);
        flushIndex(idx);
        indexDocs(idx, 5, 5);
        flushIndex(idx);
        indexDocs(idx, 5, 10);
        flushIndex(idx);

        client().admin().indices().prepareForceMerge(idx).setMaxNumSegments(1).get();

        assertBusy(() -> {
            Response resp = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_analyze"));
            Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, resp.getEntity().getContent(), true);
            assertThat(((Number) map.get("total_rows")).longValue(), equalTo(15L));
        }, 30, TimeUnit.SECONDS);
    }

    // --- Helper methods for gap-fill tests ---

    private void createCompositeIndexWithMultiTypeMapping(String indexName) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene");

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settingsBuilder)
            .setMapping(
                "kw",
                "type=keyword",
                "int_v",
                "type=integer",
                "long_v",
                "type=long",
                "dbl_v",
                "type=double",
                "bool_v",
                "type=boolean",
                "date_v",
                "type=date"
            )
            .get();
        ensureGreen(indexName);
    }

    private void createCompositeIndexWithNullableMapping(String indexName) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene");

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settingsBuilder)
            .setMapping("kw", "type=keyword", "int_v", "type=integer", "dbl_v", "type=double")
            .get();
        ensureGreen(indexName);
    }

    // ==== NEW FORMAT-SPECIFIC STATS ENDPOINTS ====

    @SuppressWarnings("unchecked")
    public void testParquetIndexStats() throws Exception {
        String idx = "parquet-stats-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        indexDocs(idx, 20, 0);
        flushIndex(idx);
        Map<String, Object> stats = DataFormatApiTestUtils.getParquetStats(getRestClient(), idx, Collections.emptyMap());
        Map<String, Object> parquet = DataFormatApiTestUtils.mapAt(stats, "indices", idx, "parquet");
        assertThat("Parquet stats should be present", parquet, notNullValue());
    }

    @SuppressWarnings("unchecked")
    public void testLuceneIndexStats() throws Exception {
        String idx = "lucene-stats-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx, true);
        indexDocs(idx, 20, 0);
        flushIndex(idx);
        Map<String, Object> stats = DataFormatApiTestUtils.getLuceneStats(getRestClient(), idx, Collections.emptyMap());
        Map<String, Object> lucene = DataFormatApiTestUtils.mapAt(stats, "indices", idx, "lucene");
        assertThat("Lucene stats should be present", lucene, notNullValue());
    }

    @SuppressWarnings("unchecked")
    public void testParquetIndexStatsNonExistentIndex() throws Exception {
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/nonexistent-index/_stats"));
            fail("Expected 404 for non-existent index");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    @SuppressWarnings("unchecked")
    public void testParquetNodeStatsAllNodes() throws Exception {
        String idx = "parquet-nodes-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx);
        indexDocs(idx, 15, 0);
        flushIndex(idx);
        Map<String, Object> stats = DataFormatApiTestUtils.getParquetNodeStats(getRestClient(), null, Collections.emptyMap());
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        assertThat("Nodes map should not be null", nodes, notNullValue());
        assertThat("At least one node should report parquet stats", nodes.size(), greaterThan(0));
    }

    @SuppressWarnings("unchecked")
    public void testLuceneNodeStatsAllNodes() throws Exception {
        String idx = "lucene-nodes-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createCompositeIndex(idx, true);
        indexDocs(idx, 15, 0);
        flushIndex(idx);
        Map<String, Object> stats = DataFormatApiTestUtils.getLuceneNodeStats(getRestClient(), null, Collections.emptyMap());
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        assertThat("Nodes map should not be null", nodes, notNullValue());
        assertThat("At least one node should report lucene stats", nodes.size(), greaterThan(0));
    }

    @SuppressWarnings("unchecked")
    public void testParquetNodeStatsInvalidNode() throws Exception {
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/_nodes/nonexistent-node/_stats"));
            fail("Expected 400 for non-existent node");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }
}
