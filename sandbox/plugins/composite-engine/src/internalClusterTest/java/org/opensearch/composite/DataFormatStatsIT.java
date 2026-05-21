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
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

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

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx + "?node=_local"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        assertThat(responseMap, notNullValue());
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        assertThat(indices, notNullValue());
        assertFalse("Response should not be empty for _local node", indices.isEmpty());
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
}
