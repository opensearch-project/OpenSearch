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
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

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

    @SuppressWarnings("unchecked")
    public void testDataFormatStats() throws Exception {
        createCompositeIndex(INDEX_NAME + "-stats");
        indexDocs(INDEX_NAME + "-stats", 10, 0);
        flushIndex(INDEX_NAME + "-stats");

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats"));
        assertThat(response.getStatusLine().getStatusCode(), org.hamcrest.Matchers.equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            true
        );

        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        assertThat(indices, notNullValue());
        assertTrue("Response should contain the test index", indices.containsKey(INDEX_NAME + "-stats"));

        Map<String, Object> indexStats = (Map<String, Object>) indices.get(INDEX_NAME + "-stats");
        Map<String, Object> composite = (Map<String, Object>) indexStats.get("composite");
        assertThat(composite, notNullValue());

        // Verify per_format breakdown exists with parquet stats
        Map<String, Object> perFormat = (Map<String, Object>) composite.get("per_format");
        assertThat("per_format breakdown should exist", perFormat, notNullValue());
        assertTrue("per_format should contain parquet stats", perFormat.containsKey("parquet"));
    }

    @SuppressWarnings("unchecked")
    public void testDataFormatStatsWithShardLevel() throws Exception {
        createCompositeIndex(INDEX_NAME + "-shard");
        indexDocs(INDEX_NAME + "-shard", 10, 0);
        flushIndex(INDEX_NAME + "-shard");

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats?level=shards"));
        assertThat(response.getStatusLine().getStatusCode(), org.hamcrest.Matchers.equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            true
        );

        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(INDEX_NAME + "-shard");
        assertThat(indexStats, notNullValue());

        // Verify shard-level detail is present
        Map<String, Object> shards = (Map<String, Object>) indexStats.get("shards");
        assertThat("Shard-level detail should be present when level=shards", shards, notNullValue());
        assertFalse("Shards map should not be empty", shards.isEmpty());
    }

    @SuppressWarnings("unchecked")
    public void testCatalogSnapshot() throws Exception {
        createCompositeIndex(INDEX_NAME + "-catalog");
        indexDocs(INDEX_NAME + "-catalog", 10, 0);
        flushIndex(INDEX_NAME + "-catalog");

        Response response = getRestClient().performRequest(
            new Request("GET", "/_plugins/composite/" + INDEX_NAME + "-catalog/_catalog_snapshot")
        );
        assertThat(response.getStatusLine().getStatusCode(), org.hamcrest.Matchers.equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            true
        );

        assertEquals(INDEX_NAME + "-catalog", responseMap.get("index"));
        assertThat(responseMap.get("generation"), notNullValue());

        List<Object> segments = (List<Object>) responseMap.get("segments");
        assertThat("segments array should exist", segments, notNullValue());
        assertThat("segments should not be empty", segments.size(), greaterThan(0));

        Map<String, Object> summary = (Map<String, Object>) responseMap.get("summary");
        assertThat("summary should exist", summary, notNullValue());
        assertThat("summary should have by_format", summary.get("by_format"), notNullValue());
        assertThat("summary should have by_extension", summary.get("by_extension"), notNullValue());
    }

    @SuppressWarnings("unchecked")
    public void testParquetAnalyze() throws Exception {
        createCompositeIndex(INDEX_NAME + "-analyze");
        indexDocs(INDEX_NAME + "-analyze", 10, 0);
        flushIndex(INDEX_NAME + "-analyze");

        Response response = getRestClient().performRequest(
            new Request("GET", "/_plugins/parquet/" + INDEX_NAME + "-analyze/_analyze")
        );
        assertThat(response.getStatusLine().getStatusCode(), org.hamcrest.Matchers.equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            true
        );

        assertEquals(INDEX_NAME + "-analyze", responseMap.get("index"));
        assertThat("total_rows should be > 0", ((Number) responseMap.get("total_rows")).longValue(), greaterThan(0L));
        assertThat("total_size_bytes should be > 0", ((Number) responseMap.get("total_size_bytes")).longValue(), greaterThan(0L));

        List<Map<String, Object>> fields = (List<Map<String, Object>>) responseMap.get("fields");
        assertThat("fields array should exist", fields, notNullValue());
        assertThat("fields should not be empty", fields.size(), greaterThan(0));

        // Verify each field has required attributes
        for (Map<String, Object> field : fields) {
            assertThat("field should have name", field.get("name"), notNullValue());
            assertThat("field should have type", field.get("type"), notNullValue());
            assertThat("field should have compression", field.get("compression"), notNullValue());
            assertThat("field should have encodings", field.get("encodings"), notNullValue());
            assertThat("field should have total_compressed_bytes", field.get("total_compressed_bytes"), notNullValue());
            assertThat("field should have total_uncompressed_bytes", field.get("total_uncompressed_bytes"), notNullValue());
        }

        // Verify compression_ratio > 0 for at least one field
        boolean hasPositiveRatio = fields.stream()
            .anyMatch(f -> ((Number) f.get("compression_ratio")).doubleValue() > 0);
        assertTrue("At least one field should have compression_ratio > 0", hasPositiveRatio);

        // Verify footer_size exists and is >= 0
        assertThat("footer_size should exist", responseMap.get("footer_size"), notNullValue());
        assertThat("footer_size should be >= 0", ((Number) responseMap.get("footer_size")).longValue(), greaterThanOrEqualTo(0L));

        // Verify sorting_columns field exists (may be null if no sort configured)
        assertTrue("sorting_columns field should exist", responseMap.containsKey("sorting_columns"));

        // Verify each field has new required attributes
        for (Map<String, Object> field : fields) {
            assertThat("field should have num_values > 0", ((Number) field.get("num_values")).longValue(), greaterThan(0L));
            assertThat("field has_bloom_filter should be boolean", field.get("has_bloom_filter"), instanceOf(Boolean.class));
            assertThat(
                "field bloom_filter_size should be >= 0",
                ((Number) field.get("bloom_filter_size")).longValue(),
                greaterThanOrEqualTo(0L)
            );
            assertThat("field should have total_num_pages >= 0", ((Number) field.get("total_num_pages")).longValue(),
                greaterThanOrEqualTo(0L));

            Map<String, Object> stats = (Map<String, Object>) field.get("stats");
            assertThat("field should have stats object", stats, notNullValue());
            assertTrue("stats should have min", stats.containsKey("min"));
            assertTrue("stats should have max", stats.containsKey("max"));
            assertTrue("stats should have null_count", stats.containsKey("null_count"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testParquetAnalyzeWithFileLevel() throws Exception {
        createCompositeIndex(INDEX_NAME + "-file");
        indexDocs(INDEX_NAME + "-file", 10, 0);
        flushIndex(INDEX_NAME + "-file");

        Response response = getRestClient().performRequest(
            new Request("GET", "/_plugins/parquet/" + INDEX_NAME + "-file/_analyze?file_level=true")
        );
        assertThat(response.getStatusLine().getStatusCode(), org.hamcrest.Matchers.equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            true
        );

        List<Object> files = (List<Object>) responseMap.get("files");
        assertThat("files array should be present when file_level=true", files, notNullValue());
        assertThat("files should not be empty", files.size(), greaterThan(0));

        // Verify each file entry has row_groups with columns and page_stats
        boolean foundPageStats = false;
        for (Object fileObj : files) {
            Map<String, Object> file = (Map<String, Object>) fileObj;
            List<Map<String, Object>> rowGroups = (List<Map<String, Object>>) file.get("row_groups");
            assertThat("file entry should have row_groups", rowGroups, notNullValue());

            for (Map<String, Object> rowGroup : rowGroups) {
                List<Map<String, Object>> columns = (List<Map<String, Object>>) rowGroup.get("columns");
                assertThat("row_group should have columns", columns, notNullValue());

                for (Map<String, Object> column : columns) {
                    Map<String, Object> pageStats = (Map<String, Object>) column.get("page_stats");
                    if (pageStats != null) {
                        foundPageStats = true;
                        assertThat("page_stats should have num_pages", pageStats.get("num_pages"), notNullValue());
                        assertThat("page_stats should have boundary_order", pageStats.get("boundary_order"), notNullValue());
                    }
                }
            }
        }
        assertTrue("At least one column should have page_stats with num_pages and boundary_order", foundPageStats);
    }
}
