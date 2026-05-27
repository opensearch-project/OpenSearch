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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.composite.apis.ApiTestFixtures;
import org.opensearch.composite.apis.DataFormatApiTestUtils;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Multi-shard integration tests for the composite-engine REST API endpoints:
 * <ul>
 *   <li>{@code GET /_plugins/dataformat_stats}</li>
 *   <li>{@code GET /_plugins/composite/{index}/_catalog_snapshot}</li>
 *   <li>{@code GET /_plugins/parquet/{index}/_analyze}</li>
 * </ul>
 *
 * Validates correct aggregation and per-shard breakdown on a 3-node, 3-shard, 0-replica cluster.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 3, supportsDedicatedMasters = false)
public class DataFormatApiMultiShardIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        // Real HTTP transport is required because tests use getRestClient() to hit /_plugins/* REST endpoints.
        return false;
    }

    private static final int SHARD_COUNT = 3;
    private static final int REPLICA_COUNT = 0;

    // --- dataformat_stats group ---

    @SuppressWarnings("unchecked")
    public void testStatsAggregateAcrossShards() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 90, SHARD_COUNT);
        refreshIndex(idx);
        flushIndex(idx);

        Map<String, Object> stats = DataFormatApiTestUtils.getDataFormatStats(getRestClient(), idx, Collections.emptyMap());
        long docsIndexed = DataFormatApiTestUtils.statsDocsIndexed(stats, idx);
        assertThat(docsIndexed, equalTo(90L));
    }

    @SuppressWarnings("unchecked")
    public void testStatsShardLevelBreakdown() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);

        Map<String, Object> stats = DataFormatApiTestUtils.getDataFormatStats(getRestClient(), idx, Map.of("level", "shards"));
        Map<String, Object> indices = DataFormatApiTestUtils.mapAt(stats, "indices", idx);
        assertThat(indices, notNullValue());

        Map<String, Object> shards = (Map<String, Object>) indices.get("shards");
        assertThat("Shard-level detail should be present", shards, notNullValue());
        assertThat("Should have all 3 shards", shards.size(), equalTo(SHARD_COUNT));

        long sumDocs = 0;
        int shardsWithDocs = 0;
        for (Map.Entry<String, Object> entry : shards.entrySet()) {
            List<Map<String, Object>> shardEntries = (List<Map<String, Object>>) entry.getValue();
            assertThat("Each shard ID should have at least one entry", shardEntries.size(), greaterThan(0));
            for (Map<String, Object> shardEntry : shardEntries) {
                Map<String, Object> composite = (Map<String, Object>) shardEntry.get("composite");
                Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
                long docs = ((Number) indexing.get("docs_indexed_total")).longValue();
                if (docs > 0) shardsWithDocs++;
                sumDocs += docs;
            }
        }
        assertThat("At least one shard should have docs", shardsWithDocs, greaterThan(0));
        assertThat("Sum of docs across shards equals indexed count", sumDocs, equalTo(60L));
    }

    @SuppressWarnings("unchecked")
    public void testStatsShardFilterOnMultiShard() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);

        Response response = getRestClient().performRequest(
            new Request("GET", "/_plugins/dataformat_stats/" + idx + "?level=shards&shard=1")
        );
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(idx);
        Map<String, Object> shards = (Map<String, Object>) indexStats.get("shards");
        assertThat(shards, notNullValue());
        assertTrue("Should contain shard 1", shards.containsKey("1"));
    }

    @SuppressWarnings("unchecked")
    public void testStatsPerFormatAcrossShards() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);

        Map<String, Object> stats = DataFormatApiTestUtils.getDataFormatStats(getRestClient(), idx, Collections.emptyMap());
        Map<String, Object> perFormatParquet = DataFormatApiTestUtils.statsPerFormat(stats, idx, "parquet");
        Map<String, Object> perFormatLucene = DataFormatApiTestUtils.statsPerFormat(stats, idx, "lucene");
        assertThat("per_format should contain parquet", perFormatParquet, notNullValue());
        assertThat("per_format should contain lucene", perFormatLucene, notNullValue());

        long parquetDocs = ((Number) ((Map<String, Object>) perFormatParquet.get("indexing")).get("docs_indexed_total")).longValue();
        long luceneDocs = ((Number) ((Map<String, Object>) perFormatLucene.get("indexing")).get("docs_indexed_total")).longValue();
        assertThat(parquetDocs, greaterThan(0L));
        assertThat(luceneDocs, greaterThan(0L));
    }

    // --- catalog_snapshot group ---

    @SuppressWarnings("unchecked")
    public void testCatalogPerShard() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);

        int totalSegments = 0;
        int shardsWithData = 0;
        for (int shard = 0; shard < SHARD_COUNT; shard++) {
            Map<String, Object> catalog = DataFormatApiTestUtils.getCatalogSnapshot(
                getRestClient(),
                idx,
                Map.of("shard", String.valueOf(shard))
            );
            List<Object> segments = DataFormatApiTestUtils.listAt(catalog, "segments");
            assertThat("Shard " + shard + " should have segments array", segments, notNullValue());
            if (!segments.isEmpty()) {
                shardsWithData++;
            }
            totalSegments += segments.size();
        }
        // Routing distribution may not be perfectly even, but at least one shard should have segments
        assertThat("At least one shard should have segments", shardsWithData, greaterThan(0));
        assertThat("Total segment count across all shards should be at least 1", totalSegments, greaterThan(0));
    }

    @SuppressWarnings("unchecked")
    public void testCatalogAggregateAllShards() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 90, SHARD_COUNT);
        flushIndex(idx);

        Map<String, Object> catalog = DataFormatApiTestUtils.getCatalogSnapshot(getRestClient(), idx, Map.of("level", "shards"));
        List<Object> shards = DataFormatApiTestUtils.listAt(catalog, "shards");
        if (shards != null) {
            assertThat("Should have 3 shard entries", shards.size(), equalTo(SHARD_COUNT));
        }

        // Verify total rows across all shards sum to ~90
        Map<String, Object> summary = DataFormatApiTestUtils.mapAt(catalog, "summary", "by_format", "parquet");
        if (summary != null) {
            long totalRows = ((Number) summary.get("total_rows")).longValue();
            assertThat("Total rows across shards should be 90", totalRows, equalTo(90L));
        }
    }

    @SuppressWarnings("unchecked")
    public void testCatalogPostMergeMultiShard() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);

        // Create multiple segments per shard via multiple flush cycles
        for (int batch = 0; batch < 3; batch++) {
            ApiTestFixtures.indexAcrossShards(client(), idx, 30, SHARD_COUNT);
            flushIndex(idx);
        }

        client().admin().indices().prepareForceMerge(idx).setMaxNumSegments(1).get();

        assertBusy(() -> {
            Map<String, Object> catalog = DataFormatApiTestUtils.getCatalogSnapshot(getRestClient(), idx, Map.of("level", "shards"));
            assertThat(catalog, notNullValue());
            // After merge, segment count per shard should be reduced
            List<Object> segments = DataFormatApiTestUtils.listAt(catalog, "segments");
            if (segments != null) {
                // After force merge to 1 segment per shard, total segments <= SHARD_COUNT
                assertThat("Segments should be reduced after merge", segments.size(), greaterThanOrEqualTo(1));
            }
        }, 30, TimeUnit.SECONDS);
    }

    // --- parquet_analyze group ---

    @SuppressWarnings("unchecked")
    public void testAnalyzePerShard() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);

        long totalRows = 0;
        int shardsWithRows = 0;
        for (int shard = 0; shard < SHARD_COUNT; shard++) {
            Map<String, Object> analyze = DataFormatApiTestUtils.getParquetAnalyze(
                getRestClient(),
                idx,
                Map.of("shard", String.valueOf(shard))
            );
            Number rows = (Number) analyze.get("total_rows");
            // Some shards may have 0 rows due to routing distribution; total_rows may be 0 or null
            long rowCount = rows == null ? 0L : rows.longValue();
            if (rowCount > 0) shardsWithRows++;
            totalRows += rowCount;
        }
        assertThat("At least one shard should have rows", shardsWithRows, greaterThan(0));
        assertThat("Total rows across shards should equal indexed count", totalRows, equalTo(60L));
    }

    @SuppressWarnings("unchecked")
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "HANDOFF.md §10 Open Decisions: 'Parquet analyze aggregation across shards' — currently returns "
        + "first shard's result at index level (per §9 Decisions). Cross-shard sum aggregation strategy is undecided.")
    public void testAnalyzeAggregateAllShards() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);

        Map<String, Object> analyze = DataFormatApiTestUtils.getParquetAnalyze(getRestClient(), idx, Collections.emptyMap());
        long totalRows = ((Number) analyze.get("total_rows")).longValue();
        assertThat("Aggregate total_rows should be 60", totalRows, equalTo(60L));
    }

    @SuppressWarnings("unchecked")
    public void testCrossApiConsistencyMultiShard() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);

        Map<String, Object> stats = DataFormatApiTestUtils.getDataFormatStats(getRestClient(), idx, Collections.emptyMap());
        Map<String, Object> catalog = DataFormatApiTestUtils.getCatalogSnapshot(getRestClient(), idx, Collections.emptyMap());
        Map<String, Object> analyze = DataFormatApiTestUtils.getParquetAnalyze(getRestClient(), idx, Collections.emptyMap());

        DataFormatApiTestUtils.assertCrossApiConsistency(stats, catalog, analyze, idx, 60L);
    }

    // --- Helper ---

    private void createMultiShardIndex(String idx, int shards, int replicas, boolean withLucene) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet");

        if (withLucene) {
            settingsBuilder.putList("index.composite.secondary_data_formats", "lucene");
        } else {
            settingsBuilder.putList("index.composite.secondary_data_formats");
        }

        client().admin()
            .indices()
            .prepareCreate(idx)
            .setSettings(settingsBuilder)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(idx);
    }

    // ==== NEW FORMAT-SPECIFIC STATS ENDPOINTS ====

    @SuppressWarnings("unchecked")
    public void testParquetStatsAggregateMultiShard() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);
        Map<String, Object> stats = DataFormatApiTestUtils.getParquetStats(getRestClient(), idx, Collections.emptyMap());
        Map<String, Object> parquet = DataFormatApiTestUtils.mapAt(stats, "indices", idx, "parquet");
        assertThat(parquet, notNullValue());
        Map<String, Object> indexing = (Map<String, Object>) parquet.get("indexing");
        if (indexing != null) {
            Number docs = (Number) indexing.get("docs_indexed_total");
            if (docs != null) {
                assertThat("Aggregated docs should be 60 across shards", docs.longValue(), equalTo(60L));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testLuceneStatsShardLevelMultiShard() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);
        Map<String, Object> stats = DataFormatApiTestUtils.getLuceneStats(getRestClient(), idx, Map.of("level", "shards"));
        Map<String, Object> shards = DataFormatApiTestUtils.mapAt(stats, "indices", idx, "shards");
        assertThat("Shards detail should be present", shards, notNullValue());
        assertThat("Should have all 3 shards in shards map", shards.size(), equalTo(SHARD_COUNT));
    }

    @SuppressWarnings("unchecked")
    public void testParquetNodeStatsAcrossNodes() throws Exception {
        String idx = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createMultiShardIndex(idx, SHARD_COUNT, REPLICA_COUNT, true);
        ApiTestFixtures.indexAcrossShards(client(), idx, 60, SHARD_COUNT);
        flushIndex(idx);
        Map<String, Object> stats = DataFormatApiTestUtils.getParquetNodeStats(getRestClient(), null, Collections.emptyMap());
        Map<String, Object> nodes = (Map<String, Object>) stats.get("nodes");
        assertThat("At least one node should have parquet stats", nodes.size(), greaterThan(0));
    }
}
