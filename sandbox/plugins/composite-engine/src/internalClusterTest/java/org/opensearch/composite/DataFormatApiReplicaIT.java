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
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.composite.apis.DataFormatApiTestUtils;
import org.opensearch.plugins.Plugin;
import org.opensearch.transport.Netty4ModulePlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests validating all 3 REST APIs (stats, catalog, analyze) on a
 * 2-node, 1-shard, 1-replica cluster using segment replication + remote store.
 *
 * <p>Extends {@link DataFormatAwareReplicationBaseIT} which provides the required
 * segment replication and remote store infrastructure for DFA replica testing.
 *
 * @opensearch.experimental
 */
public class DataFormatApiReplicaIT extends DataFormatAwareReplicationBaseIT {

    @Override
    protected boolean addMockHttpTransport() {
        // Real HTTP transport is required because tests use getRestClient() to hit /_plugins/* REST endpoints.
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Add Netty4ModulePlugin to provide a real HTTP server transport. Base class only adds composite plugins.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(Netty4ModulePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
    }

    private static final String IDX = "dfa-api-replica-idx";

    private void createReplicaIndex() throws Exception {
        createDfaIndex(1);
    }

    private void indexAndFlush(int docCount) {
        indexDocs(docCount);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();
    }

    private void waitForReplicaSync() throws Exception {
        assertBusy(() -> {
            var health = client().admin().cluster().prepareHealth(INDEX_NAME).setWaitForActiveShards(2).get();
            assertThat(health.getActiveShards(), greaterThanOrEqualTo(2));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getJson(String path) throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", path));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
    }

    // --- Stats API Tests ---

    @SuppressWarnings("unchecked")
    public void testStatsPrimaryReplicaConsistency() throws Exception {
        createReplicaIndex();
        indexAndFlush(30);
        waitForReplicaSync();

        assertBusy(() -> {
            Map<String, Object> resp = getJson("/_plugins/dataformat_stats/" + INDEX_NAME + "?level=shards");
            Map<String, Object> indices = (Map<String, Object>) resp.get("indices");
            Map<String, Object> indexStats = (Map<String, Object>) indices.get(INDEX_NAME);
            Map<String, Object> shards = (Map<String, Object>) indexStats.get("shards");
            assertThat("shards map should be present", shards, notNullValue());

            List<Map<String, Object>> shard0 = (List<Map<String, Object>>) shards.get("0");
            assertThat("shard 0 entries should exist", shard0, notNullValue());
            assertThat("should have primary + replica entries", shard0.size(), equalTo(2));

            for (Map<String, Object> entry : shard0) {
                Map<String, Object> composite = (Map<String, Object>) entry.get("composite");
                Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
                long docsIndexed = ((Number) indexing.get("docs_indexed_total")).longValue();
                assertThat("each shard copy should reflect 30 docs", docsIndexed, equalTo(30L));
            }
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testStatsNotDoubleCountedWithReplica() throws Exception {
        createReplicaIndex();
        indexAndFlush(50);
        waitForReplicaSync();

        assertBusy(() -> {
            Map<String, Object> resp = getJson("/_plugins/dataformat_stats/" + INDEX_NAME);
            Map<String, Object> indices = (Map<String, Object>) resp.get("indices");
            Map<String, Object> indexStats = (Map<String, Object>) indices.get(INDEX_NAME);
            Map<String, Object> composite = (Map<String, Object>) indexStats.get("composite");
            Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
            long docsIndexed = ((Number) indexing.get("docs_indexed_total")).longValue();
            // Index-level must NOT double-count: should be 50, not 100
            assertThat("docs_indexed_total must not double-count replicas", docsIndexed, equalTo(50L));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testStatsShardLevelShowsPrimaryAndReplica() throws Exception {
        createReplicaIndex();
        indexAndFlush(30);
        waitForReplicaSync();

        assertBusy(() -> {
            Map<String, Object> resp = getJson("/_plugins/dataformat_stats/" + INDEX_NAME + "?level=shards");
            Map<String, Object> indices = (Map<String, Object>) resp.get("indices");
            Map<String, Object> indexStats = (Map<String, Object>) indices.get(INDEX_NAME);
            Map<String, Object> shards = (Map<String, Object>) indexStats.get("shards");
            assertThat(shards, notNullValue());

            List<Map<String, Object>> shard0 = (List<Map<String, Object>>) shards.get("0");
            assertThat(shard0, notNullValue());
            assertThat("should have 2 entries (primary + replica)", shard0.size(), equalTo(2));

            boolean foundPrimary = false;
            boolean foundReplica = false;
            for (Map<String, Object> entry : shard0) {
                Boolean isPrimary = (Boolean) entry.get("primary");
                assertThat("entry must have 'primary' field", isPrimary, notNullValue());
                if (isPrimary) foundPrimary = true;
                else foundReplica = true;
            }
            assertTrue("should have a primary shard entry", foundPrimary);
            assertTrue("should have a replica shard entry", foundReplica);
        }, 30, TimeUnit.SECONDS);
    }

    // --- Catalog Snapshot API Tests ---

    @SuppressWarnings("unchecked")
    public void testCatalogPrimaryVsReplica() throws Exception {
        createReplicaIndex();
        indexAndFlush(30);
        waitForReplicaSync();

        assertBusy(() -> {
            Map<String, Object> resp = getJson("/_plugins/composite/" + INDEX_NAME + "/_catalog_snapshot?level=shards");
            // Expect shards array with 2 entries (primary + replica)
            List<Object> shards = (List<Object>) resp.get("shards");
            if (shards == null) {
                // Alternative structure: might be nested under index
                Map<String, Object> indices = (Map<String, Object>) resp.get("indices");
                if (indices != null) {
                    Map<String, Object> indexData = (Map<String, Object>) indices.get(INDEX_NAME);
                    shards = (List<Object>) indexData.get("shards");
                }
            }
            assertThat("shards array should be present at shard level", shards, notNullValue());
            assertThat("should have 2 shard entries (primary + replica)", shards.size(), equalTo(2));

            for (Object shardObj : shards) {
                Map<String, Object> shard = (Map<String, Object>) shardObj;
                List<Object> segments = (List<Object>) shard.get("segments");
                assertThat("each shard copy should have segments", segments, notNullValue());
                assertThat("segments should not be empty", segments.size(), greaterThanOrEqualTo(1));

                Map<String, Object> summary = (Map<String, Object>) shard.get("summary");
                assertThat("summary should exist", summary, notNullValue());
                Map<String, Object> byFormat = (Map<String, Object>) summary.get("by_format");
                Map<String, Object> parquet = (Map<String, Object>) byFormat.get("parquet");
                long totalRows = ((Number) parquet.get("total_rows")).longValue();
                assertThat("each shard copy should have 30 total_rows", totalRows, equalTo(30L));
            }
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testCatalogAfterReplicaSync() throws Exception {
        createReplicaIndex();
        indexAndFlush(20);
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        waitForReplicaSync();

        // Use the base class convergence assertion which checks catalog files match
        assertCatalogSnapshotsConverged(INDEX_NAME);
    }

    // --- Parquet Analyze API Tests ---

    @SuppressWarnings("unchecked")
    public void testAnalyzePrimaryVsReplica() throws Exception {
        createReplicaIndex();
        indexAndFlush(30);
        waitForReplicaSync();

        assertBusy(() -> {
            Map<String, Object> resp = getJson("/_plugins/parquet/" + INDEX_NAME + "/_analyze?level=shards");
            // Expect response handles both primary and replica
            // Structure may have shards array or per-shard breakdown
            List<Object> shards = (List<Object>) resp.get("shards");
            if (shards != null) {
                assertThat("should have 2 shard entries", shards.size(), equalTo(2));
                for (Object shardObj : shards) {
                    Map<String, Object> shard = (Map<String, Object>) shardObj;
                    long totalRows = ((Number) shard.get("total_rows")).longValue();
                    assertThat("each shard copy should report 30 rows", totalRows, equalTo(30L));

                    List<Map<String, Object>> fields = (List<Map<String, Object>>) shard.get("fields");
                    assertThat("fields should be present", fields, notNullValue());
                    assertThat("fields should not be empty", fields.size(), greaterThanOrEqualTo(1));
                }
            } else {
                // If no shard-level breakdown, at least verify index-level is correct
                long totalRows = ((Number) resp.get("total_rows")).longValue();
                assertThat("total_rows should be 30", totalRows, equalTo(30L));
            }
        }, 30, TimeUnit.SECONDS);
    }

    // --- Cross-API Consistency ---

    @SuppressWarnings("unchecked")
    public void testCrossApiConsistencyWithReplica() throws Exception {
        createReplicaIndex();
        indexAndFlush(40);
        waitForReplicaSync();

        assertBusy(() -> {
            // Stats
            Map<String, Object> stats = getJson("/_plugins/dataformat_stats/" + INDEX_NAME);
            Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
            Map<String, Object> indexStats = (Map<String, Object>) indices.get(INDEX_NAME);
            Map<String, Object> composite = (Map<String, Object>) indexStats.get("composite");
            Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
            long statsDocsIndexed = ((Number) indexing.get("docs_indexed_total")).longValue();

            // Catalog
            Map<String, Object> catalog = getJson("/_plugins/composite/" + INDEX_NAME + "/_catalog_snapshot");
            Map<String, Object> summary = (Map<String, Object>) catalog.get("summary");
            Map<String, Object> byFormat = (Map<String, Object>) summary.get("by_format");
            Map<String, Object> parquet = (Map<String, Object>) byFormat.get("parquet");
            long catalogRows = ((Number) parquet.get("total_rows")).longValue();

            // Analyze
            Map<String, Object> analyze = getJson("/_plugins/parquet/" + INDEX_NAME + "/_analyze");
            long analyzeRows = ((Number) analyze.get("total_rows")).longValue();

            // All three must agree on 40 docs
            assertThat("stats docs_indexed_total", statsDocsIndexed, equalTo(40L));
            assertThat("catalog total_rows", catalogRows, equalTo(40L));
            assertThat("analyze total_rows", analyzeRows, equalTo(40L));
        }, 30, TimeUnit.SECONDS);
    }

    // ==== NEW FORMAT-SPECIFIC STATS ENDPOINTS ====

    @SuppressWarnings("unchecked")
    public void testParquetStatsWithReplica() throws Exception {
        createReplicaIndex();
        indexAndFlush(30);
        waitForReplicaSync();
        Map<String, Object> stats = DataFormatApiTestUtils.getParquetStats(getRestClient(), INDEX_NAME, Collections.emptyMap());
        Map<String, Object> parquet = DataFormatApiTestUtils.mapAt(stats, "indices", INDEX_NAME, "parquet");
        assertThat("Parquet stats should be present with replica", parquet, notNullValue());
    }

    @SuppressWarnings("unchecked")
    public void testLuceneStatsWithReplica() throws Exception {
        // The replica test cluster uses parquet-only indices (no lucene secondary).
        // Verify the lucene/_stats endpoint gracefully returns 200 with empty stats
        // rather than 500 / errors.
        createReplicaIndex();
        indexAndFlush(30);
        waitForReplicaSync();
        Map<String, Object> stats = DataFormatApiTestUtils.getLuceneStats(getRestClient(), INDEX_NAME, Collections.emptyMap());
        Map<String, Object> lucene = DataFormatApiTestUtils.mapAt(stats, "indices", INDEX_NAME, "lucene");
        // Lucene block should always be present (graceful empty) even when no lucene secondary.
        assertThat("Lucene stats block should be present (empty if no secondary)", lucene, notNullValue());
    }
}
