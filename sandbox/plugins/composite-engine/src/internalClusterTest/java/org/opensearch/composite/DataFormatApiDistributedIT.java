/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Distributed integration tests for the 3 composite-engine REST APIs on a
 * 3-node, 2-shard, 1-replica cluster, including concurrency and shard relocation scenarios.
 *
 * @opensearch.experimental
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class DataFormatApiDistributedIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        // Real HTTP transport is required because tests use getRestClient() to hit /_plugins/* REST endpoints.
        return false;
    }

    private static final int NUM_SHARDS = 2;
    private static final int NUM_REPLICAS = 1;

    private void createDistributedIndex(String idx) {
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", NUM_SHARDS)
            .put("index.number_of_replicas", NUM_REPLICAS)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene");
        client().admin()
            .indices()
            .prepareCreate(idx)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(idx);
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "DataFormatAwareEngine peer recovery Phase[2]: 'updates/deletes not supported' during translog ops replay. "
        + "Phase[1] translog UUID was fixed in this branch (LuceneCommitter.discoverAndTrimUnsafeCommits read top-level commit data). "
        + "Phase[2] requires engine-level support for op replay during recovery; tracked separately.")
    @SuppressWarnings("unchecked")
    public void testApisFullDistributed() throws Exception {
        String idx = "dist-full-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        createDistributedIndex(idx);
        indexDocs(idx, 100, 0);
        flushIndex(idx);
        refreshIndex(idx);

        assertBusy(() -> {
            Map<String, Object> stats = getStats(idx);
            Map<String, Object> catalog = getCatalog(idx);
            Map<String, Object> analyze = getAnalyze(idx);

            long docsIndexed = statsDocsIndexed(stats, idx);
            assertThat(docsIndexed, equalTo(100L));

            Map<String, Object> summary = mapAt(catalog, "summary", "by_format", "parquet");
            assertThat(summary, notNullValue());
            long catalogRows = ((Number) summary.get("total_rows")).longValue();
            assertThat(catalogRows, equalTo(100L));

            long analyzeRows = ((Number) analyze.get("total_rows")).longValue();
            assertThat(analyzeRows, equalTo(100L));
        }, 60, TimeUnit.SECONDS);
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "DataFormatAwareEngine peer recovery Phase[2]: 'updates/deletes not supported' during translog ops replay. "
        + "Phase[1] translog UUID was fixed in this branch (LuceneCommitter.discoverAndTrimUnsafeCommits read top-level commit data). "
        + "Phase[2] requires engine-level support for op replay during recovery; tracked separately.")
    @SuppressWarnings("unchecked")
    public void testStatsConcurrentIndexing() throws Exception {
        String idx = "dist-concurrent-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        createDistributedIndex(idx);

        try (BackgroundIndexer indexer = newBackgroundIndexer(idx)) {
            indexer.start(-1);
            waitForIndexerDocs(50, indexer);

            long prevDocs = 0;
            for (int i = 0; i < 5; i++) {
                Map<String, Object> stats = getStats(idx);
                long docsIndexed = statsDocsIndexed(stats, idx);
                assertThat("docs_indexed_total must be monotonically non-decreasing", docsIndexed, greaterThanOrEqualTo(prevDocs));
                prevDocs = docsIndexed;
            }

            indexer.stopAndAwaitStopped();
            long totalDocs = indexer.totalIndexedDocs();
            flushIndex(idx);
            refreshIndex(idx);

            assertBusy(() -> {
                Map<String, Object> stats = getStats(idx);
                long finalDocs = statsDocsIndexed(stats, idx);
                assertThat(finalDocs, equalTo(totalDocs));
            }, 60, TimeUnit.SECONDS);
        }
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "DataFormatAwareEngine peer recovery Phase[2]: 'updates/deletes not supported' during translog ops replay. "
        + "Phase[1] translog UUID was fixed in this branch (LuceneCommitter.discoverAndTrimUnsafeCommits read top-level commit data). "
        + "Phase[2] requires engine-level support for op replay during recovery; tracked separately.")
    @SuppressWarnings("unchecked")
    public void testCrossApiDuringWrites() throws Exception {
        String idx = "dist-crossapi-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        createDistributedIndex(idx);

        try (BackgroundIndexer indexer = newBackgroundIndexer(idx)) {
            indexer.start(-1);
            waitForIndexerDocs(30, indexer);

            // Query all 3 APIs while indexing — no exceptions expected
            for (int i = 0; i < 4; i++) {
                getStats(idx);
                getCatalog(idx);
                getAnalyze(idx);
            }

            indexer.stopAndAwaitStopped();
            long totalDocs = indexer.totalIndexedDocs();
            flushIndex(idx);
            refreshIndex(idx);

            // After stopping, all 3 APIs should eventually agree
            assertBusy(() -> {
                Map<String, Object> stats = getStats(idx);
                Map<String, Object> catalog = getCatalog(idx);
                Map<String, Object> analyze = getAnalyze(idx);

                long docsIndexed = statsDocsIndexed(stats, idx);
                assertThat(docsIndexed, equalTo(totalDocs));

                Map<String, Object> summary = mapAt(catalog, "summary", "by_format", "parquet");
                assertThat(summary, notNullValue());
                long catalogRows = ((Number) summary.get("total_rows")).longValue();
                assertThat(catalogRows, equalTo(totalDocs));

                long analyzeRows = ((Number) analyze.get("total_rows")).longValue();
                assertThat(analyzeRows, equalTo(totalDocs));
            }, 60, TimeUnit.SECONDS);
        }
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "DataFormatAwareEngine peer recovery Phase[2]: 'updates/deletes not supported' during translog ops replay. "
        + "Phase[1] translog UUID was fixed in this branch (LuceneCommitter.discoverAndTrimUnsafeCommits read top-level commit data). "
        + "Phase[2] requires engine-level support for op replay during recovery; tracked separately.")
    @SuppressWarnings("unchecked")
    public void testStatsAfterShardRelocation() throws Exception {
        String idx = "dist-relocation-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        createDistributedIndex(idx);
        indexDocs(idx, 60, 0);
        flushIndex(idx);
        refreshIndex(idx);

        // Pick a data node to exclude
        String excludedNode = internalCluster().getDataNodeNames().iterator().next();

        // Trigger shard relocation by excluding the node
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", excludedNode))
            .get();

        // Wait for relocation to complete
        assertBusy(() -> ensureGreen(idx), 60, TimeUnit.SECONDS);

        // Verify stats still report correct count post-relocation
        assertBusy(() -> {
            Map<String, Object> stats = getStats(idx);
            long docsIndexed = statsDocsIndexed(stats, idx);
            assertThat(docsIndexed, equalTo(60L));
        }, 60, TimeUnit.SECONDS);

        // Clean up exclusion
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._name"))
            .get();
    }

    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "DataFormatAwareEngine peer recovery Phase[2]: 'updates/deletes not supported' during translog ops replay. "
        + "Phase[1] translog UUID was fixed in this branch (LuceneCommitter.discoverAndTrimUnsafeCommits read top-level commit data). "
        + "Phase[2] requires engine-level support for op replay during recovery; tracked separately.")
    @SuppressWarnings("unchecked")
    public void testCrossApiPostMergeDistributed() throws Exception {
        String idx = "dist-merge-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        createDistributedIndex(idx);

        // Index in 3 batches with flushes to create multiple segments per shard
        indexDocs(idx, 20, 0);
        flushIndex(idx);
        indexDocs(idx, 20, 20);
        flushIndex(idx);
        indexDocs(idx, 20, 40);
        flushIndex(idx);

        // Record initial segment count from catalog
        Map<String, Object> preMergeCatalog = getCatalog(idx);
        List<Object> preMergeSegments = listAt(preMergeCatalog, "segments");
        assertThat(preMergeSegments, notNullValue());
        int initialSegmentCount = preMergeSegments.size();

        // Force merge to 1 segment per shard
        client().admin().indices().prepareForceMerge(idx).setMaxNumSegments(1).get();

        // Wait for replica sync and verify cross-API consistency
        assertBusy(() -> {
            Map<String, Object> stats = getStats(idx);
            Map<String, Object> catalog = getCatalog(idx);
            Map<String, Object> analyze = getAnalyze(idx);

            long docsIndexed = statsDocsIndexed(stats, idx);
            assertThat(docsIndexed, equalTo(60L));

            Map<String, Object> summary = mapAt(catalog, "summary", "by_format", "parquet");
            assertThat(summary, notNullValue());
            long catalogRows = ((Number) summary.get("total_rows")).longValue();
            assertThat(catalogRows, equalTo(60L));

            long analyzeRows = ((Number) analyze.get("total_rows")).longValue();
            assertThat(analyzeRows, equalTo(60L));

            // Post-merge segments should be <= initial (force merge reduces segments)
            List<Object> postMergeSegments = listAt(catalog, "segments");
            assertThat(postMergeSegments, notNullValue());
            assertThat(postMergeSegments.size(), lessThanOrEqualTo(initialSegmentCount));
        }, 60, TimeUnit.SECONDS);
    }

    // --- Helper methods ---

    private BackgroundIndexer newBackgroundIndexer(String idx) {
        return new BackgroundIndexer(idx, "_doc", client(), -1, RandomizedTest.scaledRandomIntBetween(2, 5), false, random());
    }

    private void waitForIndexerDocs(long numDocs, BackgroundIndexer indexer) throws Exception {
        assertBusy(
            () -> assertTrue(
                "expected at least " + numDocs + " acked docs, got " + indexer.totalIndexedDocs(),
                indexer.totalIndexedDocs() >= numDocs
            ),
            60,
            TimeUnit.SECONDS
        );
    }

    private Map<String, Object> getStats(String idx) throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + idx));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
    }

    private Map<String, Object> getCatalog(String idx) throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/composite/" + idx + "/_catalog_snapshot"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
    }

    private Map<String, Object> getAnalyze(String idx) throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_analyze"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
    }

    @SuppressWarnings("unchecked")
    private static long statsDocsIndexed(Map<String, Object> stats, String idx) {
        Map<String, Object> indices = (Map<String, Object>) stats.get("indices");
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(idx);
        Map<String, Object> composite = (Map<String, Object>) indexStats.get("composite");
        Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
        return ((Number) indexing.get("docs_indexed_total")).longValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapAt(Map<String, Object> root, String... path) {
        Map<String, Object> current = root;
        for (String key : path) {
            if (current == null) return null;
            current = (Map<String, Object>) current.get(key);
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> listAt(Map<String, Object> root, String... path) {
        if (path.length == 0) return null;
        Map<String, Object> parent = path.length == 1 ? root : mapAt(root, java.util.Arrays.copyOf(path, path.length - 1));
        if (parent == null) return null;
        Object val = parent.get(path[path.length - 1]);
        return val instanceof List ? (List<Object>) val : null;
    }
}
