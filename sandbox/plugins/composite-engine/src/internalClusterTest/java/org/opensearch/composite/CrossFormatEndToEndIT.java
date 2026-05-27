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
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Cross-format end-to-end integration tests for the composite engine.
 * Verifies that stats APIs accurately reflect indexing, flush, refresh,
 * and merge operations across parquet primary and lucene secondary formats.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class CrossFormatEndToEndIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        // Real HTTP transport is required because tests use getRestClient() to hit /_plugins/* REST endpoints.
        return false;
    }

    private static final String INDEX_PREFIX = "cross-format-";

    @SuppressWarnings("unchecked")
    public void testStatsReflectIndexing() throws Exception {
        String idx = INDEX_PREFIX + "reflect-indexing";
        createCompositeIndex(idx, true);
        indexDocs(idx, 10, 0);
        flushIndex(idx);

        Map<String, Object> composite = getCompositeStats(idx);
        Map<String, Object> indexing = (Map<String, Object>) composite.get("indexing");
        assertThat(((Number) indexing.get("docs_indexed_total")).longValue(), equalTo(10L));

        // Index 10 more and verify cumulative count
        indexDocs(idx, 10, 10);
        flushIndex(idx);

        composite = getCompositeStats(idx);
        indexing = (Map<String, Object>) composite.get("indexing");
        assertThat(((Number) indexing.get("docs_indexed_total")).longValue(), equalTo(20L));
    }

    @SuppressWarnings("unchecked")
    public void testStatsReflectFlush() throws Exception {
        String idx = INDEX_PREFIX + "reflect-flush";
        createCompositeIndex(idx, true);
        // Two indexing batches with a flush in between guarantee the second flush has work to do
        // (a subsequent flush with no new docs is a no-op at the engine layer and won't increment).
        indexDocs(idx, 5, 0);
        flushIndex(idx);
        indexDocs(idx, 5, 5);
        flushIndex(idx);

        Map<String, Object> composite = getCompositeStats(idx);
        Map<String, Object> flush = (Map<String, Object>) composite.get("flush");
        assertThat(((Number) flush.get("flush_total")).longValue(), greaterThanOrEqualTo(2L));
    }

    @SuppressWarnings("unchecked")
    public void testStatsReflectRefresh() throws Exception {
        String idx = INDEX_PREFIX + "reflect-refresh";
        createCompositeIndex(idx, true);
        // Each refresh needs new docs to actually trigger a refresh at the engine layer;
        // back-to-back refreshes without new docs are no-ops and won't increment the counter.
        indexDocs(idx, 2, 0);
        refreshIndex(idx);
        indexDocs(idx, 2, 2);
        refreshIndex(idx);
        indexDocs(idx, 2, 4);
        refreshIndex(idx);

        Map<String, Object> composite = getCompositeStats(idx);
        Map<String, Object> refresh = (Map<String, Object>) composite.get("refresh");
        assertThat(((Number) refresh.get("refresh_total")).longValue(), greaterThanOrEqualTo(3L));
    }

    @SuppressWarnings("unchecked")
    public void testStatsReflectMerge() throws Exception {
        String idx = INDEX_PREFIX + "reflect-merge";
        System.setProperty("opensearch.pluggable.dataformat.merge.enabled", "true");
        try {
            createCompositeIndex(idx, true);

            // Create multiple segments
            indexDocs(idx, 10, 0);
            flushIndex(idx);
            indexDocs(idx, 10, 10);
            flushIndex(idx);
            indexDocs(idx, 10, 20);
            flushIndex(idx);

            // Force a merge deterministically (background merges may not fire on small segments).
            client().admin().indices().prepareForceMerge(idx).setMaxNumSegments(1).get();

            // assertBusy because force-merge triggers async merge tasks; the stats counter
            // increments asynchronously as the merge completes.
            assertBusy(() -> {
                Map<String, Object> composite = getCompositeStats(idx);
                Map<String, Object> merge = (Map<String, Object>) composite.get("merge");
                assertThat(merge, notNullValue());
                assertThat(((Number) merge.get("merges_total")).longValue(), greaterThanOrEqualTo(1L));
            }, 30, TimeUnit.SECONDS);

            Map<String, Object> composite = getCompositeStats(idx);
            Map<String, Object> merge = (Map<String, Object>) composite.get("merge");
            assertThat(((Number) merge.get("merge_time_millis")).longValue(), greaterThan(0L));
        } finally {
            System.clearProperty("opensearch.pluggable.dataformat.merge.enabled");
        }
    }

    @SuppressWarnings("unchecked")
    public void testCrossFormatConsistencyAfterOperations() throws Exception {
        String idx = INDEX_PREFIX + "consistency";
        createCompositeIndex(idx, true);
        indexDocs(idx, 20, 0);
        flushIndex(idx);
        refreshIndex(idx);

        Map<String, Object> composite = getCompositeStats(idx);

        // Verify per_format has both parquet and lucene
        Map<String, Object> perFormat = (Map<String, Object>) composite.get("per_format");
        assertThat(perFormat, hasKey("parquet"));
        assertThat(perFormat, hasKey("lucene"));

        // Both formats should show consistent indexing counts
        Map<String, Object> parquetStats = (Map<String, Object>) perFormat.get("parquet");
        Map<String, Object> parquetIndexing = (Map<String, Object>) parquetStats.get("indexing");
        long parquetCount = ((Number) parquetIndexing.get("docs_indexed_total")).longValue();
        assertThat(parquetCount, greaterThan(0L));

        Map<String, Object> luceneStats = (Map<String, Object>) perFormat.get("lucene");
        Map<String, Object> luceneIndexing = (Map<String, Object>) luceneStats.get("indexing");
        long luceneCount = ((Number) luceneIndexing.get("docs_indexed_total")).longValue();
        assertThat(luceneCount, greaterThan(0L));

        // Composite total counts each LOGICAL doc once, not once per format. Each format also
        // sees every logical doc, so per-format counts should EACH equal the composite total.
        Map<String, Object> totalIndexing = (Map<String, Object>) composite.get("indexing");
        long total = ((Number) totalIndexing.get("docs_indexed_total")).longValue();
        assertThat("Composite total should equal parquet count", total, equalTo(parquetCount));
        assertThat("Composite total should equal lucene count", total, equalTo(luceneCount));
        assertThat("All 20 logical docs should be counted", total, equalTo(20L));
    }

    // --- Helper ---

    @SuppressWarnings("unchecked")
    private Map<String, Object> getCompositeStats(String indexName) throws Exception {
        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/dataformat_stats/" + indexName));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Map<String, Object> responseMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get("indices");
        assertThat(indices, hasKey(indexName));

        Map<String, Object> indexStats = (Map<String, Object>) indices.get(indexName);
        Map<String, Object> composite = (Map<String, Object>) indexStats.get("composite");
        assertThat(composite, notNullValue());
        return composite;
    }
}
