/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.Map;

/**
 * Integration test verifying that analytics-engine queries surface in the standard
 * {@code search.query_total} / {@code search.query_time_in_millis} counters under
 * {@code GET /_nodes/stats} via the {@link org.opensearch.plugins.SearchStatsContributor}
 * extension point. Existing tooling (e.g. Tumbler) polls these fields, so wiring the
 * analytics counters there means no tooling changes are required to observe analytics
 * traffic.
 */
public class SearchStatsContributorIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    /**
     * Multi-shard index so the test can distinguish the per-query contributor
     * (queryCount delta ≈ N) from the per-shard Lucene-equivalent contributor
     * (queryCount delta ≈ N × shards). Catches the regression class where the
     * contributor counts 1-per-query and silently under-reports search rate
     * by the shard fan-out factor to downstream consumers of node stats.
     */
    private static final int NUMBER_OF_SHARDS = 3;
    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET, NUMBER_OF_SHARDS);
            dataProvisioned = true;
        }
    }

    /**
     * Guards the 1-per-query bug: cluster-wide query_total must grow with the
     * shard fan-out factor. A 1-per-query contributor would produce delta ≈ N
     * (instead of N × shards), under-reporting search rate by the shard fan-out
     * factor to downstream consumers of node stats.
     */
    public void testQueryTotalScalesWithShardFanOut() throws IOException {
        ensureDataProvisioned();

        long before = getQueryTotalAcrossNodes();
        fireQueries();
        long after = getQueryTotalAcrossNodes();

        long delta = after - before;
        long minExpected = (long) TOTAL_QUERIES * NUMBER_OF_SHARDS / 2;
        assertTrue(
            "search.query_total must grow with shard fan-out (fired " + TOTAL_QUERIES
                + " on " + NUMBER_OF_SHARDS + "-shard index, expected ≥ " + minExpected
                + ", observed delta=" + delta + ")",
            delta >= minExpected
        );
    }

    /**
     * Guards the latency-aggregation bug: query_time_in_millis must move when
     * real PPL work runs. A delta of 0 means the contributor's time source
     * dropped every recording (e.g. an all-or-nothing window that collapsed).
     */
    public void testQueryTimeIncrementsAfterAnalyticsQueries() throws IOException {
        ensureDataProvisioned();

        long before = getQueryTimeAcrossNodes();
        fireQueries();
        long after = getQueryTimeAcrossNodes();

        long delta = after - before;
        assertTrue("search.query_time_in_millis must grow > 0 after PPL work, observed delta=" + delta, delta > 0);
    }

    /**
     * Minimum-viable check: every fired query must contribute at least once to
     * cluster-wide query_total. Catches regressions where the contributor stops
     * firing entirely (e.g. returns null, or filters out all stages).
     */
    public void testQueryTotalIncrementsAtLeastOncePerQuery() throws IOException {
        ensureDataProvisioned();

        long before = getQueryTotalAcrossNodes();
        fireQueries();
        long after = getQueryTotalAcrossNodes();

        long delta = after - before;
        assertTrue(
            "search.query_total must grow by at least one per query (fired " + TOTAL_QUERIES
                + ", observed delta=" + delta + ")",
            delta >= TOTAL_QUERIES
        );
    }

    private static final int TOTAL_QUERIES = 15;

    private void fireQueries() throws IOException {
        // Varied shapes so the contributor sees both real elapsed time and a non-trivial count.
        for (int i = 0; i < 5; i++) {
            executePpl("source=" + DATASET.indexName + " | fields str0");
            executePpl("source=" + DATASET.indexName + " | where num0 > 0 | fields str0, num0");
            executePpl("source=" + DATASET.indexName + " | stats avg(num0)");
        }
    }

    private long getQueryTotalAcrossNodes() throws IOException {
        return sumSearchField("query_total");
    }

    private long getQueryTimeAcrossNodes() throws IOException {
        return sumSearchField("query_time_in_millis");
    }

    @SuppressWarnings("unchecked")
    private long sumSearchField(String field) throws IOException {
        Request request = new Request("GET", "/_nodes/stats/indices/search");
        Response response = client().performRequest(request);
        Map<String, Object> body = entityAsMap(response);
        Map<String, Object> nodes = (Map<String, Object>) body.get("nodes");
        long total = 0;
        for (Object nodeValue : nodes.values()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodeValue;
            Map<String, Object> indices = (Map<String, Object>) nodeStats.get("indices");
            if (indices == null) continue;
            Map<String, Object> search = (Map<String, Object>) indices.get("search");
            if (search == null) continue;
            Number value = (Number) search.get(field);
            if (value != null) {
                total += value.longValue();
            }
        }
        return total;
    }

}
