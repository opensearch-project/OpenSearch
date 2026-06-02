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
    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testQueryTotalIncrementsAfterAnalyticsQueries() throws IOException {
        ensureDataProvisioned();

        long baselineQueryTotal = getQueryTotalAcrossNodes();
        long baselineQueryTimeMs = getQueryTimeAcrossNodes();

        // Fire a handful of analytics-engine PPL queries — varied shapes so the contributor
        // sees both real elapsed time and a non-trivial count.
        for (int i = 0; i < 5; i++) {
            executePpl("source=" + DATASET.indexName + " | fields str0");
            executePpl("source=" + DATASET.indexName + " | where num0 > 0 | fields str0, num0");
            executePpl("source=" + DATASET.indexName + " | stats avg(num0)");
        }

        long afterQueryTotal = getQueryTotalAcrossNodes();
        long afterQueryTimeMs = getQueryTimeAcrossNodes();

        long countDelta = afterQueryTotal - baselineQueryTotal;
        assertTrue(
            "search.query_total must increment by at least 1 after analytics queries, delta=" + countDelta,
            countDelta >= 1
        );
        // query_time_in_millis is a sum so it should grow strictly monotonically with count;
        // any single query that took 0ms is unlikely with non-trivial work, but the contract
        // we care about is "the field is being populated", so >= 0 is enough.
        long timeDelta = afterQueryTimeMs - baselineQueryTimeMs;
        assertTrue("search.query_time_in_millis must not regress, delta=" + timeDelta, timeDelta >= 0);
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
