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
import java.util.List;
import java.util.Map;

/**
 * Reproduction for the reported node-killing crash on partially-consumed stream drop
 * ({@code upcallLinker.cpp:137} fatal, {@code SecurityException: SharedUtils is not
 * allowed to call System::exit(1)}).
 *
 * <p><b>Reported mechanism (from the reporter's hs_err):</b>
 * <ol>
 *   <li>An unsorted filter + LIMIT query satisfies the limit and DataFusion's
 *       {@code LimitStream} stops reading; the {@code IndexedStream} is dropped
 *       partially consumed.</li>
 *   <li>The drop releases the last {@code Arc} to the per-query
 *       {@code HashMap<annotationId, OnceLock<ProviderHandle>>};
 *       {@code ProviderHandle::drop} upcalls {@code releaseProvider} into Java.</li>
 *   <li>The Java side throws (binding already unregistered / handle already closed).</li>
 *   <li>An exception cannot cross an FFM upcall; the JDK falls back to
 *       {@code System.exit(1)}, the security policy forbids it, and the JVM dies
 *       fatally.</li>
 * </ol>
 *
 * <p><b>Why this lives in the REST QA module:</b> the crash needs (a) a real
 * distribution with the agent security policy (so {@code System.exit} is denied),
 * (b) the Lucene secondary so the filter leaf is delegation-possible and the
 * provider map is actually populated (parquet-only fixtures never allocate a
 * {@code ProviderHandle}), and (c) enough matching rows that batches are still
 * in flight when LIMIT stops reading.
 *
 * <p>The alternation below is the reporter's exact PPL sequence; they observed the
 * node dying at round 5, or up to ~30s after a single query (deferred drop).
 * If the bug fires, the cluster stops answering — asserted by the post-round probes.
 *
 * @opensearch.internal
 */
public class PartialStreamDropCrashIT extends AnalyticsRestTestCase {

    private static final String INDEX = "bench";
    private static final int TOTAL_DOCS = 500_000;
    private static final int BULK_BATCH = 10_000;
    private static final int ROUNDS = 5;

    public void testAlternatingPplRoundsDoNotKillTheNode() throws Exception {
        createBenchIndex();
        seedDocs();

        for (int round = 1; round <= ROUNDS; round++) {
            assertRowCount("round " + round + " q1", executePpl("source=" + INDEX + " | head 10"), 10);
            assertRowCount("round " + round + " q2", executePpl("source=" + INDEX + " | where brand='brand-3' | head 10"), 10);
            assertRowCount("round " + round + " q3", executePpl("source=" + INDEX + " | where brand='brand-3' | stats count()"), 1);
            assertRowCount("round " + round + " q4", executePpl("source=" + INDEX + " | stats avg(price) by brand"), 5);
            logger.info("--> round {} ok", round);
        }

        // The reported crash can be deferred up to ~30s after the triggering query
        // (native drop on a runtime thread after Java tore down the query binding).
        // Probe through that window; if the upcall fatal fires the node is gone and
        // the probe request fails.
        for (int i = 1; i <= 7; i++) {
            Thread.sleep(5_000);
            Response health = client().performRequest(new Request("GET", "/_cluster/health"));
            assertEquals("node must survive deferred cleanup (t+" + i * 5 + "s)", 200, health.getStatusLine().getStatusCode());
            assertRowCount("post-round probe t+" + i * 5 + "s", executePpl("source=" + INDEX + " | head 10"), 10);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void assertRowCount(String context, Map<String, Object> pplResponse, int expected) {
        List<Object> rows = (List<Object>) pplResponse.get("datarows");
        assertNotNull(context + ": datarows missing in " + pplResponse.keySet(), rows);
        assertEquals(context + ": row count", expected, rows.size());
    }

    private void createBenchIndex() throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {
            // index may not exist yet
        }
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": 1,"
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": \"lucene\""
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                // Indexed keyword: makes `where brand=...` a delegation-possible leaf so
                // the per-query ProviderHandle map is populated — required to reach the
                // crashing drop path.
                + "    \"brand\": { \"type\": \"keyword\" },"
                + "    \"price\": { \"type\": \"integer\" }"
                + "  }"
                + "}"
                + "}"
        );
        Response response = client().performRequest(create);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    /** 500k docs (reporter's scale), brand-0..brand-4 round-robin → 100k rows per brand. */
    private void seedDocs() throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < TOTAL_DOCS; i++) {
            bulk.append("{\"index\":{}}\n")
                .append("{\"brand\":\"brand-")
                .append(i % 5)
                .append("\",\"price\":")
                .append(i % 1000)
                .append("}\n");
            if ((i + 1) % BULK_BATCH == 0) {
                sendBulk(bulk.toString());
                bulk.setLength(0);
                if ((i + 1) % 100_000 == 0) {
                    client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
                    client().performRequest(new Request("POST", "/" + INDEX + "/_flush"));
                    logger.info("--> seeded {} docs", i + 1);
                }
            }
        }
        if (bulk.length() > 0) {
            sendBulk(bulk.toString());
        }
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush"));
    }

    private void sendBulk(String body) throws IOException {
        Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
        bulk.setJsonEntity(body);
        Response response = client().performRequest(bulk);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
