/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for non-aggregate sort pushdown on a multi-shard index. The result tests
 * assert the merged output is the correct, globally-ordered top-N (catches dropped rows /
 * wrong ordering); {@link #testExplain_sortPushedToShardFragment()} asserts via EXPLAIN that
 * the collated Sort is actually pushed below the exchange (into the shard fragment).
 */
public class SortPushdownIT extends AnalyticsRestTestCase {

    private static volatile boolean provisioned = false;
    private static final String INDEX = "parquet_hits";
    private static final String COL = "ResolutionWidth";

    private void ensureProvisioned() throws Exception {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), ClickBenchTestHelper.DATASET, 2);
            provisioned = true;
        }
    }

    /** sort desc + head 10 → global top-10, descending. Top row must equal the global max. */
    public void testSortDescHead_globalTopN() throws Exception {
        ensureProvisioned();
        List<List<Object>> rows = datarows(executePpl("source = " + INDEX + " | sort - " + COL + " | head 10 | fields " + COL));
        assertEquals(10, rows.size());
        assertMonotonic(rows, false);
        assertEquals("top row must equal global max", globalAgg("max", null), num(rows.get(0).get(0)), 0.0);
    }

    /** sort asc + head 10 → global bottom-10, ascending. Top row must equal the global min. */
    public void testSortAscHead_globalBottomN() throws Exception {
        ensureProvisioned();
        List<List<Object>> rows = datarows(executePpl("source = " + INDEX + " | sort " + COL + " | head 10 | fields " + COL));
        assertEquals(10, rows.size());
        assertMonotonic(rows, true);
        assertEquals("top row must equal global min", globalAgg("min", null), num(rows.get(0).get(0)), 0.0);
    }

    /** Multi-key sort: lexicographic ordering (primary asc, secondary asc on ties) survives the exchange. */
    public void testMultiKeySort_lexicographicOrder() throws Exception {
        ensureProvisioned();
        List<List<Object>> rows = datarows(
            executePpl("source = " + INDEX + " | sort " + COL + ", AdvEngineID | head 20 | fields " + COL + ", AdvEngineID")
        );
        assertTrue("expected 1..20 rows", rows.size() > 0 && rows.size() <= 20);
        for (int i = 1; i < rows.size(); i++) {
            double p0 = num(rows.get(i - 1).get(0)), p1 = num(rows.get(i).get(0));
            assertTrue("primary key ascending at " + i, p0 <= p1);
            if (p0 == p1) {
                assertTrue("secondary key ascending on tie at " + i, num(rows.get(i - 1).get(1)) <= num(rows.get(i).get(1)));
            }
        }
    }

    /** Sort + WHERE: pushdown coexists with filter delegation; result is the filtered global top-N. */
    public void testSortWithFilter_globalTopN() throws Exception {
        ensureProvisioned();
        String where = "where " + COL + " > 0";
        long filtered = (long) globalAgg("count", where);
        List<List<Object>> rows = datarows(executePpl("source = " + INDEX + " | " + where + " | sort - " + COL + " | head 10 | fields " + COL));
        assertEquals(Math.min(10, filtered), rows.size());
        assertMonotonic(rows, false);
        if (filtered > 0) {
            assertEquals("top must equal filtered max", globalAgg("max", where), num(rows.get(0).get(0)), 0.0);
            for (List<Object> r : rows) {
                assertTrue("every row satisfies the filter", num(r.get(0)) > 0);
            }
        }
    }

    /** Pure LIMIT (no ORDER BY): rewriter must skip; coordinator still bounds the result to N globally. */
    public void testPureLimit_noSort_count() throws Exception {
        ensureProvisioned();
        long total = (long) globalAgg("count", null);
        List<List<Object>> rows = datarows(executePpl("source = " + INDEX + " | head 10 | fields " + COL));
        assertEquals(Math.min(10, total), rows.size());
    }

    /** head larger than per-shard cardinality: still globally ordered, top == global max. */
    public void testLargeHead_globalOrder() throws Exception {
        ensureProvisioned();
        long total = (long) globalAgg("count", null);
        List<List<Object>> rows = datarows(executePpl("source = " + INDEX + " | sort - " + COL + " | head 100 | fields " + COL));
        assertEquals(Math.min(100, total), rows.size());
        assertMonotonic(rows, false);
        assertEquals(globalAgg("max", null), num(rows.get(0).get(0)), 0.0);
    }

    /** EXPLAIN confirms the collated Sort is pushed below the exchange (into the shard fragment). */
    @SuppressWarnings("unchecked")
    public void testExplain_sortPushedToShardFragment() throws Exception {
        ensureProvisioned();
        Map<String, Object> profile = (Map<String, Object>) executeExplain("source = " + INDEX + " | sort - " + COL + " | head 10")
            .get("profile");
        String plan = String.join("\n", (List<String>) profile.get("full_plan"));
        long sorts = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertTrue("expected coordinator + pushed shard Sort, plan:\n" + plan, sorts >= 2);
        int er = plan.indexOf("ExchangeReducer");
        assertTrue("a Sort must sit below the exchange (pushed down), plan:\n" + plan, er >= 0 && plan.indexOf("OpenSearchSort", er) > er);
    }

    /** UNION ALL ({@code | append}): result is the global top-N, and the Sort is pushed below each arm's exchange. */
    @SuppressWarnings("unchecked")
    public void testUnionAll_sortPushedIntoEachArm() throws Exception {
        ensureProvisioned();
        String union = "source = " + INDEX + " | append [ source = " + INDEX + " ] | sort - " + COL + " | head 10";

        List<List<Object>> rows = datarows(executePpl(union + " | fields " + COL));
        assertEquals(10, rows.size());
        assertMonotonic(rows, false);
        assertEquals("top row must equal global max (union of index with itself)", globalAgg("max", null), num(rows.get(0).get(0)), 0.0);

        String plan = String.join("\n", (List<String>) ((Map<String, Object>) executeExplain(union).get("profile")).get("full_plan"));
        String[] lines = plan.split("\n", -1);
        assertTrue("two arm exchanges, plan:\n" + plan, plan.lines().filter(l -> l.contains("ExchangeReducer")).count() >= 2);
        for (int i = 0; i + 1 < lines.length; i++) {
            if (lines[i].contains("ExchangeReducer")) {
                assertTrue("a Sort must be pushed below each arm exchange, plan:\n" + plan, lines[i + 1].contains("OpenSearchSort"));
            }
        }
    }

    /** {@code fn} is count/min/max; {@code where} is an optional leading filter clause (may be null). */
    private double globalAgg(String fn, String where) throws Exception {
        String filter = where == null ? "" : where + " | ";
        String agg = "count".equals(fn) ? "count() as v" : fn + "(" + COL + ") as v";
        List<List<Object>> rows = datarows(executePpl("source = " + INDEX + " | " + filter + "stats " + agg));
        return num(rows.get(0).get(0));
    }

    private static void assertMonotonic(List<List<Object>> rows, boolean ascending) {
        for (int i = 1; i < rows.size(); i++) {
            double prev = num(rows.get(i - 1).get(0));
            double cur = num(rows.get(i).get(0));
            assertTrue("rows not " + (ascending ? "ascending" : "descending") + " at " + i, ascending ? prev <= cur : prev >= cur);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> datarows(Map<String, Object> result) {
        return (List<List<Object>>) result.get("datarows");
    }

    private static double num(Object cell) {
        return ((Number) cell).doubleValue();
    }

    private Map<String, Object> executeExplain(String ppl) throws Exception {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        return assertOkAndParse(client().performRequest(request), "EXPLAIN: " + ppl);
    }
}
