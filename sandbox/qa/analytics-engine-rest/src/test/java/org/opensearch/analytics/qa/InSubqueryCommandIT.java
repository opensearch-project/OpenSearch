/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code in} subqueries on the analytics-engine
 * route. Mirrors {@code CalcitePPLInSubqueryIT} from the {@code opensearch-project/sql}
 * repository — every {@code @Test} method here corresponds to a method of the same name
 * in the source IT, adapted to this project's {@code AnalyticsRestTestCase} harness.
 *
 * <p>Each test sends a PPL query through {@code POST /_analytics/ppl}. The {@code worker}
 * / {@code work_information} / {@code occupation} datasets are 1:1 ports of the upstream
 * fixtures (with {@code text} fields swapped to {@code keyword} so docvalues / parquet
 * read paths are exercised the same way as the rest of the analytics-engine ITs).
 *
 * <p>Subquery decorrelation in this backend is performed by Calcite's
 * {@code RelDecorrelator} run from {@code CorrelationLowering} just before isthmus
 * encoding (see {@code DataFusionFragmentConvertor.lowerCorrelation}). Equi correlated
 * IN / scalar / EXISTS subqueries flatten into expression-level {@code RexSubQuery}
 * which {@code datafusion-substrait} consumes natively.
 *
 * <p>Tests that depend on the {@code plugins.calcite.subsearch.max_out} cluster setting
 * are intentionally not ported — that knob is not wired through the analytics-engine
 * route. The decorrelation behaviour itself is what these ITs exercise.
 */
public class InSubqueryCommandIT extends AnalyticsRestTestCase {

    private static final Dataset WORKER = new Dataset("worker", "worker");
    private static final Dataset WORK_INFORMATION = new Dataset("work_information", "work_information");
    private static final Dataset OCCUPATION = new Dataset("occupation", "occupation");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), WORKER);
            DatasetProvisioner.provision(client(), WORK_INFORMATION);
            DatasetProvisioner.provision(client(), OCCUPATION);
            dataProvisioned = true;
        }
    }

    public void testSelfInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id in [source=worker | where country = 'USA' | fields id]"
                + " | fields name, country, occupation, id, salary"
        );
        assertRowsAnyOrder(
            response,
            row("Hello", "USA", "Artist", 1001, 70000),
            row("Tommy", "USA", "Teacher", 1006, 30000)
        );
    }

    public void testWhereInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id in [source = work_information | fields uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(
            response,
            row(1002, "John", 120000),
            row(1003, "David", 120000),
            row(1000, "Jake", 100000),
            row(1005, "Jane", 90000),
            row(1006, "Tommy", 30000)
        );
    }

    public void testFilterInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id in [source = work_information | fields uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(
            response,
            row(1002, "John", 120000),
            row(1003, "David", 120000),
            row(1000, "Jake", 100000),
            row(1005, "Jane", 90000),
            row(1006, "Tommy", 30000)
        );
    }

    public void testInSubqueryWithParentheses() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where (id) in [source = work_information | fields uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(
            response,
            row(1002, "John", 120000),
            row(1003, "David", 120000),
            row(1000, "Jake", 100000),
            row(1005, "Jane", 90000),
            row(1006, "Tommy", 30000)
        );
    }

    public void testTwoExpressionsInSubquery() throws IOException {
        // Sort by id as a secondary key so the two 120000-salary rows have a stable order
        // — DataFusion's hash-join output ordering for equal salary values is not stable
        // and the upstream IT relies on engine-specific tie-breaking we don't replicate.
        Map<String, Object> response = executePpl(
            "source = worker | where (id, name) in [source = work_information | fields uid, name]"
                + " | sort - salary, id"
                + " | fields id, name, salary"
        );
        assertRowsEqual(
            response,
            row(1002, "John", 120000),
            row(1003, "David", 120000),
            row(1000, "Jake", 100000),
            row(1005, "Jane", 90000)
        );
    }

    public void testWhereNotInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id not in [source = work_information | fields uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(response, row(1001, "Hello", 70000), row(1004, "David", 0));
    }

    public void testFilterNotInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id not in [source = work_information | fields uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(response, row(1001, "Hello", 70000), row(1004, "David", 0));
    }

    @AwaitsFix(bugUrl = "OpenSearchAggregateSplitRule rejects nullable filter expressions ('filter must be BOOLEAN NOT NULL') on the multi-column NOT IN's COUNT FILTER aggregate. Independent of subquery decorrelation.")
    public void testTwoExpressionsNotInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where (id, name) not in [source = work_information | fields uid, name]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(response, row(1001, "Hello", 70000), row(1006, "Tommy", 30000), row(1004, "David", 0));
    }

    public void testEmptyInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id not in [source = work_information | where uid = 0000 | fields uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(
            response,
            row(1002, "John", 120000),
            row(1003, "David", 120000),
            row(1000, "Jake", 100000),
            row(1005, "Jane", 90000),
            row(1001, "Hello", 70000),
            row(1006, "Tommy", 30000),
            row(1004, "David", 0)
        );
    }

    public void testNestedInSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id in ["
                + "    source = work_information"
                + "    | where occupation in ["
                + "        source = occupation"
                + "        | where occupation != 'Engineer'"
                + "        | fields occupation"
                + "      ]"
                + "    | fields uid"
                + "  ]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(
            response,
            row(1002, "John", 120000),
            row(1003, "David", 120000),
            row(1006, "Tommy", 30000)
        );
    }

    public void testNestedInSubquery2() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id in ["
                + "    source = work_information"
                + "    | where occupation in ["
                + "        source = occupation"
                + "        | where occupation != 'Engineer'"
                + "        | fields occupation"
                + "      ]"
                + "    | fields uid"
                + "  ]"
                + " | sort - salary | fields name, country, occupation, id, salary"
        );
        assertRowsEqual(
            response,
            row("John", "Canada", "Doctor", 1002, 120000),
            row("David", null, "Doctor", 1003, 120000),
            row("Tommy", "USA", "Teacher", 1006, 30000)
        );
    }

    public void testInSubqueryWithTableAlias() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker as o | where id in ["
                + "    source = work_information as i"
                + "    | where i.department = 'DATA'"
                + "    | fields uid"
                + "  ]"
                + " | sort - o.salary"
                + " | fields o.id, o.name, o.salary"
        );
        assertRowsEqual(response, row(1002, "John", 120000), row(1005, "Jane", 90000));
    }

    public void testInCorrelatedSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where name in ["
                + "    source = work_information | where id = uid and"
                + "    (like(occupation, '%ist') or occupation = 'Engineer') | fields name"
                + "  ]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(
            response,
            row(1002, "John", 120000),
            row(1000, "Jake", 100000),
            row(1005, "Jane", 90000)
        );
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    @SuppressWarnings("unused")
    private void assertErrorAny(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = entityAsMap(response);
            fail("Expected query [" + ppl + "] to fail but got: " + body);
        } catch (ResponseException expected) {
            // Any error response is acceptable.
        }
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings({ "unchecked", "varargs" })
    private final void assertRowsEqual(Map<String, Object> response, List<Object>... expected) {
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", actualRows);
        assertEquals("Row count mismatch", expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertEquals("Cell mismatch at row " + i + ", col " + j, want.get(j), got.get(j));
            }
        }
    }

    @SafeVarargs
    @SuppressWarnings({ "unchecked", "varargs" })
    private final void assertRowsAnyOrder(Map<String, Object> response, List<Object>... expected) {
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", actualRows);
        assertEquals("Row count mismatch", expected.length, actualRows.size());
        for (List<Object> want : expected) {
            assertTrue(
                "Expected row " + want + " not found in " + actualRows,
                actualRows.stream().anyMatch(got -> rowsEqual(want, got))
            );
        }
    }

    private static boolean rowsEqual(List<Object> want, List<Object> got) {
        if (want.size() != got.size()) return false;
        for (int j = 0; j < want.size(); j++) {
            Object w = want.get(j);
            Object g = got.get(j);
            if (w == null ? g != null : !w.equals(g)) {
                if (w instanceof Number && g instanceof Number) {
                    if (((Number) w).doubleValue() != ((Number) g).doubleValue()) return false;
                } else {
                    return false;
                }
            }
        }
        return true;
    }
}
