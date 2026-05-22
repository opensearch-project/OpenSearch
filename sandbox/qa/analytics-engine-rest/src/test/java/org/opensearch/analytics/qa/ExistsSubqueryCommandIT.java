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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code exists} / {@code not exists} subqueries
 * on the analytics-engine route. Mirrors {@code CalcitePPLExistsSubqueryIT} from the
 * {@code opensearch-project/sql} repository — every {@code @Test} method here corresponds
 * to a method of the same name in the source IT, adapted to this project's
 * {@code AnalyticsRestTestCase} harness.
 *
 * <p>Decorrelation is performed by Calcite's {@code RelDecorrelator} run from
 * {@code CorrelationLowering} just before isthmus encoding (see
 * {@code DataFusionFragmentConvertor.lowerCorrelation}). Equi correlated EXISTS / NOT
 * EXISTS subqueries flatten into expression-level {@code RexSubQuery} which
 * {@code datafusion-substrait} consumes natively, then DataFusion's optimizer rewrites
 * them as semi / anti joins.
 */
public class ExistsSubqueryCommandIT extends AnalyticsRestTestCase {

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

    public void testSimpleExistsSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists [source = work_information | where id = uid]"
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

    public void testExistsSubqueryAndAggregation() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists [source = work_information | where id = uid]"
                + " | stats count() by country"
        );
        assertRowsAnyOrder(response, row(1L, null), row(2L, "Canada"), row(1L, "USA"), row(1L, "England"));
    }

    public void testSimpleExistsSubqueryInFilter() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists [source = work_information | where id = uid]"
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

    public void testNotExistsSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where not exists [source = work_information | where id = uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(response, row(1001, "Hello", 70000), row(1004, "David", 0));
    }

    public void testNotExistsSubqueryInFilter() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where not exists [source = work_information | where id = uid]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(response, row(1001, "Hello", 70000), row(1004, "David", 0));
    }

    public void testEmptyExistsSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists ["
                + "    source = work_information | where uid = 0000 AND id = uid"
                + "  ]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowCount(response, 0);
    }

    public void testUncorrelatedExistsSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists [source = work_information | where name = 'Tom']"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowCount(response, 7);

        response = executePpl(
            "source = worker | where not exists [source = work_information | where name = 'Tom']"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowCount(response, 0);
    }

    public void testUncorrelatedExistsSubqueryCheckTheReturnContentOfInnerTableIsEmptyOrNot() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists [source = work_information]"
                + " | eval constant = 'Bala'"
                + " | fields constant"
        );
        assertRowsAnyOrder(
            response,
            row("Bala"),
            row("Bala"),
            row("Bala"),
            row("Bala"),
            row("Bala"),
            row("Bala"),
            row("Bala")
        );

        response = executePpl(
            "source = worker | where exists [source = work_information | where uid = 999]"
                + " | eval constant = 'Bala'"
                + " | fields constant"
        );
        assertRowCount(response, 0);
    }

    public void testNestedExistsSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists ["
                + "    source = work_information"
                + "    | where exists ["
                + "        source = occupation"
                + "        | where occupation.occupation = work_information.occupation"
                + "      ]"
                + "    | where id = uid"
                + "  ]"
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

    public void testExistsSubqueryWithConjunction() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where exists ["
                + "    source = work_information"
                + "    | where id = uid AND"
                + "      worker.name = work_information.name AND"
                + "      worker.occupation = work_information.occupation"
                + "  ]"
                + " | sort - salary"
                + " | fields id, name, salary"
        );
        assertRowsEqual(response, row(1003, "David", 120000), row(1000, "Jake", 100000));
    }

    public void testIssue3566() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | fields id, country | where exists ["
                + "    source = work_information | where id = uid"
                + "  ]"
                + " | stats count() by country"
        );
        assertRowsAnyOrder(response, row(1L, null), row(1L, "England"), row(1L, "USA"), row(2L, "Canada"));
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

    @SuppressWarnings("unchecked")
    private static void assertRowCount(Map<String, Object> response, int expected) {
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Row count mismatch", expected, rows.size());
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
