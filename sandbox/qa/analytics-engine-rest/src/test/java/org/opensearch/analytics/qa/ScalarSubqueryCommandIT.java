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
 * Self-contained integration test for PPL scalar subqueries on the analytics-engine
 * route. Mirrors {@code CalcitePPLScalarSubqueryIT} from the {@code opensearch-project/sql}
 * repository — every {@code @Test} method here corresponds to a method of the same name
 * in the source IT, adapted to this project's {@code AnalyticsRestTestCase} harness.
 *
 * <p>Decorrelation is performed by Calcite's {@code RelDecorrelator} run from
 * {@code CorrelationLowering} just before isthmus encoding (see
 * {@code DataFusionFragmentConvertor.lowerCorrelation}). Equi correlated scalar
 * subqueries flatten into expression-level {@code RexSubQuery} which
 * {@code datafusion-substrait} consumes natively. Non-equi correlated forms are
 * lowered by the same pass into standard joins + aggregates.
 */
public class ScalarSubqueryCommandIT extends AnalyticsRestTestCase {

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

    public void testUncorrelatedScalarSubqueryInSelect() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | eval count_dept = [source = work_information | stats count(department)]"
                + " | fields name, count_dept"
        );
        assertRowsAnyOrder(
            response,
            row("Jake", 5L),
            row("Hello", 5L),
            row("John", 5L),
            row("David", 5L),
            row("David", 5L),
            row("Jane", 5L),
            row("Tommy", 5L)
        );
    }

    public void testUncorrelatedScalarSubqueryInExpressionInSelect() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | eval count_dept = [source = work_information | stats count(department)] + 10"
                + " | fields name, count_dept"
        );
        assertRowsAnyOrder(
            response,
            row("Jake", 15L),
            row("Hello", 15L),
            row("John", 15L),
            row("David", 15L),
            row("David", 15L),
            row("Jane", 15L),
            row("Tommy", 15L)
        );
    }

    public void testUncorrelatedScalarSubqueryInSelectAndWhere() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id > [source = work_information | stats count(department)] + 999"
                + " | eval count_dept = [source = work_information | stats count(department)]"
                + " | fields name, count_dept"
        );
        assertRowsAnyOrder(response, row("Jane", 5L), row("Tommy", 5L));
    }

    public void testUncorrelatedScalarSubqueryInSelectAndInFilter() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id > [ source = work_information | stats count(department) ] + 999"
                + " | eval count_dept = [source = work_information | stats count(department)]"
                + " | fields name, count_dept"
        );
        assertRowsAnyOrder(response, row("Jane", 5L), row("Tommy", 5L));
    }

    public void testCorrelatedScalarSubqueryInSelect() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | eval count_dept = ["
                + "    source = work_information | where id = uid | stats count(department)"
                + "  ]"
                + " | fields id, name, count_dept"
        );
        assertRowsAnyOrder(
            response,
            row(1000, "Jake", 1L),
            row(1001, "Hello", 0L),
            row(1002, "John", 1L),
            row(1003, "David", 1L),
            row(1004, "David", 0L),
            row(1005, "Jane", 1L),
            row(1006, "Tommy", 1L)
        );
    }

    public void testCorrelatedScalarSubqueryInSelectWithNonEqual() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | eval count_dept = ["
                + "    source = work_information | where id > uid | stats count(department)"
                + "  ]"
                + " | fields id, name, count_dept"
        );
        assertRowsAnyOrder(
            response,
            row(1000, "Jake", 0L),
            row(1001, "Hello", 1L),
            row(1002, "John", 1L),
            row(1003, "David", 2L),
            row(1004, "David", 3L),
            row(1005, "Jane", 3L),
            row(1006, "Tommy", 4L)
        );
    }

    public void testCorrelatedScalarSubqueryInWhere() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id = ["
                + "    source = work_information | where id = uid | stats max(uid)"
                + "  ]"
                + " | fields id, name"
        );
        assertRowsAnyOrder(
            response,
            row(1000, "Jake"),
            row(1002, "John"),
            row(1003, "David"),
            row(1005, "Jane"),
            row(1006, "Tommy")
        );
    }

    public void testCorrelatedScalarSubqueryInFilter() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id = [ source = work_information | where id = uid | stats max(uid) ]"
                + " | fields id, name"
        );
        assertRowsAnyOrder(
            response,
            row(1000, "Jake"),
            row(1002, "John"),
            row(1003, "David"),
            row(1005, "Jane"),
            row(1006, "Tommy")
        );
    }

    public void testDisjunctiveCorrelatedScalarSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where ["
                + "    source = work_information | where id = uid OR uid = 1010 | stats count()"
                + "  ] > 0"
                + " | fields id, name"
        );
        assertRowsAnyOrder(
            response,
            row(1000, "Jake"),
            row(1002, "John"),
            row(1003, "David"),
            row(1005, "Jane"),
            row(1006, "Tommy")
        );
    }

    public void testTwoUncorrelatedScalarSubqueriesInOr() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id = ["
                + "    source = work_information | sort uid | stats max(uid)"
                + "  ] OR id = ["
                + "    source = work_information | sort uid | where department = 'DATA' | stats min(uid)"
                + "  ]"
                + " | fields id, name"
        );
        assertRowsAnyOrder(response, row(1002, "John"), row(1006, "Tommy"));
    }

    public void testTwoCorrelatedScalarSubqueriesInOr() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id = ["
                + "    source = work_information | where id = uid | stats max(uid)"
                + "  ] OR id = ["
                + "    source = work_information | sort uid | where department = 'DATA' | stats min(uid)"
                + "  ]"
                + " | fields id, name"
        );
        assertRowsAnyOrder(
            response,
            row(1000, "Jake"),
            row(1002, "John"),
            row(1003, "David"),
            row(1005, "Jane"),
            row(1006, "Tommy")
        );
    }

    public void testNestedScalarSubquery() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker | where id = ["
                + "    source = work_information"
                + "    | where uid = ["
                + "        source = occupation | stats min(salary)"
                + "      ] + 1000"
                + "    | sort department"
                + "    | stats max(uid)"
                + "  ]"
                + " | fields id, name"
        );
        assertRowsAnyOrder(response, row(1000, "Jake"));
    }

    public void testNestedScalarSubqueryWithTableAlias() throws IOException {
        Map<String, Object> response = executePpl(
            "source = worker as o | where id = ["
                + "    source = work_information as i"
                + "    | where uid = ["
                + "        source = occupation as n | stats min(n.salary)"
                + "      ] + 1000"
                + "    | sort i.department"
                + "    | stats max(i.uid)"
                + "  ]"
                + " | fields o.id, o.name"
        );
        assertRowsAnyOrder(response, row(1000, "Jake"));
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
