/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filter-delegation matrix IT. Walks every enabled {@link Shape} through both
 * {@code prefer_metadata_driver} values, asserts response equality (against
 * {@code ppl/expected/q{N}.json}) and per-cell {@code chosen_backend} / {@code tree_shape}
 * on the SHARD_FRAGMENT profile.
 *
 * <p>Leaf vocabulary: <b>Dual</b> (DataFusion + Lucene, e.g. keyword EQUALS),
 * <b>Native</b> (DataFusion only, e.g. long EQUALS), <b>Delegated</b> (Lucene only,
 * e.g. {@code match()} on text).
 *
 * <p>TODO: also assert Lucene was actually consulted on the data node — count +
 * chosen_backend + tree_shape can all match even when DataFusion evaluated everything
 * natively. Hook into {@code profile.stages[*].tasks[*].data_node_metrics.ffm_collector_calls}
 * once #21972 lands.
 */
public class FilterDelegationGoldenIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("app_logs_filter_delegation", "app_logs_filter_delegation");

    /**
     * Use {@link ExpectedResponseStrategy#FAIL_ON_MISSING} so any shape with a query file
     * but no expected file is a hard failure — preventing silent passes during bring-up.
     */
    private static final ExpectedResponseStrategy STRATEGY = ExpectedResponseStrategy.FAIL_ON_MISSING;

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    @Override
    public void tearDown() throws Exception {
        // Restore default so a test that toggled prefer_metadata_driver doesn't leak into the next.
        try {
            setPreferMetadataDriver(true);
        } finally {
            super.tearDown();
        }
    }

    /**
     * One JUnit method per {@link Shape}. JUnit reports each independently, so a single
     * failure doesn't hide the rest, and any one shape can be targeted with gradle's
     * built-in {@code --tests "*FilterDelegationGoldenIT.testAndDualDual"} — no system
     * properties or harness plumbing needed.
     */
    public void testSingleDual() throws Exception { runShape(Shape.SINGLE_DUAL); }
    public void testSingleNative() throws Exception { runShape(Shape.SINGLE_NATIVE); }
    public void testSingleDelegated() throws Exception { runShape(Shape.SINGLE_DELEGATED); }

    public void testAndDualDual() throws Exception { runShape(Shape.AND_DUAL_DUAL); }
    public void testAndNativeNative() throws Exception { runShape(Shape.AND_NATIVE_NATIVE); }
    public void testAndDelegatedDelegated() throws Exception { runShape(Shape.AND_DELEGATED_DELEGATED); }
    public void testAndDualNative() throws Exception { runShape(Shape.AND_DUAL_NATIVE); }
    public void testAndDualDelegated() throws Exception { runShape(Shape.AND_DUAL_DELEGATED); }
    public void testAndNativeDelegated() throws Exception { runShape(Shape.AND_NATIVE_DELEGATED); }

    public void testOrDualDual() throws Exception { runShape(Shape.OR_DUAL_DUAL); }
    public void testOrNativeNative() throws Exception { runShape(Shape.OR_NATIVE_NATIVE); }
    public void testOrDelegatedDelegated() throws Exception { runShape(Shape.OR_DELEGATED_DELEGATED); }
    public void testOrDualNative() throws Exception { runShape(Shape.OR_DUAL_NATIVE); }
    public void testOrDualDelegated() throws Exception { runShape(Shape.OR_DUAL_DELEGATED); }
    public void testOrNativeDelegated() throws Exception { runShape(Shape.OR_NATIVE_DELEGATED); }

    public void testAndDualDualDual() throws Exception { runShape(Shape.AND_DUAL_DUAL_DUAL); }
    public void testAndNativeNativeNative() throws Exception { runShape(Shape.AND_NATIVE_NATIVE_NATIVE); }
    public void testAndDelegatedDelegatedDelegated() throws Exception { runShape(Shape.AND_DELEGATED_DELEGATED_DELEGATED); }
    public void testAndDualDualDelegated() throws Exception { runShape(Shape.AND_DUAL_DUAL_DELEGATED); }
    public void testAndDualDualNative() throws Exception { runShape(Shape.AND_DUAL_DUAL_NATIVE); }
    public void testAndDelegatedDelegatedNative() throws Exception { runShape(Shape.AND_DELEGATED_DELEGATED_NATIVE); }
    public void testAndDualDelegatedNative() throws Exception { runShape(Shape.AND_DUAL_DELEGATED_NATIVE); }

    public void testOrDualDualDual() throws Exception { runShape(Shape.OR_DUAL_DUAL_DUAL); }
    public void testOrNativeNativeNative() throws Exception { runShape(Shape.OR_NATIVE_NATIVE_NATIVE); }
    public void testOrDelegatedDelegatedDelegated() throws Exception { runShape(Shape.OR_DELEGATED_DELEGATED_DELEGATED); }
    public void testOrDualDualDelegated() throws Exception { runShape(Shape.OR_DUAL_DUAL_DELEGATED); }
    public void testOrDualDualNative() throws Exception { runShape(Shape.OR_DUAL_DUAL_NATIVE); }
    public void testOrDelegatedDelegatedNative() throws Exception { runShape(Shape.OR_DELEGATED_DELEGATED_NATIVE); }
    public void testOrDualDelegatedNative() throws Exception { runShape(Shape.OR_DUAL_DELEGATED_NATIVE); }

    public void testNotDual() throws Exception { runShape(Shape.NOT_DUAL); }
    public void testNotNative() throws Exception { runShape(Shape.NOT_NATIVE); }
    public void testNotDelegated() throws Exception { runShape(Shape.NOT_DELEGATED); }

    public void testMixedOrOfAndsOfDuals() throws Exception { runShape(Shape.MIXED_OR_OF_ANDS_OF_DUALS); }
    public void testMixedOrOfAndsOfDelegated() throws Exception { runShape(Shape.MIXED_OR_OF_ANDS_OF_DELEGATED); }
    public void testMixedOrOfDualDelegatedAnds() throws Exception { runShape(Shape.MIXED_OR_OF_DUAL_DELEGATED_ANDS); }
    public void testMixedAndOfDualDelegatedOrs() throws Exception { runShape(Shape.MIXED_AND_OF_DUAL_DELEGATED_ORS); }
    public void testMixedOrOfAndOfDualsAndNative() throws Exception { runShape(Shape.MIXED_OR_OF_AND_OF_DUALS_AND_NATIVE); }
    public void testMixedNotOfAndOfDuals() throws Exception { runShape(Shape.MIXED_NOT_OF_AND_OF_DUALS); }

    // =====================================================================
    // Shape catalogue — owns query number, per-cell ShardStage, and oracle
    // location identity in one place. The query body lives at
    // {@code ppl/q{N}.ppl}, expected response at {@code ppl/expected/q{N}.json}.
    // =====================================================================

    /**
     * Per-shape expected (chosen_backend, tree_shape), one cell per {@code prefer_metadata_driver}
     * value: {@code (preferTrue, preferFalse)}.
     */
    private enum Shape {
        // Cells are (prefer_metadata_driver=true, prefer=false). prefer=true lets Lucene drive the
        // whole stage when every arm is Lucene-viable (chosen=lucene, no tree_shape); a native arm
        // forces datafusion. prefer=false runs the combiner, where delegation shape is decided by
        // tree position: a dual-viable leaf stays performance-delegated under AND; under OR/NOT it's
        // reclassified to correctness and ships to Lucene (fusing with same-backend correctness
        // siblings). A delegated shipment beside a native arm under OR is INTERLEAVED; otherwise CONJUNCTIVE.

        // Single leaf (3)
        SINGLE_DUAL(1, lucene(), df("CONJUNCTIVE")),
        SINGLE_NATIVE(2, df(null), df(null)),
        SINGLE_DELEGATED(3, lucene(), df("CONJUNCTIVE")),

        // Two-leaf AND (6)
        AND_DUAL_DUAL(4, lucene(), df("CONJUNCTIVE")),
        AND_NATIVE_NATIVE(5, df(null), df(null)),
        AND_DELEGATED_DELEGATED(6, lucene(), df("CONJUNCTIVE")),
        AND_DUAL_NATIVE(7, df("CONJUNCTIVE"), df("CONJUNCTIVE")),
        AND_DUAL_DELEGATED(8, lucene(), df("CONJUNCTIVE")),
        AND_NATIVE_DELEGATED(9, df("CONJUNCTIVE"), df("CONJUNCTIVE")),

        // Two-leaf OR (6)
        OR_DUAL_DUAL(10, lucene(), df("CONJUNCTIVE")),
        OR_NATIVE_NATIVE(11, df(null), df(null)),
        OR_DELEGATED_DELEGATED(12, lucene(), df("CONJUNCTIVE")),
        OR_DUAL_NATIVE(13, df("INTERLEAVED_BOOLEAN_EXPRESSION"), df("INTERLEAVED_BOOLEAN_EXPRESSION")),
        OR_DUAL_DELEGATED(14, lucene(), df("CONJUNCTIVE")),
        OR_NATIVE_DELEGATED(15, df("INTERLEAVED_BOOLEAN_EXPRESSION"), df("INTERLEAVED_BOOLEAN_EXPRESSION")),

        // Three-leaf AND (7)
        AND_DUAL_DUAL_DUAL(16, lucene(), df("CONJUNCTIVE")),
        AND_NATIVE_NATIVE_NATIVE(17, df(null), df(null)),
        AND_DELEGATED_DELEGATED_DELEGATED(18, lucene(), df("CONJUNCTIVE")),
        AND_DUAL_DUAL_DELEGATED(19, lucene(), df("CONJUNCTIVE")),
        AND_DUAL_DUAL_NATIVE(20, df("CONJUNCTIVE"), df("CONJUNCTIVE")),
        AND_DELEGATED_DELEGATED_NATIVE(21, df("CONJUNCTIVE"), df("CONJUNCTIVE")),
        AND_DUAL_DELEGATED_NATIVE(22, df("CONJUNCTIVE"), df("CONJUNCTIVE")),

        // Three-leaf OR (7)
        OR_DUAL_DUAL_DUAL(23, lucene(), df("CONJUNCTIVE")),
        OR_NATIVE_NATIVE_NATIVE(24, df(null), df(null)),
        OR_DELEGATED_DELEGATED_DELEGATED(25, lucene(), df("CONJUNCTIVE")),
        OR_DUAL_DUAL_DELEGATED(26, lucene(), df("CONJUNCTIVE")),
        OR_DUAL_DUAL_NATIVE(27, df("INTERLEAVED_BOOLEAN_EXPRESSION"), df("INTERLEAVED_BOOLEAN_EXPRESSION")),
        OR_DELEGATED_DELEGATED_NATIVE(28, df("INTERLEAVED_BOOLEAN_EXPRESSION"), df("INTERLEAVED_BOOLEAN_EXPRESSION")),
        OR_DUAL_DELEGATED_NATIVE(29, df("INTERLEAVED_BOOLEAN_EXPRESSION"), df("INTERLEAVED_BOOLEAN_EXPRESSION")),

        // NOT(leaf) (3)
        NOT_DUAL(30, df("CONJUNCTIVE"), df("CONJUNCTIVE")),
        NOT_NATIVE(31, df(null), df(null)),
        NOT_DELEGATED(32, df("CONJUNCTIVE"), df("CONJUNCTIVE")),

        // Mixed connectors, depth 2 (6)
        MIXED_OR_OF_ANDS_OF_DUALS(33, lucene(), df("CONJUNCTIVE")),
        MIXED_OR_OF_ANDS_OF_DELEGATED(34, lucene(), df("CONJUNCTIVE")),
        MIXED_OR_OF_DUAL_DELEGATED_ANDS(35, lucene(), df("CONJUNCTIVE")),
        MIXED_AND_OF_DUAL_DELEGATED_ORS(36, lucene(), df("CONJUNCTIVE")),
        MIXED_OR_OF_AND_OF_DUALS_AND_NATIVE(37, df("INTERLEAVED_BOOLEAN_EXPRESSION"), df("INTERLEAVED_BOOLEAN_EXPRESSION")),
        MIXED_NOT_OF_AND_OF_DUALS(38, df("CONJUNCTIVE"), df("CONJUNCTIVE"));

        final int queryNumber;
        final Map<Boolean, ChosenBackendandTreeShape> cells;

        Shape(int queryNumber, ChosenBackendandTreeShape preferTrue, ChosenBackendandTreeShape preferFalse) {
            this.queryNumber = queryNumber;
            Map<Boolean, ChosenBackendandTreeShape> map = new LinkedHashMap<>();
            map.put(true, preferTrue);
            map.put(false, preferFalse);
            this.cells = java.util.Collections.unmodifiableMap(map);
        }
    }

    private static ChosenBackendandTreeShape lucene() {
        return new ChosenBackendandTreeShape("lucene", null);
    }

    private static ChosenBackendandTreeShape df(String treeShape) {
        return new ChosenBackendandTreeShape("datafusion", treeShape);
    }

    // =====================================================================
    // Driver / matrix harness
    // =====================================================================

    /** Asserted SHARD_FRAGMENT profile fields. {@code treeShape == null} means the field
     *  must be absent (Lucene-as-driver, or no delegation instruction). */
    private record ChosenBackendandTreeShape(String chosenBackend, String treeShape) {}

    private void runShape(Shape shape) throws Exception {
        int queryNumber = shape.queryNumber;
        String ppl = DatasetProvisioner.loadResource(DATASET.queryResourcePath("ppl", "ppl", queryNumber)).trim();
        ppl = ppl.replace(DATASET.name, DATASET.indexName);

        for (Map.Entry<Boolean, ChosenBackendandTreeShape> entry : shape.cells.entrySet()) {
            boolean prefer = entry.getKey();
            ChosenBackendandTreeShape expected = entry.getValue();
            setPreferMetadataDriver(prefer);

            String label = shape + " prefer=" + prefer;

            // Profile=false path — guards against any profile-only-induced behavior change masking a regression.
            Map<String, Object> bareResponse = executePpl(ppl, false);
            String bareValidationError = ResponseValidator.validate(DATASET, "ppl", queryNumber, bareResponse, STRATEGY);
            if (bareValidationError != null) {
                fail(label + " (profile=false) — " + bareValidationError);
            }

            // Profile=true path — same execution path, additionally carries SHARD_FRAGMENT profile.
            Map<String, Object> response = executePpl(ppl, true);
            String validationError = ResponseValidator.validate(DATASET, "ppl", queryNumber, response, STRATEGY);
            if (validationError != null) {
                fail(label + " (profile=true) — " + validationError);
            }

            Map<String, Object> stage = shardFragmentStage(response);
            assertEquals(label + " — chosen_backend", expected.chosenBackend(), stage.get("chosen_backend"));
            assertEquals(label + " — tree_shape", expected.treeShape(), stage.get("tree_shape"));
        }
    }

    /**
     * Executes a PPL query against the real SQL-plugin endpoint. When {@code profile=true},
     * the response additionally carries the analytics-engine {@code profile} block.
     * Mirrors {@code rows} ↔ {@code datarows} so {@link ResponseValidator} works.
     */
    private Map<String, Object> executePpl(String ppl, boolean profile) throws Exception {
        Request request = new Request("POST", "/_plugins/_ppl");
        String body = profile
            ? "{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}"
            : "{\"query\": \"" + escapeJson(ppl) + "\"}";
        request.setJsonEntity(body);
        Map<String, Object> parsed = assertOkAndParse(client().performRequest(request), "PPL: " + ppl);
        if (parsed.containsKey("datarows") && parsed.containsKey("rows") == false) {
            parsed.put("rows", parsed.get("datarows"));
        }
        return parsed;
    }

    private Map<String, Object> shardFragmentStage(Map<String, Object> response) {
        @SuppressWarnings("unchecked")
        Map<String, Object> profile = (Map<String, Object>) response.get("profile");
        if (profile == null) {
            throw new AssertionError("No 'profile' block in response — request must set profile=true");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> plan = (Map<String, Object>) profile.get("plan");
        if (plan == null) {
            throw new AssertionError("No 'plan' block in profile: " + profile);
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> stages = (List<Map<String, Object>>) plan.get("stages");
        for (Map<String, Object> stage : stages) {
            if ("SHARD_FRAGMENT".equals(stage.get("execution_type"))) return stage;
        }
        throw new AssertionError("No SHARD_FRAGMENT stage in profile: " + stages);
    }

    private void setPreferMetadataDriver(boolean value) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.planner.prefer_metadata_driver\": " + value + "}}");
        client().performRequest(req);
    }
}
