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
 * Filter-delegation matrix IT. Walks every enabled {@link Shape} through the
 * {@code (prefer_metadata_driver × fuse_dual_viable)} 4-cell matrix, asserts response
 * equality (against {@code ppl/expected/q{N}.json}) and per-cell {@code chosen_backend} /
 * {@code tree_shape} on the SHARD_FRAGMENT profile.
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
        // Restore defaults so a test that toggled {prefer,fuse} doesn't leak into the next.
        try {
            setPreferMetadataDriver(true);
            setFuseDualViable(true);
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
     * Per-cell matrix for {@code (prefer_metadata_driver, fuse_dual_viable)}.
     *
     * <p>Cell args are in the order: {@code (prefer=true,fuse=false)},
     * {@code (true,true)}, {@code (false,false)}, {@code (false,true)}.
     *
     * <p>{@link ChosenBackendandTreeShape#placeholder()} disables the stage assertion for that cell
     * (the response oracle still runs). Used for shapes whose query throws before
     * producing a profile (the 4 known-red bug shapes).
     */
    private enum Shape {
        // Single leaf (3)
        SINGLE_DUAL(1,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        SINGLE_NATIVE(2,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null)),
        SINGLE_DELEGATED(3,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),

        // Two-leaf AND (6)
        AND_DUAL_DUAL(4,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_NATIVE_NATIVE(5,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null)),
        AND_DELEGATED_DELEGATED(6,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_DUAL_NATIVE(7,
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_DUAL_DELEGATED(8,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_NATIVE_DELEGATED(9,
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),

        // Two-leaf OR (6)
        OR_DUAL_DUAL(10,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        OR_NATIVE_NATIVE(11,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null)),
        OR_DELEGATED_DELEGATED(12,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        OR_DUAL_NATIVE(13,
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION")),
        OR_DUAL_DELEGATED(14,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        OR_NATIVE_DELEGATED(15,
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION")),

        // Three-leaf AND (7)
        AND_DUAL_DUAL_DUAL(16,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_NATIVE_NATIVE_NATIVE(17,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null)),
        AND_DELEGATED_DELEGATED_DELEGATED(18,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_DUAL_DUAL_DELEGATED(19,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_DUAL_DUAL_NATIVE(20,
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_DELEGATED_DELEGATED_NATIVE(21,
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        AND_DUAL_DELEGATED_NATIVE(22,
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),

        // Three-leaf OR (7)
        OR_DUAL_DUAL_DUAL(23,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        OR_NATIVE_NATIVE_NATIVE(24,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null)),
        OR_DELEGATED_DELEGATED_DELEGATED(25,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        OR_DUAL_DUAL_DELEGATED(26,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        OR_DUAL_DUAL_NATIVE(27,
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION")),
        OR_DELEGATED_DELEGATED_NATIVE(28,
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION")),
        OR_DUAL_DELEGATED_NATIVE(29,
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION")),

        // NOT(leaf) (3)
        NOT_DUAL(30,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null)),
        NOT_NATIVE(31,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null)),
        NOT_DELEGATED(32,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),

        // Mixed connectors, depth 2 (6)
        MIXED_OR_OF_ANDS_OF_DUALS(33,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        MIXED_OR_OF_ANDS_OF_DELEGATED(34,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        MIXED_OR_OF_DUAL_DELEGATED_ANDS(35,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        MIXED_AND_OF_DUAL_DELEGATED_ORS(36,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        MIXED_OR_OF_AND_OF_DUALS_AND_NATIVE(37,
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION")),
        MIXED_NOT_OF_AND_OF_DUALS(38,
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null),
            new ChosenBackendandTreeShape("datafusion", null), new ChosenBackendandTreeShape("datafusion", null));

        final int queryNumber;
        final Map<SettingCombination, ChosenBackendandTreeShape> cells;

        Shape(int queryNumber,
              ChosenBackendandTreeShape preferTrue_fuseFalse,
              ChosenBackendandTreeShape preferTrue_fuseTrue,
              ChosenBackendandTreeShape preferFalse_fuseFalse,
              ChosenBackendandTreeShape preferFalse_fuseTrue) {
            this.queryNumber = queryNumber;
            Map<SettingCombination, ChosenBackendandTreeShape> map = new LinkedHashMap<>();
            map.put(new SettingCombination(true,  false), preferTrue_fuseFalse);
            map.put(new SettingCombination(true,  true),  preferTrue_fuseTrue);
            map.put(new SettingCombination(false, false), preferFalse_fuseFalse);
            map.put(new SettingCombination(false, true),  preferFalse_fuseTrue);
            this.cells = java.util.Collections.unmodifiableMap(map);
        }
    }

    // =====================================================================
    // Driver / matrix harness
    // =====================================================================

    /** Cluster-setting combination: ({@code prefer_metadata_driver}, {@code fuse_dual_viable}). */
    private record SettingCombination(boolean prefer, boolean fuse) {}

    /** Asserted SHARD_FRAGMENT profile fields. {@code treeShape == null} means the field
     *  must be absent (Lucene-as-driver has no delegation instruction). A {@code null}
     *  {@code chosenBackend} marks an unfilled placeholder cell — the harness will skip
     *  the stage assertions for that cell, but still validate the row oracle. */
    private record ChosenBackendandTreeShape(String chosenBackend, String treeShape) {
        static ChosenBackendandTreeShape placeholder() { return new ChosenBackendandTreeShape(null, null); }
        boolean isPlaceholder() { return chosenBackend == null; }
    }

    private void runShape(Shape shape) throws Exception {
        int queryNumber = shape.queryNumber;
        String ppl = DatasetProvisioner.loadResource(DATASET.queryResourcePath("ppl", "ppl", queryNumber)).trim();
        ppl = ppl.replace(DATASET.name, DATASET.indexName);

        for (Map.Entry<SettingCombination, ChosenBackendandTreeShape> entry : shape.cells.entrySet()) {
            SettingCombination key = entry.getKey();
            ChosenBackendandTreeShape expected = entry.getValue();
            setPreferMetadataDriver(key.prefer());
            setFuseDualViable(key.fuse());

            String label = shape + " prefer=" + key.prefer() + ",fuse=" + key.fuse();

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

            if (expected.isPlaceholder() == false) {
                Map<String, Object> stage = shardFragmentStage(response);
                assertEquals(label + " — chosen_backend", expected.chosenBackend(), stage.get("chosen_backend"));
                assertEquals(label + " — tree_shape", expected.treeShape(), stage.get("tree_shape"));
            }
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
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        for (Map<String, Object> stage : stages) {
            if ("SHARD_FRAGMENT".equals(stage.get("execution_type"))) return stage;
        }
        throw new AssertionError("No SHARD_FRAGMENT stage in profile: " + stages);
    }

    private void setFuseDualViable(boolean value) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.delegation.fuse_dual_viable\": " + value + "}}");
        client().performRequest(req);
    }

    private void setPreferMetadataDriver(boolean value) throws Exception {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.planner.prefer_metadata_driver\": " + value + "}}");
        client().performRequest(req);
    }
}
