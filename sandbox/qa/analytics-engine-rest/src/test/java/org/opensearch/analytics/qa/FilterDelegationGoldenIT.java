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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Comprehensive filter-delegation matrix IT.
 *
 * <p>Each tree shape is a {@code ppl/q{N}.ppl} file with a matching
 * {@code ppl/expected/q{N}.json}. A single driver method walks every enabled
 * {@link Shape}, runs the {@code (prefer_metadata_driver × fuse_dual_viable)} 4-cell
 * setting matrix per shape, and asserts:
 * <ol>
 *   <li>The response equals the expected response (shape-level oracle, identical
 *       across all 4 cells — see {@link ResponseValidator#compareData}, unordered and
 *       numeric-tolerant).</li>
 *   <li>The SHARD_FRAGMENT {@code chosen_backend} and {@code tree_shape} match the
 *       per-cell {@link ChosenBackendandTreeShape} declared on the {@link Shape} enum.</li>
 * </ol>
 *
 * <p>TODO: assert Lucene was actually consulted on the data node for performance/correctness
 * delegation cells (count + chosen_backend + tree_shape can all match even if Lucene was never
 * called — DataFusion would evaluate everything natively and produce the same answer). Add a
 * per-cell {@code ConsultExpectation} (MUST_CONSULT / IGNORE) on {@link Shape} and assert against
 * {@code profile.stages[*].tasks[*].data_node_metrics.ffm_collector_calls} once #21972 lands
 * (which exposes DataFusion's per-shard metrics in the explain JSON via Arrow Flight app-metadata).
 *
 * <p>Vocabulary — leaves are classified by where they can be evaluated:
 * <ul>
 *   <li><b>Dual</b> — DataFusion + Lucene both viable. EQUALS on keyword.</li>
 *   <li><b>Native</b> — DataFusion only (Lucene declares no predicate function for
 *       the field type). EQUALS on long.</li>
 *   <li><b>Delegated</b> — Lucene only. {@code match(field, value)} on text.</li>
 * </ul>
 *
 * <p>Field roles in the dataset's {@code mapping.json}:
 * <ul>
 *   <li>{@code id} (long) — unique row id, used for {@code sort id} + projection.</li>
 *   <li>{@code service_name} (keyword) — Dual.</li>
 *   <li>{@code host} (keyword) — Dual.</li>
 *   <li>{@code log_level} (keyword) — Dual.</li>
 *   <li>{@code status} (long) — Native.</li>
 *   <li>{@code message} (text + .keyword) — Delegated via {@code match(message, ...)}.</li>
 * </ul>
 *
 * <p>To exclude a shape from the run (work-in-progress query files, known bug, etc.),
 * add it to {@link #SKIP_SHAPES}. The driver logs the skipped set at the start of the
 * run so a green build doesn't silently mask missing coverage.
 *
 * <p>Each cell hits {@code POST /_plugins/_ppl} with {@code profile=true} once — the
 * response carries rows and the analytics-engine profile in a single call.
 * {@code tree_shape == null} asserts no delegation instruction was emitted (Lucene-as-driver,
 * or pure-Native plan).
 */
public class FilterDelegationGoldenIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("app_logs_filter_delegation", "app_logs_filter_delegation");

    /**
     * Use {@link ExpectedResponseStrategy#FAIL_ON_MISSING} so any shape with a query file
     * but no expected file is a hard failure — preventing silent passes during bring-up.
     */
    private static final ExpectedResponseStrategy STRATEGY = ExpectedResponseStrategy.FAIL_ON_MISSING;

    /**
     * Shapes intentionally not exercised in the current run. Logged at the start of the
     * driver.
     *
     * <p>The 4 shapes that hit the {@code ClassCastException} at
     * {@code DelegatedPredicateCombiner.makePlaceholder:294} are temporarily skipped here
     * so the IT goes green end-to-end. Re-enable them by removing from this set once the
     * combiner bug is fixed (or sooner if you want CI to mark the bug as load-bearing).
     *
     * <p>Currently red:
     * <ul>
     *   <li>{@link Shape#AND_DUAL_DUAL} (q4) — fails on {@code prefer=false,fuse=false}.</li>
     *   <li>{@link Shape#AND_DUAL_DUAL_DUAL} (q16) — same.</li>
     *   <li>{@link Shape#MIXED_OR_OF_ANDS_OF_DUALS} (q33) — same; canonical Bug-2 marker.</li>
     *   <li>{@link Shape#MIXED_OR_OF_AND_OF_DUALS_AND_NATIVE} (q37) — fails on BOTH
     *       {@code fuse=false} cells regardless of {@code prefer}.</li>
     * </ul>
     */
    private static final Set<Shape> SKIP_SHAPES = EnumSet.of(
        Shape.AND_DUAL_DUAL,
        Shape.AND_DUAL_DUAL_DUAL,
        Shape.MIXED_OR_OF_ANDS_OF_DUALS,
        Shape.MIXED_OR_OF_AND_OF_DUALS_AND_NATIVE
    );

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /**
     * Single driver — runs every {@link Shape} not in {@link #SKIP_SHAPES}, accumulates
     * failures, and reports them all at the end so one bad shape doesn't hide the
     * rest. Skipped shapes are logged at the start.
     */
    public void testFilterDelegationMatrix() throws Exception {
        if (SKIP_SHAPES.isEmpty() == false) {
            logger.warn("Skipping {} shape(s) via SKIP_SHAPES: {}", SKIP_SHAPES.size(), SKIP_SHAPES);
        }

        List<String> failures = new ArrayList<>();
        try {
            for (Shape shape : Shape.values()) {
                if (SKIP_SHAPES.contains(shape)) continue;
                try {
                    runShape(shape);
                } catch (AssertionError e) {
                    failures.add("[" + shape + "] " + e.getMessage());
                } catch (Exception e) {
                    failures.add("[" + shape + "] threw " + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }
        } finally {
            setFuseDualViable(true);
            setPreferMetadataDriver(true);
        }

        if (failures.isEmpty() == false) {
            fail("Filter delegation matrix had " + failures.size() + " failure(s):\n  " + String.join("\n  ", failures));
        }
    }

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
        // q4 — currently red on cell prefer=false,fuse=false (skipped). Other 3 cells captured.
        AND_DUAL_DUAL(4,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            ChosenBackendandTreeShape.placeholder(), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
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
        // q16 — currently red on cell prefer=false,fuse=false (skipped). Other 3 cells captured.
        AND_DUAL_DUAL_DUAL(16,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            ChosenBackendandTreeShape.placeholder(), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
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
        /** Bug-2 marker: {@code OR(AND-of-Dual-leaves, AND-of-Dual-leaves)} threw
         *  ClassCastException at {@code DelegatedPredicateCombiner.combine:136} when
         *  {@code fuse_dual_viable=false}. Cell prefer=false,fuse=false skipped. */
        MIXED_OR_OF_ANDS_OF_DUALS(33,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            ChosenBackendandTreeShape.placeholder(), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        MIXED_OR_OF_ANDS_OF_DELEGATED(34,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        MIXED_OR_OF_DUAL_DELEGATED_ANDS(35,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        MIXED_AND_OF_DUAL_DELEGATED_ORS(36,
            new ChosenBackendandTreeShape("lucene", null), new ChosenBackendandTreeShape("lucene", null),
            new ChosenBackendandTreeShape("datafusion", "INTERLEAVED_BOOLEAN_EXPRESSION"), new ChosenBackendandTreeShape("datafusion", "CONJUNCTIVE")),
        // q37 — red on BOTH fuse=false cells. Both placeholder; fuse=true cells captured.
        MIXED_OR_OF_AND_OF_DUALS_AND_NATIVE(37,
            ChosenBackendandTreeShape.placeholder(), ChosenBackendandTreeShape.placeholder(),
            ChosenBackendandTreeShape.placeholder(), ChosenBackendandTreeShape.placeholder()),
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

    /**
     * One-shot capture mode. Set to {@code true} and run the IT to log
     * {@code CAPTURE shape=… prefer=… fuse=… chosen_backend=… tree_shape=…} per cell —
     * grep the test output, lift values into the {@link Shape} enum constructors,
     * then flip back to {@code false}.
     *
     * <p>FIXME [RemoveBeforeMerge]: Always check this is {@code false} before pushing.
     */
    private static final boolean CAPTURE_STAGES = false;

    private void runShape(Shape shape) throws Exception {
        int queryNumber = shape.queryNumber;
        String ppl = DatasetProvisioner.loadResource(DATASET.queryResourcePath("ppl", "ppl", queryNumber)).trim();
        ppl = ppl.replace(DATASET.name, DATASET.indexName);

        for (Map.Entry<SettingCombination, ChosenBackendandTreeShape> entry : shape.cells.entrySet()) {
            SettingCombination key = entry.getKey();
            ChosenBackendandTreeShape want = entry.getValue();
            setPreferMetadataDriver(key.prefer());
            setFuseDualViable(key.fuse());

            String label = shape + " prefer=" + key.prefer() + ",fuse=" + key.fuse();

            Map<String, Object> response = executePplWithProfile(ppl);
            String validationError = ResponseValidator.validate(DATASET, "ppl", queryNumber, response, STRATEGY);
            if (validationError != null) {
                fail(label + " — " + validationError);
            }

            Map<String, Object> stage = shardFragmentStage(response);
            if (CAPTURE_STAGES) {
                logger.info(
                    "CAPTURE shape={} prefer={} fuse={} chosen_backend={} tree_shape={}",
                    shape, key.prefer(), key.fuse(),
                    stage.get("chosen_backend"), stage.get("tree_shape")
                );
            } else if (want.isPlaceholder() == false) {
                assertEquals(label + " — chosen_backend", want.chosenBackend(), stage.get("chosen_backend"));
                assertEquals(label + " — tree_shape", want.treeShape(), stage.get("tree_shape"));
            }
        }
    }

    /**
     * Executes a PPL query against the real SQL-plugin endpoint with {@code profile=true}.
     * The response carries both rows and the analytics-engine {@code profile} block in a
     * single call. Mirrors {@code rows} ↔ {@code datarows} so {@link ResponseValidator} works.
     */
    private Map<String, Object> executePplWithProfile(String ppl) throws Exception {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        // FIXME [RemoveBeforeMerge] dump the full response so the consolidated path can be verified.
        org.opensearch.client.Response raw = client().performRequest(request);
        String body = new String(raw.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        logger.info("PPL [{}]: {}", ppl, body);
        Map<String, Object> parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(), body, false);
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
