/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import org.opensearch.analytics.qa.AnalyticsRestTestCase;
import org.opensearch.analytics.qa.Dataset;
import org.opensearch.analytics.qa.DatasetProvisioner;
import org.opensearch.analytics.qa.DatasetProvisioner.SegmentLayout;
import org.opensearch.analytics.qa.planshape.ExpectedQueryPlan.PlanShapeLayer;
import org.opensearch.client.Request;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared harness for plan-shape golden ITs. One JUnit case per (query, combo): each applies the
 * combo's cluster settings, captures the {@code /_plugins/_ppl?profile=true} plan under both segment
 * layouts ({@link SegmentLayout}), renders it into the {@link PlanShapeLayer}s, and asserts each
 * against the query's golden. The non-shard layers are segment-independent (asserted once); the
 * shard physical plan is asserted per segment layout.
 *
 * <p>Per-workload subclasses supply a {@link WorkloadSpec} via a static {@code @ParametersFactory}
 * that calls {@link #buildQueryAndSettingCombinationsToRun(WorkloadSpec)} (the factory must be
 * static, so it cannot read an instance field).
 */
public abstract class PlanShapeGoldenTestBase extends AnalyticsRestTestCase {

    /** Classpath path of the global combos registry. */
    private static final String COMBOS_RESOURCE = "planshape/combos.yaml";

    /** One (query, combo) pair — the unit a JUnit case asserts. {@code toString()} names the case. */
    public static final class GoldenCase {
        final WorkloadSpec workload;
        final String queryId;
        final String comboName;

        GoldenCase(WorkloadSpec workload, String queryId, String comboName) {
            this.workload = workload;
            this.queryId = queryId;
            this.comboName = comboName;
        }

        @Override
        public String toString() {
            return workload.name() + "/" + queryId + "[" + comboName + "]";
        }
    }

    /**
     * The two segment layouts every combo is captured under — a controlled axis (see core-axioms):
     * the shard physical plan can legitimately differ with segment count (e.g. the scan's
     * {@code input_partitions}). The non-shard layers (post_cbo / fragment / coord) are NOT
     * segment-derived and are captured once (from the single-segment index).
     */
    private static final SegmentLayout[] SEGMENT_LAYOUTS = { SegmentLayout.SINGLE_SEGMENT, SegmentLayout.MULTI_SEGMENT };

    /** One unit of YAML indentation; golden nesting is whole multiples of this. */
    private static final String INDENT = "  ";

    /** YAML key for the deduped shard physical plan (single-segment == multi-segment). */
    private static final String SHARD_KEY = PlanShapeLayer.SHARD_PHYSICAL.yamlKey();
    /** YAML keys for the per-segment-layout shard physical plans (used only when they diverge). */
    private static String shardKey(SegmentLayout layout) {
        return SHARD_KEY + "_" + layout.suffix;
    }

    private final GoldenCase testCase;

    protected PlanShapeGoldenTestBase(GoldenCase testCase) {
        this.testCase = testCase;
    }

    /**
     * Build the JUnit parameter rows — one per (query, setting-combo) — for a workload, the source for
     * the subclass {@code @ParametersFactory}. Each query's golden declares which combos it applies
     * to, intersected with the combos this run exercises ({@link #getSettingCombosToRun}). Static, so
     * it can't read an instance field.
     */
    protected static List<Object[]> buildQueryAndSettingCombinationsToRun(WorkloadSpec spec) {
        try {
            SettingsComboRegistry combos = SettingsComboRegistry.load(COMBOS_RESOURCE);
            List<String> runCombos = getSettingCombosToRun(combos);
            // When generating, a golden may not exist yet — use every run-combo per query.
            // When asserting, only the combos the golden declares it applies to (intersected).
            boolean generating = System.getProperty("plan.generate") != null;

            List<Object[]> cases = new ArrayList<>();
            for (String queryId : spec.queryIds()) {
                List<String> queryCombos = generating
                    ? runCombos
                    : ExpectedQueryPlanLoader.loadGolden(spec.goldenResourcePath(queryId), spec.queryDir())
                        .appliesCombos().stream().filter(runCombos::contains).toList();
                for (String comboName : queryCombos) {
                    cases.add(new Object[] { new GoldenCase(spec, queryId, comboName) });
                }
            }
            return cases;
        } catch (IOException e) {
            throw new RuntimeException("failed building plan-shape cases for workload " + spec.name(), e);
        }
    }

    /** The {@link SettingsCombo} names this run should exercise: {@code -Dplan.combos=a,b} or {@code all}, else the file defaults. */
    private static List<String> getSettingCombosToRun(SettingsComboRegistry combos) {
        String prop = System.getProperty("plan.combos");
        if (prop == null || prop.isBlank()) {
            return combos.defaults();
        }
        if (prop.trim().equals("all")) {
            return combos.allNames();
        }
        List<String> requested = List.of(prop.trim().split("\\s*,\\s*"));
        // Validate up front: an unknown name in assert mode would otherwise just filter to zero cases
        // and silently run nothing. byName throws with the list of defined combos.
        requested.forEach(combos::byName);
        return requested;
    }

    public void testPlanShape() throws Exception {
        SettingsComboRegistry combos = SettingsComboRegistry.load(COMBOS_RESOURCE);
        SettingsCombo combo = combos.byName(testCase.comboName);
        boolean generating = System.getProperty("plan.generate") != null;
        // Assertion mode needs the golden; generate mode does not (it's creating it) and reads the
        // query text straight from the workload's query file.
        ExpectedQueryPlan golden = generating
            ? null
            : ExpectedQueryPlanLoader.loadGolden(testCase.workload.goldenResourcePath(testCase.queryId), testCase.workload.queryDir());
        String queryText = generating
            ? loadResource(testCase.workload.queryDir() + "/" + testCase.queryId + ".ppl").strip()
            : golden.queryText();

        applyClusterSettings(combo);

        // Capture under both segment layouts. The non-shard layers come from the single-segment
        // index (they are not segment-derived); the shard physical plan is captured per layout.
        Map<SegmentLayout, ProfilePlanExtractor> captured = new LinkedHashMap<>();
        for (SegmentLayout layout : SEGMENT_LAYOUTS) {
            provisionOnce(testCase.workload, combo.numberOfShards(), layout);
            String indexName = indexNameFor(testCase.workload, combo.numberOfShards(), layout);
            // Point the query at the per-layout index. Replaces every occurrence of the dataset
            // name; relies on it being a distinctive token (the source name), not a substring that
            // also appears elsewhere in the query.
            String ppl = queryText.replace(testCase.workload.dataset().name, indexName);
            Map<String, Object> response = executePplWithProfile(ppl);
            captured.put(layout, ProfilePlanExtractor.extractFrom(response, indexName));
        }

        // -Dplan.generate=write|print : capture instead of assert. write -> the q{N}.plan.yaml in
        // the source tree; print -> log the block. Absent -> assertion mode.
        String generate = System.getProperty("plan.generate");
        if (generate != null) {
            String comboYaml = renderComboYaml(captured);
            if ("print".equals(generate)) {
                logger.info("plan-shape generated for {}:\n{}", testCase, comboYaml);
            } else {
                writeGolden(comboYaml);
            }
            return;
        }

        // Non-shard layers: assert once (from the single-segment capture — segment-independent).
        ProfilePlanExtractor singleSegment = captured.get(SegmentLayout.SINGLE_SEGMENT);
        for (PlanShapeLayer layer : PlanShapeLayer.values()) {
            if (layer == PlanShapeLayer.SHARD_PHYSICAL) {
                continue;
            }
            assertPlanShapeLayer(golden.expected(testCase.comboName, layer), singleSegment.layer(layer), layer.toString());
        }
        assertShardPhysical(golden, captured);
    }

    /**
     * Assert the shard physical plan for both segment layouts. A golden stores either a single
     * deduped {@code shard_physical} (when the two are identical) or both {@code shard_physical_1seg}
     * / {@code shard_physical_nseg} (when they diverge). Resolve per layout: prefer the
     * layout-specific key, else fall back to the deduped key.
     */
    private void assertShardPhysical(ExpectedQueryPlan golden, Map<SegmentLayout, ProfilePlanExtractor> captured) {
        for (SegmentLayout layout : SEGMENT_LAYOUTS) {
            Optional<String> actual = captured.get(layout).layer(PlanShapeLayer.SHARD_PHYSICAL);
            Optional<String> expected = golden.expectedTextForLayerKey(testCase.comboName, shardKey(layout));
            if (expected.isEmpty()) {
                expected = golden.expectedTextForLayerKey(testCase.comboName, SHARD_KEY); // deduped
            }
            assertPlanShapeLayer(expected, actual, PlanShapeLayer.SHARD_PHYSICAL + "[" + layout.suffix + "]");
        }
    }

    /**
     * Render this (query, combo)'s captured layers as the YAML body that sits under {@code plans:}.
     * Non-shard layers come from the single-segment capture (segment-independent). The shard physical
     * plan is deduped: if the single- and multi-segment plans are identical, emit one
     * {@code shard_physical}; if they diverge, emit both {@code shard_physical_1seg} and
     * {@code shard_physical_nseg}.
     */
    private String renderComboYaml(Map<SegmentLayout, ProfilePlanExtractor> captured) {
        ProfilePlanExtractor singleSegment = captured.get(SegmentLayout.SINGLE_SEGMENT);
        StringBuilder yaml = new StringBuilder();
        yaml.append(INDENT).append(testCase.comboName).append(":\n");
        for (PlanShapeLayer layer : PlanShapeLayer.values()) {
            if (layer == PlanShapeLayer.SHARD_PHYSICAL) {
                appendShardPhysical(yaml, captured);
                continue;
            }
            singleSegment.layer(layer).ifPresent(text -> appendBlock(yaml, layer.yamlKey(), text));
        }
        return yaml.toString();
    }

    /** Emit the shard physical plan(s): one deduped key if the layouts match, else both. */
    private void appendShardPhysical(StringBuilder yaml, Map<SegmentLayout, ProfilePlanExtractor> captured) {
        Optional<String> oneSeg = captured.get(SegmentLayout.SINGLE_SEGMENT).layer(PlanShapeLayer.SHARD_PHYSICAL);
        Optional<String> multiSeg = captured.get(SegmentLayout.MULTI_SEGMENT).layer(PlanShapeLayer.SHARD_PHYSICAL);
        if (oneSeg.isEmpty() && multiSeg.isEmpty()) {
            return; // no shard physical (Lucene fast-path): omit, asserted absent for both
        }
        if (oneSeg.equals(multiSeg)) {
            appendBlock(yaml, SHARD_KEY, oneSeg.get()); // identical across layouts -> dedup
            return;
        }
        // Diverge: pin each layout under its own key. This also covers the asymmetric case where
        // one layout has a shard plan and the other doesn't (Optional.empty) — the present side
        // gets its key, the absent side is simply omitted (asserted absent for that layout).
        oneSeg.ifPresent(text -> appendBlock(yaml, shardKey(SegmentLayout.SINGLE_SEGMENT), text));
        multiSeg.ifPresent(text -> appendBlock(yaml, shardKey(SegmentLayout.MULTI_SEGMENT), text));
    }

    private static void appendBlock(StringBuilder yaml, String yamlKey, String text) {
        yaml.append(INDENT.repeat(2)).append(yamlKey).append(": |\n");
        for (String line : text.split("\n")) {
            yaml.append(INDENT.repeat(3)).append(line).append('\n');
        }
    }

    /**
     * Write this (query, combo)'s YAML into the query's golden file in the source tree (path from
     * {@code -Dplan.resourcesDir}). Since a golden holds all combos but each JUnit case is one
     * combo, accumulate combos per query (single-fork sequential run) and rewrite the whole file
     * each time, so the file always reflects every combo generated so far.
     */
    private void writeGolden(String comboYaml) {
        String resourcesDir = System.getProperty("plan.resourcesDir");
        if (resourcesDir == null) {
            throw new IllegalStateException("plan.generate=write needs -Dplan.resourcesDir (set by build.gradle)");
        }
        Map<String, String> yamlByCombo = GENERATED_YAML_BY_QUERY.computeIfAbsent(
            testCase.workload.name() + "/" + testCase.queryId, k -> new LinkedHashMap<>());
        yamlByCombo.put(testCase.comboName, comboYaml);

        StringBuilder doc = new StringBuilder();
        doc.append("# Generated via -Dplan.generate=write.\n");
        doc.append("query: ").append(testCase.queryId).append('\n');
        doc.append("ppl_file: ").append(testCase.queryId).append(".ppl\n");
        doc.append("applies: [").append(String.join(", ", yamlByCombo.keySet())).append("]\n");
        doc.append("plans:\n");
        yamlByCombo.values().forEach(doc::append);

        Path out = Path.of(resourcesDir, testCase.workload.goldenResourcePath(testCase.queryId));
        try {
            Files.createDirectories(out.getParent());
            Files.writeString(out, doc.toString());
            logger.info("plan-shape wrote golden {} -> {}", testCase, out);
        } catch (IOException e) {
            throw new UncheckedIOException("failed writing golden " + out, e);
        }
    }

    /** "workload/queryId" -> (comboName -> that combo's rendered YAML), accumulated across a generate run. */
    private static final Map<String, Map<String, String>> GENERATED_YAML_BY_QUERY = new ConcurrentHashMap<>();

    /** Assert one captured plan-shape layer against its expected text (present text, or empty = expected absent). */
    private void assertPlanShapeLayer(Optional<String> expected, Optional<String> actual, String layerLabel) {
        String label = testCase + " " + layerLabel;

        if (expected.isEmpty()) {
            assertTrue(
                String.format(Locale.ROOT, "%s — expected layer ABSENT but plan was produced:\n%s", label, actual.orElse("")),
                actual.isEmpty()
            );
            return;
        }
        assertTrue(
            String.format(Locale.ROOT, "%s — expected a plan but layer was ABSENT in the response", label),
            actual.isPresent()
        );
        // Compare trailing-whitespace-insensitively: a YAML block scalar (|) keeps one trailing
        // newline the captured plan string doesn't have. Plan shape is what matters, not edge newlines.
        assertEquals(
            String.format(Locale.ROOT, "%s — plan shape mismatch.\n=== ACTUAL (paste into golden) ===\n%s\n=== END ===", label, actual.get()),
            expected.get().stripTrailing(),
            actual.get().stripTrailing()
        );
    }

    private Map<String, Object> executePplWithProfile(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        return assertOkAndParse(client().performRequest(request), "PPL(profile): " + ppl);
    }

    private void applyClusterSettings(SettingsCombo combo) throws IOException {
        if (combo.clusterSettings().isEmpty()) {
            return;
        }
        StringBuilder settingsJson = new StringBuilder("{\"transient\":{");
        boolean first = true;
        for (Map.Entry<String, Object> setting : combo.clusterSettings().entrySet()) {
            if (!first) {
                settingsJson.append(',');
            }
            first = false;
            settingsJson.append('"').append(setting.getKey()).append("\":").append(toJsonValue(setting.getValue()));
        }
        settingsJson.append("}}");
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(settingsJson.toString());
        assertOkAndParse(client().performRequest(request), "PUT settings: " + settingsJson);
    }

    private static String toJsonValue(Object v) {
        return v instanceof String ? "\"" + v + "\"" : String.valueOf(v);
    }

    /**
     * One index per (shard count, segment layout) cell, so all variants coexist on one cluster,
     * e.g. {@code parquet_hits_2s_1seg} / {@code parquet_hits_2s_nseg}.
     */
    private static String indexNameFor(WorkloadSpec spec, int numberOfShards, SegmentLayout layout) {
        return spec.dataset().indexName + "_" + numberOfShards + "s_" + layout.suffix;
    }

    private void provisionOnce(WorkloadSpec spec, int numberOfShards, SegmentLayout layout) throws IOException {
        String indexName = indexNameFor(spec, numberOfShards, layout);
        if (!PROVISIONED_INDICES.add(indexName)) {
            return; // already provisioned this JVM
        }
        Dataset index = new Dataset(spec.dataset().name, indexName);
        DatasetProvisioner.provision(client(), index, numberOfShards, layout);
    }

    /** Indices already provisioned this JVM — each (shard count, segment layout) index is provisioned once. */
    private static final Set<String> PROVISIONED_INDICES = ConcurrentHashMap.newKeySet();
}
