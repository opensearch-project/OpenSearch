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
 * topologies (see {@link SegmentVariant}), renders it into the {@link PlanShapeLayer}s, and asserts
 * each against the query's golden. The non-shard layers are segment-independent (asserted once); the
 * shard physical plan is asserted per segment variant.
 *
 * <p>Per-workload subclasses supply a {@link WorkloadSpec} via a static {@code @ParametersFactory}
 * that calls {@link #enumerate(WorkloadSpec)} (the factory must be static, so it cannot read an
 * instance field).
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
     * Per-shard parquet segment topology — a controlled axis (see core-axioms): the shard physical
     * plan can legitimately differ with segment count (e.g. a TopK's runtime dynamic filter), so
     * every combo is captured under BOTH a single-segment and a multi-segment index. The non-shard
     * layers (post_cbo / fragment / coord) are NOT segment-derived and captured once (from 1seg).
     */
    enum SegmentVariant {
        SINGLE("1seg", SegmentLayout.SINGLE_SEGMENT),
        MULTI("nseg", SegmentLayout.MULTI_SEGMENT);

        final String suffix;
        final SegmentLayout layout;

        SegmentVariant(String suffix, SegmentLayout layout) {
            this.suffix = suffix;
            this.layout = layout;
        }
    }

    /** YAML key for the deduped shard physical plan (1seg == nseg). */
    private static final String SHARD_KEY = PlanShapeLayer.SHARD_PHYSICAL.yamlKey();
    /** YAML keys for the per-segment-topology shard physical plans (used only when they diverge). */
    private static String shardKey(SegmentVariant v) {
        return SHARD_KEY + "_" + v.suffix;
    }

    private final GoldenCase testCase;

    protected PlanShapeGoldenTestBase(GoldenCase testCase) {
        this.testCase = testCase;
    }

    /**
     * Build the (query x applicable-combo) cases for a workload. Each query's golden declares which
     * combos it applies to; restricted to the combos this run is exercising (defaults, or
     * {@code -Dplan.combos=...}). Static — invoked from each subclass's {@code @ParametersFactory}.
     */
    protected static List<Object[]> enumerate(WorkloadSpec spec) {
        try {
            ClassLoader cl = PlanShapeGoldenTestBase.class.getClassLoader();
            SettingsComboRegistry combos = SettingsComboRegistry.load(cl, COMBOS_RESOURCE);
            List<String> runCombos = requestedCombos(combos);
            // When generating, a golden may not exist yet — enumerate every run-combo per query.
            // When asserting, only the combos the golden declares it applies to (intersected).
            boolean generating = System.getProperty("plan.generate") != null;

            List<Object[]> cases = new ArrayList<>();
            for (String queryId : spec.queryIds()) {
                List<String> queryCombos = generating
                    ? runCombos
                    : ExpectedQueryPlanLoader.load(cl, spec.goldenResourcePath(queryId), spec.queryDir())
                        .appliesCombos().stream().filter(runCombos::contains).toList();
                for (String comboName : queryCombos) {
                    cases.add(new Object[] { new GoldenCase(spec, queryId, comboName) });
                }
            }
            return cases;
        } catch (IOException e) {
            throw new RuntimeException("failed enumerating plan-shape cases for workload " + spec.name(), e);
        }
    }

    /** Combos to run this invocation: {@code -Dplan.combos=a,b} or {@code all}, else the file defaults. */
    private static List<String> requestedCombos(SettingsComboRegistry combos) {
        String prop = System.getProperty("plan.combos");
        if (prop == null || prop.isBlank()) {
            return combos.defaults();
        }
        if (prop.trim().equals("all")) {
            return combos.allNames();
        }
        return List.of(prop.trim().split("\\s*,\\s*"));
    }

    // ------------------------------------------------------------------ the test

    public void testPlanShape() throws Exception {
        // FIXME [RemoveBeforeMerge]: verbose bring-up diagnostics throughout the test.
        logger.info("PLAN-SHAPE DIAG: ===== START case {} =====", testCase);
        ClassLoader cl = getClass().getClassLoader();
        SettingsComboRegistry combos = SettingsComboRegistry.load(cl, COMBOS_RESOURCE);
        SettingsCombo combo = combos.byName(testCase.comboName);
        boolean generating = System.getProperty("plan.generate") != null;
        // Assertion mode needs the golden; generate mode does not (it's creating it) and reads the
        // query text straight from the workload's query file.
        ExpectedQueryPlan golden = generating
            ? null
            : ExpectedQueryPlanLoader.load(cl, testCase.workload.goldenResourcePath(testCase.queryId), testCase.workload.queryDir());
        String queryText = generating
            ? loadResource(testCase.workload.queryDir() + "/" + testCase.queryId + ".ppl").strip()
            : golden.queryText();
        logger.info("PLAN-SHAPE DIAG: combo={} shards={} clusterSettings={}",
            combo.name(), combo.numberOfShards(), combo.clusterSettings());

        applyClusterSettings(combo);

        // Capture under both segment topologies. The non-shard layers come from SINGLE (they are
        // not segment-derived); SHARD_PHYSICAL is captured per variant and keyed by topology.
        Map<SegmentVariant, ProfilePlanExtractor> captured = new LinkedHashMap<>();
        for (SegmentVariant variant : SegmentVariant.values()) {
            provisionOnce(testCase.workload, combo.numberOfShards(), variant);
            String indexName = indexNameFor(testCase.workload, combo.numberOfShards(), variant);
            String ppl = queryText.replace(testCase.workload.dataset().name, indexName);
            logger.info("PLAN-SHAPE DIAG: variant={} indexName={} ppl=[{}]", variant.suffix, indexName, ppl);
            Map<String, Object> response = executePplWithProfile(ppl);
            logger.info("PLAN-SHAPE DIAG: variant={} response top-keys={} datarows={} hasError={}",
                variant.suffix, response.keySet(),
                response.get("datarows") == null ? "null" : ((List<?>) response.get("datarows")).size(),
                response.containsKey("error"));
            captured.put(variant, ProfilePlanExtractor.from(response, indexName));
        }

        // -Dplan.generate=write|print : capture instead of assert. write -> the q{N}.plan.yaml in
        // the source tree; print -> log the block. Absent -> assertion mode.
        String generate = System.getProperty("plan.generate");
        if (generate != null) {
            String comboYaml = renderComboYaml(captured);
            if ("print".equals(generate)) {
                logger.info("\n{}{}\n{}", GENERATED_BANNER, testCase, comboYaml);
            } else {
                writeGolden(comboYaml);
            }
            return;
        }

        // Non-shard layers: assert once (from the SINGLE capture — segment-independent).
        ProfilePlanExtractor single = captured.get(SegmentVariant.SINGLE);
        for (PlanShapeLayer layer : PlanShapeLayer.values()) {
            if (layer == PlanShapeLayer.SHARD_PHYSICAL) {
                continue;
            }
            assertLayer(golden.expected(testCase.comboName, layer), single.layer(layer), layer.toString());
        }
        assertShardPhysical(golden, captured);
    }

    /**
     * Assert the shard physical plan for both segment variants. A golden stores either a single
     * deduped {@code shard_physical} (when 1seg and Nseg are identical) or both
     * {@code shard_physical_1seg} / {@code shard_physical_nseg} (when they diverge). Resolve per
     * variant: prefer the variant-specific key, else fall back to the deduped key.
     */
    private void assertShardPhysical(ExpectedQueryPlan golden, Map<SegmentVariant, ProfilePlanExtractor> captured) {
        for (SegmentVariant variant : SegmentVariant.values()) {
            Optional<String> actual = captured.get(variant).layer(PlanShapeLayer.SHARD_PHYSICAL);
            Optional<String> expected = golden.expectedRaw(testCase.comboName, shardKey(variant));
            if (expected.isEmpty()) {
                expected = golden.expectedRaw(testCase.comboName, SHARD_KEY); // deduped
            }
            assertLayer(expected, actual, PlanShapeLayer.SHARD_PHYSICAL + "[" + variant.suffix + "]");
        }
    }

    private static final String GENERATED_BANNER = ">>>>> PLANSHAPE-GENERATED ";

    /**
     * Render this (query, combo)'s captured layers as the YAML body that sits under {@code plans:}.
     * Non-shard layers come from the SINGLE capture (segment-independent). SHARD_PHYSICAL is
     * deduped: if 1seg and Nseg are identical, emit one {@code shard_physical}; if they diverge,
     * emit both {@code shard_physical_1seg} and {@code shard_physical_nseg}.
     */
    private String renderComboYaml(Map<SegmentVariant, ProfilePlanExtractor> captured) {
        ProfilePlanExtractor single = captured.get(SegmentVariant.SINGLE);
        StringBuilder body = new StringBuilder();
        body.append("  ").append(testCase.comboName).append(":\n");
        for (PlanShapeLayer layer : PlanShapeLayer.values()) {
            if (layer == PlanShapeLayer.SHARD_PHYSICAL) {
                appendShardPhysical(body, captured);
                continue;
            }
            single.layer(layer).ifPresent(text -> appendBlock(body, layer.yamlKey(), text));
        }
        return body.toString();
    }

    /** Emit the shard physical plan(s): one deduped key if the variants match, else both. */
    private void appendShardPhysical(StringBuilder body, Map<SegmentVariant, ProfilePlanExtractor> captured) {
        Optional<String> oneSeg = captured.get(SegmentVariant.SINGLE).layer(PlanShapeLayer.SHARD_PHYSICAL);
        Optional<String> nSeg = captured.get(SegmentVariant.MULTI).layer(PlanShapeLayer.SHARD_PHYSICAL);
        if (oneSeg.isEmpty() && nSeg.isEmpty()) {
            logger.info("PLAN-SHAPE DIAG: {} shard_physical ABSENT for both variants -> omit", testCase);
            return; // no shard physical (Lucene fast-path): omit, asserted absent for both
        }
        if (oneSeg.equals(nSeg)) {
            logger.info("PLAN-SHAPE DIAG: {} shard_physical IDENTICAL across 1seg/nseg -> dedup to one key", testCase);
            appendBlock(body, SHARD_KEY, oneSeg.get()); // identical across topologies -> dedup
            return;
        }
        logger.info("PLAN-SHAPE DIAG: {} shard_physical DIVERGES 1seg vs nseg -> writing both keys", testCase);
        logShardDiff(oneSeg.orElse(""), nSeg.orElse(""));
        // Diverge (e.g. TopK runtime dynamic filter on multi-segment): pin each topology.
        oneSeg.ifPresent(t -> appendBlock(body, shardKey(SegmentVariant.SINGLE), t));
        nSeg.ifPresent(t -> appendBlock(body, shardKey(SegmentVariant.MULTI), t));
    }

    /** FIXME [RemoveBeforeMerge]: line-by-line diff of the 1seg vs nseg shard physical plans. */
    private void logShardDiff(String oneSeg, String nSeg) {
        String[] a = oneSeg.split("\n", -1);
        String[] b = nSeg.split("\n", -1);
        for (int i = 0; i < Math.max(a.length, b.length); i++) {
            String la = i < a.length ? a[i] : "<none>";
            String lb = i < b.length ? b[i] : "<none>";
            if (!la.equals(lb)) {
                logger.info("PLAN-SHAPE DIAG: shard_physical diff @line {}:\n  1seg: {}\n  nseg: {}", i, la, lb);
            }
        }
    }

    private static void appendBlock(StringBuilder body, String yamlKey, String text) {
        body.append("    ").append(yamlKey).append(": |\n");
        for (String line : text.split("\n")) {
            body.append("      ").append(line).append('\n');
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
        Map<String, String> byCombo = GENERATED.computeIfAbsent(testCase.workload.name() + "/" + testCase.queryId, k -> new LinkedHashMap<>());
        byCombo.put(testCase.comboName, comboYaml);

        StringBuilder doc = new StringBuilder();
        doc.append("# Generated via -Dplan.generate=write, then human-reviewed.\n");
        doc.append("query: ").append(testCase.queryId).append('\n');
        doc.append("ppl_file: ").append(testCase.queryId).append(".ppl\n");
        doc.append("applies: [").append(String.join(", ", byCombo.keySet())).append("]\n");
        doc.append("plans:\n");
        byCombo.values().forEach(doc::append);

        Path out = Path.of(resourcesDir, testCase.workload.goldenResourcePath(testCase.queryId));
        try {
            Files.createDirectories(out.getParent());
            Files.writeString(out, doc.toString());
            logger.info("{}{} -> wrote {}", GENERATED_BANNER, testCase, out);
        } catch (IOException e) {
            throw new UncheckedIOException("failed writing golden " + out, e);
        }
    }

    /** queryKey -> (comboName -> rendered combo YAML), accumulated across cases in a generate run. */
    private static final Map<String, Map<String, String>> GENERATED = new ConcurrentHashMap<>();

    /** Assert one captured layer against its expected text (present text, or empty = expected absent). */
    private void assertLayer(Optional<String> expected, Optional<String> actual, String layerLabel) {
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

    // ------------------------------------------------------------------ cluster interaction

    private Map<String, Object> executePplWithProfile(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\", \"profile\": true}");
        return assertOkAndParse(client().performRequest(request), "PPL(profile): " + ppl);
    }

    private void applyClusterSettings(SettingsCombo combo) throws IOException {
        if (combo.clusterSettings().isEmpty()) {
            logger.info("PLAN-SHAPE DIAG: combo {} has no cluster settings to apply", combo.name());
            return;
        }
        StringBuilder body = new StringBuilder("{\"transient\":{");
        boolean first = true;
        for (Map.Entry<String, Object> e : combo.clusterSettings().entrySet()) {
            if (!first) body.append(',');
            first = false;
            body.append('"').append(e.getKey()).append("\":").append(toJsonValue(e.getValue()));
        }
        body.append("}}");
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity(body.toString());
        // FIXME [RemoveBeforeMerge]: log the settings body + response.
        logger.info("PLAN-SHAPE DIAG: applying cluster settings: {}", body);
        Map<String, Object> resp = assertOkAndParse(client().performRequest(req), "PUT settings: " + body);
        logger.info("PLAN-SHAPE DIAG: cluster settings applied, ack={}", resp.get("acknowledged"));
    }

    private static String toJsonValue(Object v) {
        return v instanceof String ? "\"" + v + "\"" : String.valueOf(v);
    }

    // ------------------------------------------------------------------ provisioning (once per shard count)

    /**
     * One index per (shard count, segment topology) cell, so all variants coexist on one cluster,
     * e.g. {@code parquet_hits_2s_1seg} / {@code parquet_hits_2s_nseg}.
     */
    private static String indexNameFor(WorkloadSpec spec, int numberOfShards, SegmentVariant variant) {
        return spec.dataset().indexName + "_" + numberOfShards + "s_" + variant.suffix;
    }

    private void provisionOnce(WorkloadSpec spec, int numberOfShards, SegmentVariant variant) throws IOException {
        String indexName = indexNameFor(spec, numberOfShards, variant);
        if (PROVISIONED_INDICES.contains(indexName)) {
            logger.info("PLAN-SHAPE DIAG: index {} already provisioned, skipping", indexName);
            return;
        }
        logger.info("PLAN-SHAPE DIAG: provisioning index {} at {} shard(s) topology={} from dataset {}",
            indexName, numberOfShards, variant.suffix, spec.dataset().name);
        Dataset index = new Dataset(spec.dataset().name, indexName);
        DatasetProvisioner.provision(client(), index, numberOfShards, variant.layout);
        PROVISIONED_INDICES.add(indexName);
        // FIXME [RemoveBeforeMerge]: confirm the segment topology axis actually took effect —
        // 1seg must be exactly 1 segment per shard, nseg must be >=2. If these don't hold, the
        // shard-physical variants are meaningless, so surface it loudly here.
        logSegmentCounts(indexName, variant);
        logger.info("PLAN-SHAPE DIAG: provisioned {} OK", indexName);
    }

    /** FIXME [RemoveBeforeMerge]: log the realized per-shard segment count for a provisioned index. */
    @SuppressWarnings("unchecked")
    private void logSegmentCounts(String indexName, SegmentVariant variant) {
        try {
            Map<String, Object> resp = assertOkAndParse(
                client().performRequest(new Request("GET", "/" + indexName + "/_segments")), "_segments " + indexName);
            Map<String, Object> indices = (Map<String, Object>) resp.get("indices");
            Map<String, Object> idx = (Map<String, Object>) indices.get(indexName);
            Map<String, Object> shards = (Map<String, Object>) idx.get("shards");
            shards.forEach((shardId, replicas) -> {
                for (Object replica : (List<Object>) replicas) {
                    Map<String, Object> segs = (Map<String, Object>) ((Map<String, Object>) replica).get("segments");
                    int count = segs == null ? 0 : segs.size();
                    int expected = variant == SegmentVariant.SINGLE ? 1 : DatasetProvisioner.MULTI_SEGMENT_COUNT;
                    String warn = count != expected ? " <-- UNEXPECTED (want " + expected + ") for " + variant.suffix : "";
                    logger.info("PLAN-SHAPE DIAG: index {} [{}] shard {} -> {} segment(s){}",
                        indexName, variant.suffix, shardId, count, warn);
                }
            });
        } catch (Exception e) {
            logger.warn("PLAN-SHAPE DIAG: could not read segments for {}: {}", indexName, e.toString());
        }
    }

    /** Indices already provisioned this JVM — each shard-count index is provisioned once. */
    private static final Set<String> PROVISIONED_INDICES = ConcurrentHashMap.newKeySet();
}
