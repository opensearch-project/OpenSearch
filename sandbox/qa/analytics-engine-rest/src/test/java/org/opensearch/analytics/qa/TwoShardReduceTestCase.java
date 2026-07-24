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
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Shared machinery for the 2-shard reduce-correctness suite — provisioning, query discovery, the
 * differential / approximate runners, and reporting. Subclasses declare only their dataset tiers
 * ({@link #tiers()}) and known-broken queries ({@link #knownIssues()}).
 *
 * <p>Each {@code <name>.ppl} (source = {@code %INDEX%}) runs against {@code merge_coverage} at 1 shard
 * (oracle) and 2 shards. Exact tier: the two results must be equal (unordered, numeric-tolerant,
 * multi-value cells as multisets); a {@code <dir>/expected/<name>.json} golden additionally pins the
 * 2-shard result. Approximate tier: sketches drift across a merge, so the 2-shard result is asserted
 * within tolerance of a required golden instead. See {@code datasets/merge_coverage/README.md}.
 */
public abstract class TwoShardReduceTestCase extends AnalyticsRestTestCase {

    protected static final String DATASET_NAME = "merge_coverage";
    protected static final String INDEX_1SHARD = "merge_coverage_1shard";
    protected static final String INDEX_2SHARD = "merge_coverage_2shard";
    protected static final String INDEX_TOKEN = "%INDEX%";

    /** Relative tolerance for the approximate (HLL / t-digest) tier. */
    private static final double APPROX_REL_TOL = 0.15;
    private static final double APPROX_ABS_TOL = 1.0;

    private static final Dataset BASELINE = new Dataset(DATASET_NAME, INDEX_1SHARD);
    private static final Dataset SHARDED = new Dataset(DATASET_NAME, INDEX_2SHARD);

    private static boolean provisioned = false;

    // ── subclass contract ─────────────────────────────────────────────────────────

    /** Dataset subdirectories this suite covers, mapped to {@code true} when the tier is approximate
     *  (golden-with-tolerance) rather than exact (differential). Use an ordered map for stable logs. */
    protected abstract Map<String, Boolean> tiers();

    /**
     * The single registry of muted queries: {@code <name>.ppl} base-name → reason. Listed queries are
     * <b>skipped entirely</b> (never executed) and logged as {@code MUTED: <reason>}; everything else
     * runs and must pass. This is the one place to look for "what's currently broken at 2 shards" —
     * there is no separate annotation or directory. Skipping (rather than running-and-tolerating) is
     * required because some entries crash the data node, which would break unrelated tests.
     */
    protected Map<String, String> knownIssues() {
        return Collections.emptyMap();
    }

    /**
     * Whether to force the MPP distributed path on every query. Default {@code false} — the suite verifies
     * the coordinator-centric multi-shard reduce. A subclass returning {@code true} (with the tiny
     * {@code analytics.mpp.distribute.min_rows} floor) drives the same query families through the
     * distribution-enforcement pass + reduce-stage substrait emit, guarding the MPP-reduce shapes
     * (percentile literal, HLL partial-state typing, computed-column type drift).
     */
    protected boolean forceMpp() {
        return false;
    }

    // ── provisioning ────────────────────────────────────────────────────────────

    @Override
    protected void onBeforeQuery() throws IOException {
        if (forceMpp()) {
            Request mpp = new Request("PUT", "/_cluster/settings");
            mpp.setJsonEntity("{\"transient\":{\"analytics.mpp.enabled\": true, \"analytics.mpp.distribute.min_rows\": 1}}");
            client().performRequest(mpp);
        }
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), BASELINE, 1);
            DatasetProvisioner.provision(client(), SHARDED, 2);
            // Dimension index for the `lookup` command (shard count irrelevant — it's broadcast).
            DatasetProvisioner.provision(client(), new Dataset("merge_coverage_lookup", "merge_coverage_lookup"), 1);
            assertShardsPopulated(INDEX_2SHARD, 2);
            provisioned = true;
        }
    }

    @Override
    public void tearDown() throws Exception {
        // Reset the cluster-wide MPP transient a forceMpp() subclass applied, so it does not leak onto
        // sibling test classes on the shared cluster (see TpchPplIT for the same hygiene).
        if (forceMpp()) {
            Request reset = new Request("PUT", "/_cluster/settings");
            reset.setJsonEntity("{\"transient\":{\"analytics.mpp.enabled\": null, \"analytics.mpp.distribute.min_rows\": null}}");
            client().performRequest(reset);
        }
        super.tearDown();
    }

    // ── the test ──────────────────────────────────────────────────────────────────

    /**
     * Runs every {@code <name>.ppl} in this subclass's {@link #tiers()} against both the 1-shard and
     * 2-shard index and asserts the reduce did not change the answer. Collects all failures and
     * reports them together so one broken query doesn't hide the rest.
     */
    public void testReduceCorrectnessAcrossTwoShards() throws IOException {
        Map<String, String> muted = knownIssues();
        List<String> failures = new ArrayList<>();
        int passed = 0;
        int skipped = 0;
        int total = 0;

        for (Map.Entry<String, Boolean> tier : tiers().entrySet()) {
            String dir = tier.getKey();
            boolean approx = tier.getValue();
            for (String name : discoverQueryNames("datasets/" + DATASET_NAME + "/" + dir)) {
                total++;
                if (muted.containsKey(name)) {
                    skipped++;
                    logger.warn("[2-shard reduce] MUTED {}/{} — {}", dir, name, muted.get(name));
                    continue;
                }
                String err = approx ? runApprox(dir, name) : runDifferential(dir, name);
                if (err == null) {
                    passed++;
                } else {
                    failures.add(dir + "/" + err);
                }
            }
        }
        assertTrue("No queries discovered for tiers " + tiers().keySet() + " — dataset resources missing?", total > 0);

        logger.info("[2-shard reduce] {} passed, {} failed, {} muted (of {} across tiers {})",
            passed, failures.size(), skipped, total, tiers().keySet());
        if (failures.isEmpty() == false) {
            StringBuilder sb = new StringBuilder();
            sb.append(failures.size()).append(" of ").append(total).append(" 2-shard reduce checks failed:\n");
            for (String f : failures) {
                sb.append("  - ").append(f).append('\n');
            }
            fail(sb.toString());
        }
    }

    // ── per-query runners ────────────────────────────────────────────────────────

    /** Exact tier: 1-shard result must equal 2-shard result; if a golden exists, pin the 2-shard
     *  result to it too. Returns null on pass, else a one-line failure. */
    private String runDifferential(String dir, String name) {
        String queryDir = "datasets/" + DATASET_NAME + "/" + dir;
        try {
            String template = DatasetProvisioner.loadResource(queryDir + "/" + name + ".ppl").trim();
            Map<String, Object> r1 = executePpl(template.replace(INDEX_TOKEN, INDEX_1SHARD));
            Map<String, Object> r2 = executePpl(template.replace(INDEX_TOKEN, INDEX_2SHARD));
            logValues(dir, name, r1, r2);

            // Collection aggregations (values/list) return a multi-value cell whose element order
            // depends on the gather; sort those array cells so the differential tests the merged
            // multiset, not the gather order.
            normalizeArrayCells(r1);
            normalizeArrayCells(r2);

            String diff = ResponseValidator.compareData(r1, r2, name + " [1-shard vs 2-shard]");
            if (diff != null) {
                return diff;
            }
            Map<String, Object> golden = loadGolden(queryDir + "/expected/" + name + ".json");
            if (golden != null) {
                normalizeArrayCells(golden);
                // Pin BOTH shard counts to the golden. The differential above already proves
                // r1 == r2, so 2-shard-vs-golden alone would catch a regression transitively — but
                // pinning 1-shard directly guards the baseline even if the differential is ever
                // relaxed, and a baseline-vs-coordinator regression surfaces on the right side.
                String pin1 = ResponseValidator.compareData(golden, r1, name + " [1-shard vs golden]");
                if (pin1 != null) {
                    return pin1;
                }
                String pin2 = ResponseValidator.compareData(golden, r2, name + " [2-shard vs golden]");
                if (pin2 != null) {
                    return pin2;
                }
            }
            return null;
        } catch (Exception e) {
            logger.info("[2shard-val] {}/{} | THREW {}", dir, name, rootMessage(e));
            return name + " [" + dir + "] threw: " + rootMessage(e);
        }
    }

    /** Approx tier: differential equality is not used (sketches drift across a merge) — the 2-shard
     *  result is asserted within tolerance of a REQUIRED golden truth. Returns null on pass. */
    private String runApprox(String dir, String name) {
        String queryDir = "datasets/" + DATASET_NAME + "/" + dir;
        try {
            Map<String, Object> golden = loadGolden(queryDir + "/expected/" + name + ".json");
            if (golden == null) {
                return name + " [" + dir + "] has no golden truth file — approximate queries must ship one";
            }
            String template = DatasetProvisioner.loadResource(queryDir + "/" + name + ".ppl").trim();
            Map<String, Object> r2 = executePpl(template.replace(INDEX_TOKEN, INDEX_2SHARD));
            logger.info("[2shard-val] {}/{} | 2s={} | golden={}", dir, name,
                compact(extractRows(r2)), compact(extractRows(golden)));
            return compareWithinTolerance(extractRows(golden), extractRows(r2), name + " [2-shard vs truth ~]");
        } catch (Exception e) {
            return name + " [" + dir + "] threw: " + rootMessage(e);
        }
    }

    // ── tolerance comparison (approximate tier) ──────────────────────────────────

    /**
     * Unordered compare where numeric cells need only match within a relative tolerance and
     * non-numeric cells (group keys) must match exactly. Rows are aligned by their non-numeric cells.
     */
    private static String compareWithinTolerance(List<List<Object>> expected, List<List<Object>> actual, String label) {
        if (expected == null || actual == null) {
            return expected == actual ? null : label + ": one side empty";
        }
        if (expected.size() != actual.size()) {
            return String.format(Locale.ROOT, "%s: row count mismatch - expected %d, got %d", label, expected.size(), actual.size());
        }
        List<List<Object>> e = new ArrayList<>(expected);
        List<List<Object>> a = new ArrayList<>(actual);
        e.sort(TwoShardReduceTestCase::byGroupKeys);
        a.sort(TwoShardReduceTestCase::byGroupKeys);
        for (int i = 0; i < e.size(); i++) {
            List<Object> er = e.get(i);
            List<Object> ar = a.get(i);
            if (er.size() != ar.size()) {
                return String.format(Locale.ROOT, "%s row %d: column count mismatch", label, i);
            }
            for (int j = 0; j < er.size(); j++) {
                Object ev = er.get(j);
                Object av = ar.get(j);
                if (ev instanceof Number && av instanceof Number) {
                    double ed = ((Number) ev).doubleValue();
                    double ad = ((Number) av).doubleValue();
                    double tol = Math.max(APPROX_ABS_TOL, APPROX_REL_TOL * Math.abs(ed));
                    if (Math.abs(ed - ad) > tol) {
                        return String.format(Locale.ROOT, "%s row %d col %d: %s vs %s exceeds tolerance %.3f",
                            label, i, j, ev, av, tol);
                    }
                } else if (java.util.Objects.equals(ev, av) == false) {
                    return String.format(Locale.ROOT, "%s row %d col %d: %s vs %s", label, i, j, ev, av);
                }
            }
        }
        return null;
    }

    /** Order rows by their non-numeric cells (the group keys) so tolerant compare can align them. */
    private static int byGroupKeys(List<Object> r1, List<Object> r2) {
        StringBuilder k1 = new StringBuilder();
        StringBuilder k2 = new StringBuilder();
        for (Object o : r1) {
            if ((o instanceof Number) == false) {
                k1.append(o).append(' ');
            }
        }
        for (Object o : r2) {
            if ((o instanceof Number) == false) {
                k2.append(o).append(' ');
            }
        }
        return k1.toString().compareTo(k2.toString());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static List<List<Object>> extractRows(Map<String, Object> response) {
        if (response == null) {
            return null;
        }
        if (response.containsKey("datarows")) {
            return (List<List<Object>>) response.get("datarows");
        }
        if (response.containsKey("rows")) {
            return (List<List<Object>>) response.get("rows");
        }
        return null;
    }

    /** Load a golden {@code {"rows": [...]}} file as a Map, or null if it doesn't exist. */
    private static Map<String, Object> loadGolden(String resourcePath) throws IOException {
        if (TwoShardReduceTestCase.class.getClassLoader().getResource(resourcePath) == null) {
            return null;
        }
        String json = DatasetProvisioner.loadResource(resourcePath);
        return XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
    }

    /** Log compact 1-shard and 2-shard result rows for the report (parsed out of the log). */
    private void logValues(String tier, String name, Map<String, Object> r1, Map<String, Object> r2) {
        logger.info("[2shard-val] {}/{} | 1s={} | 2s={}", tier, name, compact(extractRows(r1)), compact(extractRows(r2)));
    }

    /** Sort any list-valued (multi-value / collection-aggregation) cell in place, so a differential
     *  compare treats {@code values(...)} / {@code list(...)} output as an unordered multiset. */
    @SuppressWarnings("unchecked")
    private static void normalizeArrayCells(Map<String, Object> response) {
        List<List<Object>> rows = extractRows(response);
        if (rows == null) {
            return;
        }
        for (List<Object> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                if (row.get(i) instanceof List) {
                    List<Object> cell = new ArrayList<>((List<Object>) row.get(i));
                    cell.sort(java.util.Comparator.comparing(o -> o == null ? "" : o.toString()));
                    row.set(i, cell);
                }
            }
        }
    }

    /** Render up to the first 8 rows compactly so the log line stays readable. */
    private static String compact(List<List<Object>> rows) {
        if (rows == null) {
            return "<none>";
        }
        int show = Math.min(8, rows.size());
        StringBuilder sb = new StringBuilder();
        sb.append('(').append(rows.size()).append(" rows) ");
        for (int i = 0; i < show; i++) {
            sb.append(rows.get(i));
            if (i < show - 1) {
                sb.append(',');
            }
        }
        if (rows.size() > show) {
            sb.append(",…");
        }
        return sb.toString();
    }

    private static String rootMessage(Throwable t) {
        Throwable r = t;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        return r.getClass().getSimpleName() + ": " + r.getMessage();
    }

    /** Assert the index spreads its docs across at least {@code minPopulated} primary shards. */
    private void assertShardsPopulated(String index, int minPopulated) throws IOException {
        Request req = new Request("GET", "/_cat/shards/" + index);
        req.addParameter("format", "json");
        req.addParameter("h", "prirep,docs,state");
        Response resp = client().performRequest(req);
        List<Object> shards = entityAsList(resp);
        int populated = 0;
        for (Object o : shards) {
            @SuppressWarnings("unchecked")
            Map<String, Object> shard = (Map<String, Object>) o;
            if ("p".equals(shard.get("prirep"))) {
                Object docs = shard.get("docs");
                if (docs != null && Integer.parseInt(docs.toString()) > 0) {
                    populated++;
                }
            }
        }
        assertTrue(
            "Index [" + index + "] must spread docs across >= " + minPopulated + " primary shards "
                + "(else the 2-shard reduce is never exercised); populated primaries=" + populated,
            populated >= minPopulated
        );
    }

    /** Discover {@code <name>.ppl} base names under a classpath resource directory (sorted). */
    private static List<String> discoverQueryNames(String resourceDir) throws IOException {
        URL url = TwoShardReduceTestCase.class.getClassLoader().getResource(resourceDir);
        if (url == null) {
            return Collections.emptyList();
        }
        TreeSet<String> names = new TreeSet<>();
        FileSystem fs = null;
        try {
            URI uri = url.toURI();
            Path dir;
            if ("jar".equals(uri.getScheme())) {
                fs = FileSystems.newFileSystem(uri, Collections.emptyMap());
                dir = fs.getPath(resourceDir);
            } else {
                dir = PathUtils.get(uri);
            }
            try (Stream<Path> stream = Files.list(dir)) {
                stream.forEach(p -> {
                    String fileName = p.getFileName().toString();
                    if (fileName.endsWith(".ppl")) {
                        names.add(fileName.substring(0, fileName.length() - ".ppl".length()));
                    }
                });
            }
        } catch (Exception e) {
            throw new IOException("Failed to discover queries in [" + resourceDir + "]", e);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        return new ArrayList<>(names);
    }
}
