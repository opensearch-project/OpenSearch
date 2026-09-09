/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for the PPL {@code grok} command on the
 * analytics-engine route.
 *
 * <p>Mirrors {@code CalcitePPLGrokIT} from the {@code opensearch-project/sql}
 * repository so the analytics-engine path can be verified inside core without a
 * cross-plugin dependency on the SQL plugin. Each test sends a PPL query through
 * {@code POST /_plugins/_ppl} (exposed by the {@code opensearch-sql} plugin),
 * which runs the {@code UnifiedQueryPlanner} → {@code CalciteRelNodeVisitor} →
 * Substrait → DataFusion pipeline.
 *
 * <p>{@code grok <field> '<grok-pattern>'} is the grok-syntax sibling of
 * {@code parse}: it lowers to one {@code ITEM(GROK(field, pattern, "grok"), "<field>")}
 * call per named group. The {@code grok} Rust UDF carries the default grok pattern
 * dictionary, recursively resolves {@code %{NAME:field}} references, and matches
 * with {@code fancy-regex} (the resolved patterns use lookbehind / atomic groups
 * the RE2-based {@code regex} crate behind {@code parse} cannot compile). Matching
 * is unanchored, mirroring Java grok's {@code Matcher.find()}.
 *
 * <p>The data is a small index whose rows reproduce the {@code bank} (email,
 * address) and {@code weblogs} (apache log line) values asserted by
 * {@code CalcitePPLGrokIT}, sorted by {@code id} for deterministic ordering.
 */
public class GrokCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("grok", "grok");

    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the grok dataset on first invocation. Mirrors
     * {@link ParseCommandIT}'s pattern — {@code client()} is unavailable at static init.
     */
    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── HOSTNAME extraction (RE2-clean pattern) ────────────────────────────────

    public void testGrokEmailHost() throws IOException {
        // ".+@%{HOSTNAME:host}" — HOSTNAME resolves to a lookaround-free regex, but
        // the synthetic-group → field remapping (name0 → host) still requires the
        // grok-aware UDF rather than the raw `parse` path.
        assertRows(
            "source=" + DATASET.indexName
                + " | sort id | grok email '.+@%{HOSTNAME:host}' | fields email, host",
            row("amberduke@pyrami.com", "pyrami.com"),
            row("hattiebond@netagy.com", "netagy.com"),
            row("nanettebates@quility.com", "quility.com")
        );
    }

    // ── NUMBER + GREEDYDATA (lookbehind + atomic group) ────────────────────────

    public void testGrokAddressOverriding() throws IOException {
        // "%{NUMBER} %{GREEDYDATA:address}" — NUMBER → BASE10NUM uses a negative
        // lookbehind `(?<![0-9.+-])` and an atomic group `(?>…)`, both rejected by
        // the RE2-based `regex` crate. The grok field name `address` overrides the
        // source `address` column.
        assertRows(
            "source=" + DATASET.indexName
                + " | sort id | grok address '%{NUMBER} %{GREEDYDATA:address}' | fields address",
            row("Holmes Lane"),
            row("Bristol Street"),
            row("Madison Street")
        );
    }

    // ── COMMONAPACHELOG (lookbehind, lookahead, atomic groups, recursion) ──────

    public void testGrokApacheLog() throws IOException {
        // "%{COMMONAPACHELOG}" recursively expands IPORHOST / HTTPDATE / NUMBER /
        // etc. into a large regex riddled with lookarounds and atomic groups.
        assertRows(
            "source=" + DATASET.indexName
                + " | sort id | grok log '%{COMMONAPACHELOG}' | fields timestamp, response",
            row("28/Sep/2022:10:15:57 -0700", "404"),
            row("28/Sep/2022:10:15:57 -0700", "100"),
            row("28/Sep/2022:10:15:57 -0700", "301")
        );
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Send a PPL query and assert each returned row equals the expected positional row.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals(
                "Column count mismatch at row " + i + " for query: " + ppl,
                want.size(),
                got.size()
            );
            for (int j = 0; j < want.size(); j++) {
                assertEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
            }
        }
    }
}
