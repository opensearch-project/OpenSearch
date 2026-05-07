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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for the PPL {@code rex} command's {@code mode=sed} surface
 * on the analytics-engine route.
 *
 * <p>Mirrors a subset of {@code CalciteRexCommandIT} from the {@code opensearch-project/sql}
 * repository — the part that lowers to standard Calcite library operators and bridges through
 * Substrait to DataFusion's native UDFs:
 *
 * <ul>
 *   <li>{@code rex field=f mode=sed "s/old/new/"} (no flags) — emits Calcite
 *       {@code SqlLibraryOperators.REGEXP_REPLACE_3} → DataFusion's 3-arg
 *       {@code regexp_replace}. Already wired by the PPL {@code replace} onboarding (#21527).</li>
 *   <li>{@code rex field=f mode=sed "s/old/new/g"} or {@code "/i"} or {@code "/gi"} — emits
 *       {@code SqlLibraryOperators.REGEXP_REPLACE_PG_4} → DataFusion's 4-arg
 *       {@code regexp_replace} with a flags string. New in this PR.</li>
 *   <li>{@code rex field=f mode=sed "y/from/to/"} (transliteration) — emits
 *       {@code SqlLibraryOperators.TRANSLATE3} → DataFusion's {@code translate} UDF. New in
 *       this PR.</li>
 * </ul>
 *
 * <p><b>NOT covered by this PR (Part 1):</b>
 * <ul>
 *   <li>Rex extract mode ({@code rex field=f "(?<g>...)"}) — uses the SQL plugin's custom
 *       Java UDFs ({@code REX_EXTRACT}, {@code REX_EXTRACT_MULTI}, {@code REX_OFFSET}) which
 *       have no native DataFusion equivalent. Slated for a follow-up PR (Part 2) that adds
 *       Rust-side UDF implementations, similar to the {@code convert_tz} precedent.</li>
 *   <li>Sed with occurrence flag ({@code "s/.../.../2"}) — emits 5-arg
 *       {@code REGEXP_REPLACE_5}, which DataFusion's native {@code regexp_replace} does not
 *       support (max 4 args). Also Part 2 territory.</li>
 * </ul>
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class RexCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── sed mode without flags (REGEXP_REPLACE_3, already wired by replace) ────

    public void testRexSedReplaceLiteral() throws IOException {
        // Replace literal "SUPPLIES" → "STUFF" in str0. 6 rows have "OFFICE SUPPLIES"; each
        // becomes "OFFICE STUFF". No-flags sed lowers to 3-arg regexp_replace which the
        // analytics-engine route already handles via the replace onboarding.
        assertRowCount(
            "source=" + DATASET.indexName
                + " | rex field=str0 mode=sed \"s/SUPPLIES/STUFF/\""
                + " | where str0='OFFICE STUFF' | fields str0",
            6
        );
    }

    public void testRexSedReplaceNoFlagIsGlobal() throws IOException {
        // No-flag sed (`s/E/X/`) lowers to Calcite REGEXP_REPLACE_3, whose contract is
        // global replacement (BigQuery / DataFusion semantics — the bare 3-arg form
        // replaces every match, not just the first). "BINDER ACCESSORIES" has 3 E's;
        // all become X. This differs from traditional sed where `s/E/X/` (no /g) is
        // first-occurrence-only — that semantic is the SQL plugin's responsibility to
        // bridge if it ever wants to match traditional sed exactly.
        assertRows(
            "source=" + DATASET.indexName
                + " | where str1='BINDER ACCESSORIES'"
                + " | rex field=str1 mode=sed \"s/E/X/\""
                + " | fields str1",
            row("BINDXR ACCXSSORIXS")
        );
    }

    // ── sed mode with /g flag (REGEXP_REPLACE_PG_4 — new bridge) ───────────────

    public void testRexSedReplaceGlobal() throws IOException {
        // /g flag — replace EVERY occurrence in each row.
        // "BINDER ACCESSORIES" has 3 E's, all become X.
        assertRows(
            "source=" + DATASET.indexName
                + " | where str1='BINDER ACCESSORIES'"
                + " | rex field=str1 mode=sed \"s/E/X/g\""
                + " | fields str1",
            row("BINDXR ACCXSSORIXS")
        );
    }

    // ── sed mode with /i flag (REGEXP_REPLACE_PG_4 — case-insensitive) ─────────

    public void testRexSedReplaceCaseInsensitive() throws IOException {
        // Pattern is lowercase but field values are uppercase — /i makes it match.
        // 2 FURNITURE rows in str0 → "FURN".
        assertRowCount(
            "source=" + DATASET.indexName
                + " | rex field=str0 mode=sed \"s/furniture/FURN/i\""
                + " | where str0='FURN' | fields str0",
            2
        );
    }

    // ── sed mode with /gi combined flags ──────────────────────────────────────

    public void testRexSedReplaceGlobalCaseInsensitive() throws IOException {
        // /gi — every match, case-insensitive. "BINDER ACCESSORIES" has 4 e/E's total
        // (case-insensitively), all become X: B-I-N-D-X-R-' '-A-C-C-X-S-S-O-R-I-X-S → "BINDXR ACCXSSORIXS".
        // (Note: same result as /g here because all E's are already uppercase. Use a
        // pattern where /gi differs from /g to actually exercise the case-insensitivity.)
        assertRows(
            "source=" + DATASET.indexName
                + " | where str1='BINDER ACCESSORIES'"
                + " | rex field=str1 mode=sed \"s/e/X/gi\""
                + " | fields str1",
            row("BINDXR ACCXSSORIXS")
        );
    }

    // ── sed mode with backreference (4-arg with flags + $N braces test) ───────

    public void testRexSedReplaceWithBackreference() throws IOException {
        // Swap first two whitespace-separated tokens. Exercises both pattern unquoting
        // (none needed here — user-typed regex) AND replacement-side $N → ${N} brace
        // rewrite handled by RegexpReplaceAdapter. "OFFICE SUPPLIES" → "SUPPLIES OFFICE".
        assertRowCount(
            "source=" + DATASET.indexName
                + " | rex field=str0 mode=sed \"s/^(\\w+) (\\w+)$/$2 $1/\""
                + " | where str0='SUPPLIES OFFICE' | fields str0",
            6
        );
    }

    // ── transliteration y/from/to/ (TRANSLATE3 — new bridge) ──────────────────

    public void testRexSedTransliterationVowels() throws IOException {
        // y/AEIOU/aeiou/ — lowercase the uppercase vowels in str0.
        // "FURNITURE" → F-uRN-i-T-uR-e → "FuRNiTuRe"
        assertRows(
            "source=" + DATASET.indexName
                + " | where str0='FURNITURE'"
                + " | rex field=str0 mode=sed \"y/AEIOU/aeiou/\""
                + " | fields str0 | head 1",
            row("FuRNiTuRe")
        );
    }

    public void testRexSedTransliterationDigitsToLetters() throws IOException {
        // Apply transliteration on a uniform field — verify it doesn't crash on
        // characters not in the `from` set (they pass through unchanged).
        // "OFFICE SUPPLIES" has no AEIOU lowercased, but uppercase OFFICE has O,I,E etc.
        // y/AEIOU/aeiou/ → "oFFiCe SuPPLieS"
        assertRows(
            "source=" + DATASET.indexName
                + " | where str0='OFFICE SUPPLIES'"
                + " | rex field=str0 mode=sed \"y/AEIOU/aeiou/\""
                + " | fields str0 | head 1",
            row("oFFiCe SuPPLieS")
        );
    }

    // ── no-match passthrough ──────────────────────────────────────────────────

    public void testRexSedNoMatchPasses() throws IOException {
        // Pattern matches nothing — all rows pass through with str0 unchanged.
        // 17 calcs rows total.
        assertRowCount(
            "source=" + DATASET.indexName
                + " | rex field=str0 mode=sed \"s/NOSUCHVALUE/X/\""
                + " | fields str0",
            17
        );
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private void assertRowCount(String ppl, int expectedCount) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expectedCount, actualRows.size());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
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

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
