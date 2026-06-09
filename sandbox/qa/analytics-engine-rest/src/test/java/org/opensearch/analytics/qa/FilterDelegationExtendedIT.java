/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Table-driven filter delegation IT for extended predicate shapes. Each case is a
 * (PPL suffix, expected value) pair — add new predicates by appending to {@link #CASES}.
 *
 * <p>Uses the {@code filter_delegation_extended} dataset (2 shards, keyword + nullable keyword
 * + integer fields) provisioned via {@link DatasetProvisioner}. The dataset is designed so
 * IS_NULL, IS_NOT_NULL, NOT_EQUALS, and future comparisons/IN/RANGE all have unambiguous
 * expected counts.
 *
 * <p>Dataset: str0 ∈ apple×3, banana×3, cherry×2, date×2 (10 docs).
 * str1 present for 6, missing for 4. num0 ∈ 10,20,...,100.
 */
public class FilterDelegationExtendedIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("filter_delegation_extended", "filter_delegation_extended");

    private record Case(String pplSuffix, long expected, boolean isRowCount) {}

    // --- NOT_EQUALS cases ---
    // str0 != 'apple' → banana×3 + cherry×2 + date×2 = 7 docs
    // str0 != 'banana' → apple×3 + cherry×2 + date×2 = 7 docs
    // str0 != 'date' → apple×3 + banana×3 + cherry×2 = 8 docs

    // --- IS_NOT_NULL cases ---
    // isnotnull(str1) → 6 docs (apple×2, banana×2, cherry×1, date×1)

    // --- IS_NULL cases ---
    // isnull(str1) → 4 docs (apple×1, banana×1, cherry×1, date×1)

    private static final List<Case> CASES = List.of(
        // ===== NOT_EQUALS with various aggregates =====
        new Case("where str0 != 'apple' | stats count() as c", 7, false),
        new Case("where str0 != 'apple' | stats sum(num0) as c", 490, false),
        new Case("where str0 != 'apple' | stats avg(num0) as c", 70, false),
        new Case("where str0 != 'apple' | stats min(num0) as c", 40, false),
        new Case("where str0 != 'apple' | stats max(num0) as c", 100, false),
        new Case("where str0 != 'apple' | stats dc(str0) as c", 3, false),
        new Case("where str0 != 'apple' | fields str0", 7, true),
        new Case("where str0 != 'date' | stats avg(num0) as c", 45, false),
        new Case("where str0 != 'date' | stats dc(str0) as c", 3, false),

        // ===== IS_NOT_NULL with various aggregates =====
        new Case("where isnotnull(str1) | stats count() as c", 6, false),
        new Case("where isnotnull(str1) | stats sum(num0) as c", 280, false),
        new Case("where isnotnull(str1) | stats min(num0) as c", 10, false),
        new Case("where isnotnull(str1) | stats max(num0) as c", 90, false),
        new Case("where isnotnull(str1) | stats dc(str0) as c", 4, false),
        new Case("where isnotnull(str1) | fields str0, str1", 6, true),

        // ===== IS_NULL with various aggregates =====
        new Case("where isnull(str1) | stats count() as c", 4, false),
        new Case("where isnull(str1) | stats sum(num0) as c", 270, false),
        new Case("where isnull(str1) | stats min(num0) as c", 30, false),
        new Case("where isnull(str1) | stats max(num0) as c", 100, false),
        new Case("where isnull(str1) | stats dc(str0) as c", 4, false),
        new Case("where isnull(str1) | fields str0", 4, true)
    );

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (!dataProvisioned) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testFilterDelegationExtended() throws Exception {
        List<String> failures = new java.util.ArrayList<>();
        for (Case c : CASES) {
            try {
                if (c.isRowCount) {
                    assertRowCount(c.pplSuffix, (int) c.expected);
                } else {
                    assertAggCount(c.pplSuffix, c.expected);
                }
            } catch (AssertionError | Exception e) {
                failures.add(c.pplSuffix + " → " + e.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            fail("Filter delegation failures (" + failures.size() + " of " + CASES.size() + "):\n"
                + String.join("\n", failures));
        }
    }

    @SuppressWarnings("unchecked")
    private void assertAggCount(String pplSuffix, long expected) throws Exception {
        String ppl = "source = " + DATASET.indexName + " | " + pplSuffix;
        Map<String, Object> result = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("datarows null for [" + pplSuffix + "]", rows);
        assertEquals("expected 1 agg row for [" + pplSuffix + "]", 1, rows.size());
        assertEquals("[" + pplSuffix + "]", expected, ((Number) rows.get(0).get(0)).longValue());
    }

    @SuppressWarnings("unchecked")
    private void assertRowCount(String pplSuffix, int expected) throws Exception {
        String ppl = "source = " + DATASET.indexName + " | " + pplSuffix;
        Map<String, Object> result = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull("datarows null for [" + pplSuffix + "]", rows);
        assertEquals("[" + pplSuffix + "] row count", expected, rows.size());
    }
}
