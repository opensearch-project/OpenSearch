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
        new Case("where isnull(str1) | fields str0", 4, true),

        // ===== LIKE (keyword field only) =====
        new Case("where str0 like 'app%' | stats count() as c", 3, false),
        new Case("where str0 like 'ban%' | stats sum(num0) as c", 150, false),
        new Case("where str0 like '%e%' | stats count() as c", 7, false),

        // ===== Comparisons (GT/GTE/LT/LTE on integer field) =====
        new Case("where num0 > 50 | stats count() as c", 5, false),
        new Case("where num0 >= 50 | stats count() as c", 6, false),
        new Case("where num0 < 40 | stats count() as c", 3, false),
        new Case("where num0 <= 40 | stats count() as c", 4, false),
        new Case("where num0 > 50 | stats sum(num0) as c", 400, false),
        new Case("where num0 > 50 | stats dc(str0) as c", 3, false),

        // ===== IN =====
        new Case("where str0 in ('apple', 'cherry') | stats count() as c", 5, false),
        new Case("where str0 in ('apple', 'cherry') | stats sum(num0) as c", 210, false),

        // ===== AVG — additional exact-integer cases =====
        new Case("where str0 != 'cherry' | stats avg(num0) as c", 50, false),

        // ===== dc() across more predicate shapes =====
        new Case("where str0 != 'banana' | stats dc(str0) as c", 3, false),
        new Case("where isnull(str1) | stats dc(num0) as c", 4, false),
        new Case("where isnotnull(str1) | stats dc(num0) as c", 6, false),

        // ===== PERCENTILE =====
        new Case("where str0 != 'apple' | stats percentile(num0, 50) as c", 70, false),
        new Case("where isnull(str1) | stats percentile(num0, 50) as c", 70, false)
    );

    private record StatisticalCase(String pplSuffix, double expected) {}

    private static final List<StatisticalCase> STATISTICAL_CASES = List.of(
        new StatisticalCase("where str0 != 'apple' | stats stddev_pop(num0) as c", 20.0),
        new StatisticalCase("where str0 != 'apple' | stats stddev_samp(num0) as c", 21.602469),
        new StatisticalCase("where isnull(str1) | stats stddev_pop(num0) as c", 25.860201),
        new StatisticalCase("where isnull(str1) | stats stddev_samp(num0) as c", 29.860788),
        new StatisticalCase("where isnotnull(str1) | stats stddev_pop(num0) as c", 27.487371),
        new StatisticalCase("where isnotnull(str1) | stats stddev_samp(num0) as c", 30.110906)
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
    public void testStatisticalAggregatesWithDelegation() throws Exception {
        List<String> failures = new java.util.ArrayList<>();
        for (StatisticalCase dc : STATISTICAL_CASES) {
            try {
                String ppl = "source = " + DATASET.indexName + " | " + dc.pplSuffix;
                Map<String, Object> result = executePpl(ppl);
                List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
                assertNotNull("datarows null for [" + dc.pplSuffix + "]", rows);
                assertEquals("expected 1 agg row for [" + dc.pplSuffix + "]", 1, rows.size());
                double actual = ((Number) rows.get(0).get(0)).doubleValue();
                assertEquals("[" + dc.pplSuffix + "]", dc.expected, actual, 0.1);
            } catch (AssertionError | Exception e) {
                failures.add(dc.pplSuffix + " → " + e.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            fail("Statistical aggregate failures (" + failures.size() + " of " + STATISTICAL_CASES.size() + "):\n"
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
