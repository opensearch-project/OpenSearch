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
 * dc()/distinct_count() correctness across all field types on a 2-shard clickbench index.
 * Compares HLL result against GROUP BY count (ground truth) with 5% tolerance.
 */
public class DistinctCountIT extends AnalyticsRestTestCase {

    private static final Dataset CLICKBENCH = ClickBenchTestHelper.DATASET;
    private static final String IDX = CLICKBENCH.indexName;
    private static volatile boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), CLICKBENCH, 2);
            provisioned = true;
        }
    }

    // ── keyword fields ──────────────────────────────────────────────────────────

    public void testDc_keyword_Title() throws Exception {
        assertDcAccurate("Title");
    }

    public void testDc_keyword_BrowserCountry() throws Exception {
        assertDcAccurate("BrowserCountry");
    }

    public void testDc_keyword_BrowserLanguage() throws Exception {
        assertDcAccurate("BrowserLanguage");
    }

    // ── short (tinyint/smallint) fields ─────────────────────────────────────────

    public void testDc_short_OS() throws Exception {
        assertDcAccurate("OS");
    }

    public void testDc_short_Age() throws Exception {
        assertDcAccurate("Age");
    }

    public void testDc_short_Income() throws Exception {
        assertDcAccurate("Income");
    }

    public void testDc_short_IsMobile() throws Exception {
        assertDcAccurate("IsMobile");
    }

    // ── integer fields ──────────────────────────────────────────────────────────

    public void testDc_integer_RegionID() throws Exception {
        assertDcAccurate("RegionID");
    }

    // ── long fields ─────────────────────────────────────────────────────────────

    public void testDc_long_RefererHash() throws Exception {
        assertDcAccurate("RefererHash");
    }

    // ── with filter (exercises delegation + partial aggregate interaction) ───────

    public void testDc_keyword_withFilter() throws Exception {
        assertDcWithFilterAccurate("Title", "GoodEvent = 1");
    }

    public void testDc_short_withFilter() throws Exception {
        assertDcWithFilterAccurate("OS", "Age > 0");
    }

    public void testDc_integer_withFilter() throws Exception {
        assertDcWithFilterAccurate("RegionID", "GoodEvent = 1");
    }

    // ── grouped dc ──────────────────────────────────────────────────────────────

    public void testDc_grouped() throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> result = executePpl(
            "source=" + IDX + " | stats distinct_count(Title) as dc by OS"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("datarows");
        assertNotNull(rows);
        assertFalse("grouped dc must return rows", rows.isEmpty());
        for (List<Object> row : rows) {
            int dc = ((Number) row.get(0)).intValue();
            assertTrue("per-group dc must be > 0", dc > 0);
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void assertDcAccurate(String field) throws Exception {
        Map<String, Object> grouped = executePpl("source=" + IDX + " | stats count() by " + field);
        int truth = ((List<List<Object>>) grouped.get("datarows")).size();

        Map<String, Object> dc = executePpl("source=" + IDX + " | stats distinct_count(" + field + ") as dc");
        int dcVal = ((Number) ((List<List<Object>>) dc.get("datarows")).get(0).get(0)).intValue();

        logger.info("dc({}): result={}, truth={}", field, dcVal, truth);
        assertWithinTolerance(field, dcVal, truth);
    }

    @SuppressWarnings("unchecked")
    private void assertDcWithFilterAccurate(String field, String filter) throws Exception {
        Map<String, Object> grouped = executePpl(
            "source=" + IDX + " | where " + filter + " | stats count() by " + field
        );
        int truth = ((List<List<Object>>) grouped.get("datarows")).size();

        Map<String, Object> dc = executePpl(
            "source=" + IDX + " | where " + filter + " | stats distinct_count(" + field + ") as dc"
        );
        int dcVal = ((Number) ((List<List<Object>>) dc.get("datarows")).get(0).get(0)).intValue();

        logger.info("dc({}) with filter '{}': result={}, truth={}", field, filter, dcVal, truth);
        assertWithinTolerance(field + " (filtered)", dcVal, truth);
    }

    private void assertWithinTolerance(String label, int dcVal, int truth) {
        if (truth <= 20) {
            assertEquals("dc(" + label + ") must be exact at low cardinality", truth, dcVal);
        } else {
            double error = Math.abs(dcVal - truth) / (double) truth;
            assertTrue(
                "dc(" + label + ") error " + String.format(java.util.Locale.ROOT, "%.1f%%", error * 100)
                    + " exceeds 5% (dc=" + dcVal + ", truth=" + truth + ")",
                error <= 0.05
            );
        }
    }
}
