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
 * Regression coverage for PPL percentile SHORTCUTS ({@code percNN}, {@code pNN}) on the
 * analytics-engine route.
 *
 * <p>The standard form {@code percentile(field, 50)} emits the percent as an INTEGER literal
 * ({@code $f1=[50]}), which {@code DataFusionFragmentConvertor.LOCAL_PERCENTILE_APPROX_OP
 * .normaliseLiteralArg} rescales from PPL's [0,100] to DataFusion's [0,1] before substrait
 * emission. The shortcut form {@code perc50(field)} instead emits a DOUBLE literal
 * ({@code $f1=[50.0E0:DOUBLE]}); the rescale guard only matched one literal representation, so
 * the unscaled {@code 50.0} reached DataFusion and failed planning with
 * "Percentile value must be between 0.0 and 1.0 inclusive, 50 is invalid". The two forms must
 * agree.
 */
public class PercentileShortcutIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** {@code perc50(num0)} must equal the known-good {@code percentile(num0, 50)}. */
    public void testPerc50_matchesStandardPercentile() throws IOException {
        assertShortcutMatchesStandard("perc50(num0)", "percentile(num0, 50)");
    }

    /** {@code p95(num0)} (the {@code pNN} spelling) must equal {@code percentile(num0, 95)}. */
    public void testP95_matchesStandardPercentile() throws IOException {
        assertShortcutMatchesStandard("p95(num0)", "percentile(num0, 95)");
    }

    /** Fractional shortcut {@code perc25.5(num0)} must equal {@code percentile(num0, 25.5)}. */
    public void testPerc25_5_matchesStandardPercentile() throws IOException {
        assertShortcutMatchesStandard("perc25.5(num0)", "percentile(num0, 25.5)");
    }

    /** Further fractional spellings exercised by the SQL plugin's percentile-shortcut suite
     *  ({@code perc99.5}, {@code p75.25}, {@code perc0.1}) — each must equal its standard form. */
    public void testFractionalSpellings_matchStandardPercentile() throws IOException {
        assertShortcutMatchesStandard("perc99.5(num0)", "percentile(num0, 99.5)");
        assertShortcutMatchesStandard("p75.25(num0)", "percentile(num0, 75.25)");
        assertShortcutMatchesStandard("perc0.1(num0)", "percentile(num0, 0.1)");
    }

    /** Rescale boundaries: perc0 → 0.0 and perc100 → 1.0 (the inclusive [0,1] edges DataFusion
     *  validates). perc100 is the value the original bug rejected as ">1.0"; assert both edges
     *  fire and equal their standard forms. */
    public void testBoundaryPercentiles_matchStandardPercentile() throws IOException {
        assertShortcutMatchesStandard("perc0(num0)", "percentile(num0, 0)");
        assertShortcutMatchesStandard("perc100(num0)", "percentile(num0, 100)");
    }

    private void assertShortcutMatchesStandard(String shortcut, String standard) throws IOException {
        double expected = singleAgg("source=" + DATASET.indexName + " | stats " + standard + " as v");
        double actual = singleAgg("source=" + DATASET.indexName + " | stats " + shortcut + " as v");
        assertEquals("shortcut " + shortcut + " must equal " + standard, expected, actual, 1e-9);
    }

    @SuppressWarnings("unchecked")
    private double singleAgg(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        assertEquals("expected one aggregate row for query: " + ppl, 1, rows.size());
        return ((Number) rows.get(0).get(0)).doubleValue();
    }
}
