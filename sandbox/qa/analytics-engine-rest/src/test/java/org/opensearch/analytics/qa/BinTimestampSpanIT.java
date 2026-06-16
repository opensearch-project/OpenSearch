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
 * End-to-end coverage for PPL {@code bin <ts_field> span=Nday} over the shared `calcs`
 * dataset. The lowering builds {@code ADDDATE(epoch_char_literal, days_since_epoch_expr)};
 * the adapter folds the literal base into seconds and emits {@code from_unixtime(...)}
 * because DataFusion cannot multiply Int64 × Interval(DayTime) at runtime.
 */
public class BinTimestampSpanIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testBinTimestampSpan7Days() throws IOException {
        // calcs.datetime0 spans 2004-07-04..08-02 (17 rows). 7-day bins anchored at the
        // unix epoch land on Mondays: 2004-06-28 + 7k. Bucket counts:
        //   2004-07-01 → 2,  2004-07-08 → 4,  2004-07-15 → 2,
        //   2004-07-22 → 7,  2004-07-29 → 2.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | bin datetime0 span=7day | stats count() as cnt by datetime0 | sort datetime0",
            row(2L, "2004-07-01 00:00:00"),
            row(4L, "2004-07-08 00:00:00"),
            row(2L, "2004-07-15 00:00:00"),
            row(7L, "2004-07-22 00:00:00"),
            row(2L, "2004-07-29 00:00:00")
        );
    }

    public void testBinTimestampSpan1Day() throws IOException {
        // 1-day bins. 14 distinct days (some collapse — e.g. 2004-07-28 has 3 rows).
        assertRowsEqual(
            "source=" + DATASET.indexName + " | bin datetime0 span=1day | stats count() as cnt by datetime0 | sort - cnt | head 3",
            row(3L, "2004-07-28 00:00:00"),
            row(2L, "2004-07-14 00:00:00"),
            row(1L, "2004-07-04 00:00:00")
        );
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                Object w = want.get(j);
                Object g = got.get(j);
                String label = "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl;
                // count() comes back as Integer when the value fits, Long otherwise; compare by magnitude.
                if (w instanceof Number && g instanceof Number) {
                    assertEquals(label, ((Number) w).longValue(), ((Number) g).longValue());
                } else {
                    assertEquals(label, w, g);
                }
            }
        }
    }
}
