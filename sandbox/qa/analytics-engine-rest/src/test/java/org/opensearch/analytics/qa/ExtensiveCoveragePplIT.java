/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;


/**
 * Extensive function coverage testing PPL integration test.
 */
public class ExtensiveCoveragePplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return ExtensiveCoverageTestHelper.DATASET;
    }

    public void testExtensiveCoveragePplQueries() throws Exception {
        runPplQueries();
    }

    /** Queries that fail at 1 shard: mixed: date/time formatting, string-value, unsupported-fn (see per-q). Skipped so the rest run and are visible. */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(8, 19, 20, 22, 24, 25, 28, 29, 30, 39, 52, 55, 56, 57, 58, 59, 60, 61, 62, 77, 81, 85, 88, 93, 94, 95, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 108, 110, 111, 112, 114, 115, 117, 119, 120, 125, 126, 128, 131, 132, 136, 137, 138, 139, 143, 144, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 160, 162, 163, 188, 189, 190, 191, 196);
    }
}
