/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Complex Redesigned (multi-index) PPL integration test. Runs PPL queries against complex_redesigned data.
 */
public class MultiSourceJoinsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return MultiSourceJoinsTestHelper.DATASET;
    }

    public void testMultiSourceJoinsPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Queries that fail at 1 shard:
     *   2, 4: multi-source join unsupported.
     *   5:    multi-filter rejection (PR #21948 — `where ... | dedup | stats | sort`).
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(2, 4, 5);
    }
}
