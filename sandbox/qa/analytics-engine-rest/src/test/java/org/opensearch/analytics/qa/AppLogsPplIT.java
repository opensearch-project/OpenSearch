/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Application log analysis PPL integration test.
 */
public class AppLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return AppLogsTestHelper.DATASET;
    }

    public void testAppLogsPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Queries that fail at 1 shard:
     *   5, 9:  unsupported operations.
     *   3, 10: multi-filter rejection (PR #21948).
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(3, 5, 9, 10);
    }
}
