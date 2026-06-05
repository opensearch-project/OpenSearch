/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Security Logs PPL integration test. Runs PPL queries against security_logs data.
 */
public class SecurityLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return SecurityLogsTestHelper.DATASET;
    }

    public void testSecurityLogsPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Queries that fail at 1 shard:
     *   1, 2, 3, 4, 5, 7, 8: unsupported operations / value mismatch.
     *   10:                  multi-filter rejection (PR #21948).
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(1, 2, 3, 4, 5, 7, 8, 10);
    }
}
