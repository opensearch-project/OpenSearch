/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

/**
 * Security Logs PPL integration test. Runs PPL queries against security_logs data.
 */
public class SecurityLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return SecurityLogsTestHelper.DATASET;
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/21948")
    public void testSecurityLogsPplQueries() throws Exception {
        runPplQueries();
    }

    /** Queries that fail at 1 shard: unsupported operations / value mismatch. Skipped so the rest run and are visible. */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(1, 2, 3, 4, 5, 7, 8);
    }
}
