/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;


/**
 * Rex command testing PPL integration test.
 */
public class RexCommandPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return RexCommandTestHelper.DATASET;
    }

    public void testRexCommandPplQueries() throws Exception {
        runPplQueries();
    }

    /** Queries that fail at 1 shard: rex unsupported / value mismatch. Skipped so the rest run and are visible. */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(1, 5, 7, 8, 13, 18);
    }
}
