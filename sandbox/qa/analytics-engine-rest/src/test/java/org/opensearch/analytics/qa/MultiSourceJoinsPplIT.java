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
 * Complex Redesigned (multi-index) PPL integration test. Runs PPL queries against complex_redesigned data.
 */
public class MultiSourceJoinsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return MultiSourceJoinsTestHelper.DATASET;
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/21948")
    public void testMultiSourceJoinsPplQueries() throws Exception {
        runPplQueries();
    }

    /** Queries that fail at 1 shard: multi-source join unsupported. Skipped so the rest run and are visible. */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(2, 4);
    }
}
