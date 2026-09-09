/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Lookup Table Queries PPL integration test (multi-index). Runs PPL queries with lookup operations.
 */
public class LookupTableQueriesPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return LookupTableQueriesTestHelper.DATASET;
    }

    public void testLookupTableQueriesPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * q1 (grok extract -> stats) now runs on the analytics route. q7 stays skipped:
     * its failure is unrelated to grok — the q7.ppl body is multi-line so the test
     * harness cannot serialize it into the {@code _ppl} request JSON ("Unterminated
     * string"), and it also exercises eventstats / dedup / percentile.
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(7);
    }
}
