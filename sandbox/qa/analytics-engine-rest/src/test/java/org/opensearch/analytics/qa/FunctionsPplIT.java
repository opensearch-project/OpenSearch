/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;


/**
 * PPL function testing PPL integration test.
 */
public class FunctionsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return FunctionsTestHelper.DATASET;
    }

    public void testFunctionsPplQueries() throws Exception {
        runPplQueries();
    }

    /** Queries that fail at 1 shard: split() unsupported. Skipped so the rest run and are visible. */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(13);
    }
}
