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
 * Lookup Table Queries PPL integration test (multi-index). Runs PPL queries with lookup operations.
 */
public class LookupTableQueriesPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return LookupTableQueriesTestHelper.DATASET;
    }

    @AwaitsFix(bugUrl = "Failing due to unsupported operations")
    public void testLookupTableQueriesPplQueries() throws Exception {
        runPplQueries();
    }
}
