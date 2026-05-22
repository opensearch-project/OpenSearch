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
 * PPL function testing PPL integration test.
 */
public class FunctionsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return FunctionsTestHelper.DATASET;
    }

    @AwaitsFix(bugUrl = "Failing due to unsupported operations")
    public void testFunctionsPplQueries() throws Exception {
        runPplQueries();
    }
}
