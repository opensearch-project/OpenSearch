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
 * Application log analysis PPL integration test.
 */
public class AppLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return AppLogsTestHelper.DATASET;
    }

    @AwaitsFix(bugUrl = "Failing due to unsupported operations")
    public void testAppLogsPplQueries() throws Exception {
        runPplQueries();
    }
}
