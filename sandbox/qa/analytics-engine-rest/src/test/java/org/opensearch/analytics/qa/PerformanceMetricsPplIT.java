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
 * Performance metrics analysis PPL integration test.
 */
public class PerformanceMetricsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return PerformanceMetricsTestHelper.DATASET;
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/21948")
    public void testPerformanceMetricsPplQueries() throws Exception {
        runPplQueries();
    }
}
