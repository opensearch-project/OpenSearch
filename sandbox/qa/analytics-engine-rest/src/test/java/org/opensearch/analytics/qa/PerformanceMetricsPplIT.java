/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Performance metrics analysis PPL integration test.
 */
public class PerformanceMetricsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return PerformanceMetricsTestHelper.DATASET;
    }

    public void testPerformanceMetricsPplQueries() throws Exception {
        runPplQueries();
    }
}
