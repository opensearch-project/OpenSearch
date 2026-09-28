/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * API Metrics PPL integration test. Runs PPL queries against API metrics data.
 */
public class ApiMetricsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return ApiMetricsTestHelper.DATASET;
    }

    public void testApiMetricsPplQueries() throws Exception {
        runPplQueries();
    }
}
