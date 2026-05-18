/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Security Logs PPL integration test. Runs PPL queries against security_logs data.
 */
public class SecurityLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return SecurityLogsTestHelper.DATASET;
    }

    public void testSecurityLogsPplQueries() throws Exception {
        runPplQueries();
    }
}
