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
 * Multi-Index Queries PPL integration test (multi-index). Tests fields, rename, top, rare, span commands.
 * Uses existing indexes from other datasets: security_logs, api_metrics, performance_metrics, exception_logs.
 */
public class MultiIndexQueriesPplIT extends BasePplIT {

    private boolean additionalDataProvisioned = false;

    @Override
    protected Dataset getDataset() {
        return MultiIndexQueriesTestHelper.DATASET;
    }

    private void ensureAdditionalDataProvisioned() throws Exception {
        if (!additionalDataProvisioned) {
            DatasetProvisioner.provision(client(), SecurityLogsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), ApiMetricsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), PerformanceMetricsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), ExceptionLogsTestHelper.DATASET);
            additionalDataProvisioned = true;
        }
    }

    @AwaitsFix(bugUrl = "Failing due to unsupported operations")
    public void testMultiIndexQueriesPplQueries() throws Exception {
        ensureAdditionalDataProvisioned();
        runPplQueries();
    }
}
