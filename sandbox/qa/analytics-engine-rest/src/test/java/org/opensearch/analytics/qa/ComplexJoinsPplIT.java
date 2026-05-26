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
 * Complex Joins PPL integration test (multi-index). Tests join operations across multiple indexes.
 * Uses existing indexes from other datasets: security_logs, app_monitor, kubernetes_logs,
 * monitor_tracking, performance_metrics, voice_verification.
 */
public class ComplexJoinsPplIT extends BasePplIT {

    private boolean additionalDataProvisioned = false;

    @Override
    protected Dataset getDataset() {
        return ComplexJoinsTestHelper.DATASET;
    }

    private void ensureAdditionalDataProvisioned() throws Exception {
        if (!additionalDataProvisioned) {
            DatasetProvisioner.provision(client(), SecurityLogsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), MultiSourceJoinsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), KubernetesLogsTestHelper.DATASET);
            DatasetProvisioner.provision(client(), PerformanceMetricsTestHelper.DATASET);
            additionalDataProvisioned = true;
        }
    }

    @AwaitsFix(bugUrl = "Failing due to unsupported operations")
    public void testComplexJoinsPplQueries() throws Exception {
        ensureAdditionalDataProvisioned();
        runPplQueries();
    }
}
