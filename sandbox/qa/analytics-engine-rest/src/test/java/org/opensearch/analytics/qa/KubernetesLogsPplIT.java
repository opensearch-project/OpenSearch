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
 * Kubernetes log analysis PPL integration test.
 */
public class KubernetesLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return KubernetesLogsTestHelper.DATASET;
    }

    @AwaitsFix(bugUrl = "Failing due to unsupported operations")
    public void testKubernetesLogsPplQueries() throws Exception {
        runPplQueries();
    }
}
