/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Kubernetes log analysis PPL integration test.
 */
public class KubernetesLogsPplIT extends BasePplIT {

    @Override
    protected Dataset getDataset() {
        return KubernetesLogsTestHelper.DATASET;
    }

    public void testKubernetesLogsPplQueries() throws Exception {
        runPplQueries();
    }

    /**
     * Queries that fail at 1 shard:
     *   9: unsupported operation.
     *   6: multi-filter rejection (PR #21948 — `dedup → stats`).
     */
    @Override
    protected java.util.Set<Integer> getSkipQueries() {
        return java.util.Set.of(6, 9);
    }
}
