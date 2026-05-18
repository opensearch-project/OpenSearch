/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Single-node topology coverage. With one data node and a multi-shard index, both
 * primaries are co-located on the coordinator — the local-fan-in path runs without
 * any remote dispatch.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class CoordinatorSingleNodeTopologyIT extends CoordinatorTopologyTestBase {

    public void testSingleNodeMultiShardSum() throws Exception {
        runSumOverSeededIndex("topology_single_node_idx", 2, 20);
    }
}
