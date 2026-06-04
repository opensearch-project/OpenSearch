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
 * Two-node topology coverage. With two data nodes and a 4-shard index the
 * primaries are typically distributed (~2 per node), so the coordinator-reduce
 * stage receives streamed batches from both same-node and peer-node fragment
 * executions. Exercises the cross-node FragmentExecutionAction streaming path.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class CoordinatorTwoNodeTopologyIT extends CoordinatorTopologyTestBase {

    public void testTwoNodeFourShardSum() throws Exception {
        runSumOverSeededIndex("topology_two_node_idx", 4, 40);
    }
}
