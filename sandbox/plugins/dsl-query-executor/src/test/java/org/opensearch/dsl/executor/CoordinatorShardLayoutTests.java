/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

/**
 * The {@code S_node} read: shards of one index on the <em>busiest node</em>, from live routing.
 */
public class CoordinatorShardLayoutTests extends OpenSearchTestCase {

    private static final String INDEX = "products";

    private OperationRouting routing;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        routing = ShardLayouts.routing();
    }

    /**
     * The value that matters: 6 shards split 4/2 is a busiest node of 4, not 6 (the index's shard count)
     * and not 3 (an average). Substituting the index-wide count would inflate the per-sub-query fragment
     * cost on every multi-node cluster and silently pin the fan-out at 1.
     */
    public void testBusiestNodeCountOnSkewedPlacement() {
        ClusterState state = state(List.of("node-0", "node-0", "node-0", "node-0", "node-1", "node-1"));
        assertEquals(4, CoordinatorShardLayout.shardsOnBusiestNode(state, routing, INDEX));
    }

    public void testBusiestNodeCountIsOneOnSingleShardIndex() {
        ClusterState state = state(List.of("node-0"), "node-0", "node-1");
        assertEquals(1, CoordinatorShardLayout.shardsOnBusiestNode(state, routing, INDEX));
    }

    /** The co-located-coordinator case: F is largest here, so K_gate is smallest. */
    public void testAllShardsOnOneNodeReturnsShardCount() {
        ClusterState state = state(List.of("node-0", "node-0", "node-0", "node-0"), "node-0");
        assertEquals(4, CoordinatorShardLayout.shardsOnBusiestNode(state, routing, INDEX));
    }

    public void testUnassignedShardsAreSkippedNotCounted() {
        // Arrays.asList, not List.of: a null placement is the point of this case.
        ClusterState state = state(Arrays.asList("node-0", "node-0", "node-0", null));
        assertEquals(3, CoordinatorShardLayout.shardsOnBusiestNode(state, routing, INDEX));
    }

    /** Never 0: the width formula divides by this value. */
    public void testNoAssignedShardReturnsOneNotZero() {
        ClusterState state = state(Arrays.asList(null, null, null));
        assertEquals(1, CoordinatorShardLayout.shardsOnBusiestNode(state, routing, INDEX));
    }

    /**
     * A routing read that throws outright — the realistic cause is an index deleted between this
     * request's cluster-state snapshot and the read — must degrade to the neutral 1, not fail the search.
     * An advisory input has no business turning a valid {@code _search} into an error.
     */
    public void testUnknownIndexReturnsOneRatherThanThrowing() {
        ClusterState state = state(List.of("node-0", "node-0"));
        assertEquals(1, CoordinatorShardLayout.shardsOnBusiestNode(state, routing, "no-such-index"));
    }

    /** Placements for shard 0..n-1; a null entry leaves that shard unassigned. */
    private static ClusterState state(List<String> placements, String... extraNodes) {
        return ShardLayouts.clusterState(INDEX, placements, extraNodes);
    }
}
