/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.coordinator;

import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage for the lane-sizing logic in
 * {@link ReduceStageExecutionFactory#resolveNumProducers(ClusterService, Stage)}.
 * The factory's per-child wiring depends on getting the producer count right —
 * a failing resolver or a missing cluster service must fall back to {@code 1} so
 * the reduce stage still runs (just without per-shard parallelism), never throw.
 */
public class ReduceStageExecutionFactoryTests extends OpenSearchTestCase {

    public void testNullClusterServiceFallsBackToOne() {
        Stage child = mock(Stage.class);
        // No interactions on `child` — the null-clusterService branch short-circuits first.
        assertEquals(1, ReduceStageExecutionFactory.resolveNumProducers(null, child));
    }

    public void testNullTargetResolverFallsBackToOne() {
        // A child without a TargetResolver is a coordinator-side child (e.g. VALUES) —
        // a single producer is the natural sizing.
        ClusterService clusterService = mock(ClusterService.class);
        Stage child = mock(Stage.class);
        when(child.getTargetResolver()).thenReturn(null);

        assertEquals(1, ReduceStageExecutionFactory.resolveNumProducers(clusterService, child));
    }

    public void testNonEmptyTargetListReturnsItsSize() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);

        TargetResolver resolver = mock(TargetResolver.class);
        when(resolver.resolve(any(), any())).thenReturn(
            // Three resolved targets — typical 3-shard SHARD_FRAGMENT child.
            List.of(stubTarget(), stubTarget(), stubTarget())
        );

        Stage child = mock(Stage.class);
        when(child.getTargetResolver()).thenReturn(resolver);

        assertEquals(3, ReduceStageExecutionFactory.resolveNumProducers(clusterService, child));
    }

    public void testEmptyTargetListClampsToOne() {
        // Resolver returned an empty list — perhaps the index is empty / closed. Lane count
        // must still be >= 1 because StreamingTable refuses zero-partition registrations.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        TargetResolver resolver = mock(TargetResolver.class);
        when(resolver.resolve(any(), any())).thenReturn(List.of());

        Stage child = mock(Stage.class);
        when(child.getTargetResolver()).thenReturn(resolver);

        assertEquals(1, ReduceStageExecutionFactory.resolveNumProducers(clusterService, child));
    }

    public void testNullTargetListClampsToOne() {
        // Defensive: the contract is non-null, but a misbehaving custom resolver
        // returning null shouldn't NPE the factory at plan-build time.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        TargetResolver resolver = mock(TargetResolver.class);
        when(resolver.resolve(any(), any())).thenReturn(null);

        Stage child = mock(Stage.class);
        when(child.getTargetResolver()).thenReturn(resolver);

        assertEquals(1, ReduceStageExecutionFactory.resolveNumProducers(clusterService, child));
    }

    public void testThrowingResolverFallsBackToOne() {
        // A resolver that throws (index missing, mapping changed mid-flight, etc.) MUST NOT
        // surface from `resolveNumProducers`. The actual scheduler will hit the same failure
        // with full context when it dispatches — we just need a safe lane sizing here.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        TargetResolver resolver = mock(TargetResolver.class);
        when(resolver.resolve(any(), any())).thenThrow(new RuntimeException("index missing"));

        Stage child = mock(Stage.class);
        when(child.getTargetResolver()).thenReturn(resolver);

        assertEquals(1, ReduceStageExecutionFactory.resolveNumProducers(clusterService, child));
    }

    public void testLargeShardCountIsPassedThroughVerbatim() {
        // The factory passes producer count through; any cap is the lane policy's job.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(mock(ClusterState.class));

        ExecutionTarget[] targets = new ExecutionTarget[64];
        for (int i = 0; i < targets.length; i++) {
            targets[i] = stubTarget();
        }
        TargetResolver resolver = mock(TargetResolver.class);
        when(resolver.resolve(any(), any())).thenReturn(List.of(targets));

        Stage child = mock(Stage.class);
        when(child.getTargetResolver()).thenReturn(resolver);

        assertEquals(64, ReduceStageExecutionFactory.resolveNumProducers(clusterService, child));
    }

    /** A no-op {@link ExecutionTarget} stand-in — the factory only counts the list. */
    private static ExecutionTarget stubTarget() {
        return new ExecutionTarget(null) {
        };
    }
}
