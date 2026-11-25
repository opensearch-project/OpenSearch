/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.canmatch.AnalyticsCanMatchAction;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.LongRange;
import org.opensearch.analytics.exec.canmatch.SortSpec;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.dag.TargetResolver;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The can-match round trip is only worth its latency on a wide fan-out, so it is gated on shard
 * count the same way vanilla gates its pre-filter phase
 * ({@code TransportSearchAction.shouldPreFilterSearchShards}): run it only when the query has
 * something to prune or order by <em>and</em> the fan-out exceeds
 * {@code analytics.query.pre_filter_shard_size}.
 *
 * <p>A sorted query drops the threshold to 1 — mirroring vanilla's {@code hasPrimaryFieldSort}
 * case — because shard ordering and the top-N gate pay for themselves as soon as there is a second
 * shard to order against.
 *
 * <p>The observable is whether a can-match request reaches the transport at all; everything
 * downstream of that is covered by {@code CanMatchPreFilterPhaseTests} and
 * {@link SortGateWiringTests}.
 */
public class CanMatchTriggerTests extends OpenSearchTestCase {

    private static final List<CanMatchFilter> FILTERS = List.of(new LongRange("total", 0L, 100L));
    private static final SortSpec SORT = new SortSpec("@timestamp", true, 10);

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    /** Narrow fan-out: the probe would cost a round trip and could save at most a shard or two. */
    public void testFanOutAtOrBelowThresholdSkipsCanMatch() {
        Fixture f = new Fixture(3, FILTERS, null, 3);

        f.start();

        verify(f.transportService, never()).sendRequest(any(), eq(AnalyticsCanMatchAction.NAME), any(), any());
        assertEquals("every shard is still dispatched", 3, f.exec.tasks().size());
    }

    /** Wide fan-out with something to prune: the probe runs, one request per shard. */
    public void testFanOutAboveThresholdRunsCanMatch() {
        Fixture f = new Fixture(4, FILTERS, null, 3);

        f.start();

        verify(f.transportService, times(4)).sendRequest(any(), eq(AnalyticsCanMatchAction.NAME), any(), any());
    }

    /** Nothing to prune and nothing to order by — shard count is irrelevant, the probe has no question to ask. */
    public void testNoFiltersAndNoSortNeverRunsCanMatch() {
        Fixture f = new Fixture(500, List.of(), null, 3);

        f.start();

        verify(f.transportService, never()).sendRequest(any(), eq(AnalyticsCanMatchAction.NAME), any(), any());
    }

    /**
     * A sorted query pre-filters from the second shard on, even though the configured threshold is far
     * higher: bounds ordering and the top-N gate need only one other shard to be worth collecting.
     */
    public void testSortPreFiltersBelowTheConfiguredThreshold() {
        Fixture f = new Fixture(2, List.of(), SORT, 128);

        f.start();

        verify(f.transportService, times(2)).sendRequest(any(), eq(AnalyticsCanMatchAction.NAME), any(), any());
    }

    /** One shard has nothing to be ordered against and nothing to prune against — not even for a sort. */
    public void testSingleShardNeverPreFilters() {
        Fixture f = new Fixture(1, FILTERS, SORT, 1);

        f.start();

        verify(f.transportService, never()).sendRequest(any(), eq(AnalyticsCanMatchAction.NAME), any(), any());
    }

    // ── fixture ──────────────────────────────────────────────────────────

    private final class Fixture {
        final ShardFragmentStageExecution exec;
        final TransportService transportService;

        Fixture(int shardCount, List<CanMatchFilter> filters, SortSpec sortSpec, int preFilterShardSize) {
            // Targets are built before any when(...) — target() stubs a mock, and Mockito reads a
            // nested stubbing as an unfinished one.
            List<ExecutionTarget> resolved = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; i++) {
                resolved.add(target(i));
            }
            AnalyticsQueryTask parentTask = mock(AnalyticsQueryTask.class);
            ClusterState clusterState = mock(ClusterState.class);

            Stage stage = mock(Stage.class);
            when(stage.getStageId()).thenReturn(0);
            when(stage.getCanMatchFilters()).thenReturn(filters);
            when(stage.getSortSpec()).thenReturn(sortSpec);
            // resolveBackendId reads the first alternative; without one the phase bails before dispatch.
            when(stage.getPlanAlternatives()).thenReturn(List.of(new StagePlan(null, "test-backend")));
            TargetResolver resolver = mock(TargetResolver.class);
            when(resolver.resolve(any(ClusterState.class), any())).thenReturn(resolved);
            when(stage.getTargetResolver()).thenReturn(resolver);

            QueryContext config = mock(QueryContext.class);
            when(config.parentTask()).thenReturn(parentTask);
            when(config.maxConcurrentShardRequestsPerNode()).thenReturn(5);
            when(config.preFilterShardSize()).thenReturn(preFilterShardSize);
            when(config.bufferAllocator()).thenReturn(allocator);

            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.state()).thenReturn(clusterState);

            transportService = mock(TransportService.class);
            AnalyticsSearchTransportService dispatcher = mock(AnalyticsSearchTransportService.class);
            when(dispatcher.getTransportService()).thenReturn(transportService);
            // The probe arms a fail-open timeout before sending; the stubbed transport never answers,
            // so the timer is created and then simply never fires within the test.
            ThreadPool threadPool = mock(ThreadPool.class);
            when(threadPool.schedule(any(), any(), any())).thenReturn(mock(Scheduler.ScheduledCancellable.class));
            StreamTransportService streamingTransportService = mock(StreamTransportService.class);
            when(streamingTransportService.getThreadPool()).thenReturn(threadPool);
            when(dispatcher.getStreamingTransportService()).thenReturn(streamingTransportService);

            Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = t -> new FragmentExecutionRequest(
                "test-query",
                0,
                t.shardId(),
                List.of(new FragmentExecutionRequest.PlanAlternative("test-backend", new byte[0], List.of()))
            );
            this.exec = new ShardFragmentStageExecution(stage, config, new NoopSink(), clusterService, requestBuilder, dispatcher);
        }

        void start() {
            exec.start(ActionListener.wrap(v -> {}, e -> {}));
        }
    }

    private static ExecutionTarget target(int ordinal) {
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("test-node-" + ordinal);
        return new ShardExecutionTarget(node, new ShardId("idx", "_na_", ordinal), ordinal);
    }

    private static final class NoopSink implements ExchangeSink {
        @Override
        public void feed(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void close() {}
    }
}
