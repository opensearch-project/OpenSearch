/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ShardTaskRunner}'s per-node admission queue behavior — verifies
 * tasks on the same node share a {@link PendingExecutions} queue while tasks on different
 * nodes get separate queues.
 */
public class ShardTaskRunnerTests extends OpenSearchTestCase {

    public void testTasksOnSameNodeShareSinglePendingQueue() {
        List<PendingExecutions> captured = new ArrayList<>();
        ShardTaskRunner runner = newRunner(captured);

        ShardStageTask t0 = shardTask(0, "node-A");
        ShardStageTask t1 = shardTask(1, "node-A");
        runner.run(t0, noopHandle());
        runner.run(t1, noopHandle());

        assertEquals("both tasks dispatched", 2, captured.size());
        assertSame("tasks on same node reuse the same pending queue", captured.get(0), captured.get(1));
    }

    public void testTasksOnDifferentNodesGetSeparatePendingQueues() {
        List<PendingExecutions> captured = new ArrayList<>();
        ShardTaskRunner runner = newRunner(captured);

        ShardStageTask onA = shardTask(0, "node-A");
        ShardStageTask onB = shardTask(1, "node-B");
        runner.run(onA, noopHandle());
        runner.run(onB, noopHandle());

        assertEquals(2, captured.size());
        assertNotSame("tasks on different nodes get distinct queues", captured.get(0), captured.get(1));
    }

    public void testPendingQueueIsLazilyCreatedAndCached() {
        List<PendingExecutions> captured = new ArrayList<>();
        ShardTaskRunner runner = newRunner(captured);

        // First three dispatches on node-A reuse the same queue; the fourth on node-B is fresh.
        runner.run(shardTask(0, "node-A"), noopHandle());
        runner.run(shardTask(1, "node-A"), noopHandle());
        runner.run(shardTask(2, "node-A"), noopHandle());
        runner.run(shardTask(3, "node-B"), noopHandle());

        assertEquals(4, captured.size());
        assertSame(captured.get(0), captured.get(1));
        assertSame(captured.get(1), captured.get(2));
        assertNotSame(captured.get(2), captured.get(3));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private ShardTaskRunner newRunner(List<PendingExecutions> capturedQueues) {
        ShardFragmentStageExecution stage = mock(ShardFragmentStageExecution.class);
        QueryContext config = mock(QueryContext.class);
        when(config.maxConcurrentShardRequests()).thenReturn(5);
        when(config.parentTask()).thenReturn(mock(AnalyticsQueryTask.class));

        AnalyticsSearchTransportService transport = mock(AnalyticsSearchTransportService.class);
        doAnswer(inv -> {
            capturedQueues.add(inv.getArgument(4));  // 5th arg is PendingExecutions
            return null;
        }).when(transport).dispatchFragmentStreaming(any(), any(), any(), any(), any());

        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder = t -> mock(FragmentExecutionRequest.class);
        return new ShardTaskRunner(stage, config, transport, requestBuilder);
    }

    private static ShardStageTask shardTask(int partitionId, String nodeId) {
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn(nodeId);
        ShardExecutionTarget target = new ShardExecutionTarget(node, new ShardId("idx", "_na_", partitionId));
        return new ShardStageTask(new StageTaskId(0, partitionId), target);
    }

    private static ActionListener<Void> noopHandle() {
        return new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception cause) {}
        };
    }
}
