/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.Version;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CanMatchPreFilterPhaseTests extends OpenSearchTestCase {

    private TransportService transportService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transportService = mock(TransportService.class);
    }

    public void testEmptyTargetsReturnsEmpty() {
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(Collections.emptyList(), new byte[] { 1, 2, 3 }, ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertNotNull(result.get());
        assertTrue(result.get().isEmpty());
        verify(transportService, never()).sendRequest(
            any(DiscoveryNode.class),
            any(String.class),
            any(),
            any(TransportResponseHandler.class)
        );
    }

    public void testEmptyFilterBytesPassesAllTargets() {
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0), target("idx", 1));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(targets, new byte[0], ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(targets, result.get());
        verify(transportService, never()).sendRequest(
            any(DiscoveryNode.class),
            any(String.class),
            any(),
            any(TransportResponseHandler.class)
        );
    }

    public void testNullFilterBytesPassesAllTargets() {
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(targets, null, ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(targets, result.get());
    }

    public void testAllTargetsMatch() {
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0), target("idx", 1));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(targets, new byte[] { 1 }, ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(2, result.get().size());
    }

    public void testTransportFailureKeepsTarget() {
        // Transport failure → fail-open → target kept
        mockCanMatchException(new TransportException("connection lost"));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(targets, new byte[] { 1 }, ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(1, result.get().size());
    }

    public void testOriginalOrderPreserved() {
        // Responses may arrive out of order; result must preserve input order
        // Both match — verify order is [0, 1] not [1, 0]
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget first = target("idx", 0);
        ExecutionTarget second = target("idx", 1);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(List.of(first, second), new byte[] { 1 }, ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(2, result.get().size());
        assertSame(first, result.get().get(0));
        assertSame(second, result.get().get(1));
    }

    public void testFiveShardsThreeMatchTwoPruned() {
        // Shards 0,2,4 match; shards 1,3 pruned
        mockCanMatchResponseSequence(true, false, true, false, true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget t0 = target("idx", 0);
        ExecutionTarget t1 = target("idx", 1);
        ExecutionTarget t2 = target("idx", 2);
        ExecutionTarget t3 = target("idx", 3);
        ExecutionTarget t4 = target("idx", 4);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(List.of(t0, t1, t2, t3, t4), new byte[] { 1 },
            ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(3, result.get().size());
        assertSame(t0, result.get().get(0));
        assertSame(t2, result.get().get(1));
        assertSame(t4, result.get().get(2));
    }

    public void testAllTargetsPrunedReturnsEmptyList() {
        mockCanMatchResponse(false);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0), target("idx", 1));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(targets, new byte[] { 1 },
            ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertNotNull(result.get());
        assertTrue(result.get().isEmpty());
    }

    public void testSingleTargetMatch() {
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget only = target("idx", 0);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(List.of(only), new byte[] { 1 },
            ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(1, result.get().size());
        assertSame(only, result.get().get(0));
    }

    public void testSingleTargetPruned() {
        mockCanMatchResponse(false);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        phase.filter(targets, new byte[] { 1 },
            ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertNotNull(result.get());
        assertTrue(result.get().isEmpty());
    }

    // --- helpers ---

    private ShardExecutionTarget target(String indexName, int shardNum) {
        DiscoveryNode node = new DiscoveryNode("node-" + shardNum, buildNewFakeTransportAddress(), Version.CURRENT);
        ShardId shardId = new ShardId(new Index(indexName, "_na_"), shardNum);
        return new ShardExecutionTarget(node, shardId, shardNum);
    }

    @SuppressWarnings("unchecked")
    private void mockCanMatchResponse(boolean canMatch) {
        doAnswer(inv -> {
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            handler.handleResponse(new AnalyticsCanMatchResponse(canMatch));
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                eq(AnalyticsCanMatchAction.NAME),
                any(TransportRequest.class),
                any(TransportResponseHandler.class)
            );
    }

    @SuppressWarnings("unchecked")
    private void mockCanMatchResponseSequence(boolean... responses) {
        final int[] callCount = { 0 };
        doAnswer(inv -> {
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            boolean match = callCount[0] < responses.length && responses[callCount[0]];
            callCount[0]++;
            handler.handleResponse(new AnalyticsCanMatchResponse(match));
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                eq(AnalyticsCanMatchAction.NAME),
                any(TransportRequest.class),
                any(TransportResponseHandler.class)
            );
    }

    @SuppressWarnings("unchecked")
    private void mockCanMatchException(TransportException exception) {
        doAnswer(inv -> {
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            handler.handleException(exception);
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                eq(AnalyticsCanMatchAction.NAME),
                any(TransportRequest.class),
                any(TransportResponseHandler.class)
            );
    }
}
