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
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportResponseHandler;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link CanMatchPreFilterPhase}: transport dispatch to each target,
 * filtering based on per-target canMatch response, and fail-open semantics on
 * transport exceptions.
 */
public class CanMatchPreFilterPhaseTests extends OpenSearchTestCase {

    private static final String BACKEND_ID = "datafusion";

    /** Empty target list short-circuits — no dispatch happens. */
    public void testEmptyTargetsReturnsEmpty() {
        StreamTransportService transport = mock(StreamTransportService.class);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transport);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();
        phase.filter(Collections.emptyList(), new byte[] { 1, 2, 3 }, BACKEND_ID, ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertNotNull(result.get());
        assertTrue(result.get().isEmpty());
        verify(transport, never()).sendRequest(any(DiscoveryNode.class), any(String.class), any(), any());
    }

    /** Empty filter bytes short-circuits — no dispatch happens, all targets pass. */
    public void testEmptyFilterBytesPassesAll() {
        StreamTransportService transport = mock(StreamTransportService.class);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transport);
        List<ExecutionTarget> targets = List.of(target("a", 0), target("b", 1));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();
        phase.filter(targets, new byte[0], BACKEND_ID, ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertEquals(targets, result.get());
        verify(transport, never()).sendRequest(any(DiscoveryNode.class), any(String.class), any(), any());
    }

    /** Every target reports canMatch=true → returns all targets. */
    public void testAllCanMatchReturnsAll() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ExecutionTarget t0 = target("a", 0);
        ExecutionTarget t1 = target("b", 1);
        stubResponses(transport, Map.of("a", true, "b", true));

        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transport);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();
        phase.filter(List.of(t0, t1), new byte[] { 1 }, BACKEND_ID, ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertEquals(Set.of(t0, t1), Set.copyOf(result.get()));
    }

    /** Targets whose handler reports canMatch=false are dropped. */
    public void testSomeCanNotMatchAreDropped() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ExecutionTarget keep = target("keep", 0);
        ExecutionTarget drop = target("drop", 1);
        stubResponses(transport, Map.of("keep", true, "drop", false));

        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transport);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();
        phase.filter(List.of(keep, drop), new byte[] { 1 }, BACKEND_ID, ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertEquals(List.of(keep), result.get());
    }

    /** Transport failures fail-open: the target is conservatively retained. */
    public void testTransportExceptionIsFailOpen() {
        StreamTransportService transport = mock(StreamTransportService.class);
        ExecutionTarget t0 = target("a", 0);
        ExecutionTarget t1 = target("b", 1);
        doAnswer(inv -> {
            DiscoveryNode node = inv.getArgument(0);
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            if (node.getName().equals("a")) {
                handler.handleException(new NodeNotConnectedException(node, "boom"));
            } else {
                handler.handleResponse(AnalyticsCanMatchResponse.YES);
            }
            return null;
        }).when(transport).sendRequest(any(DiscoveryNode.class), eq(AnalyticsCanMatchAction.NAME), any(), any());

        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transport);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();
        phase.filter(List.of(t0, t1), new byte[] { 1 }, BACKEND_ID, ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertEquals(Set.of(t0, t1), Set.copyOf(result.get()));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static ExecutionTarget target(String nodeName, int shardNum) {
        DiscoveryNode node = new DiscoveryNode(
            nodeName,
            nodeName,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + shardNum),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        return new ShardExecutionTarget(node, new ShardId("idx", "_na_", shardNum));
    }

    /**
     * Stubs {@code transport.sendRequest(...)} to immediately invoke the handler with a
     * {@link AnalyticsCanMatchResponse} keyed by node name.
     */
    private static void stubResponses(StreamTransportService transport, Map<String, Boolean> nodeNameToCanMatch) {
        doAnswer(inv -> {
            DiscoveryNode node = inv.getArgument(0);
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            Boolean canMatch = nodeNameToCanMatch.get(node.getName());
            assertNotNull("no stub for node " + node.getName(), canMatch);
            handler.handleResponse(canMatch ? AnalyticsCanMatchResponse.YES : AnalyticsCanMatchResponse.NO);
            return null;
        }).when(transport).sendRequest(any(DiscoveryNode.class), eq(AnalyticsCanMatchAction.NAME), any(), any());
    }
}
