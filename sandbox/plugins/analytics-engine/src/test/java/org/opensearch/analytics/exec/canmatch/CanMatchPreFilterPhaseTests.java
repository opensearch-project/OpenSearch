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
import org.opensearch.analytics.spi.ShardSortBounds;
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

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

        survivorsOf(
            phase,
            Collections.emptyList(),
            new byte[] { 1, 2, 3 },
            "datafusion",
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

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

        survivorsOf(phase, targets, new byte[0], "datafusion", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

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

        survivorsOf(phase, targets, null, "datafusion", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(targets, result.get());
    }

    public void testAllTargetsMatch() {
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0), target("idx", 1));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, targets, new byte[] { 1 }, "datafusion", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(2, result.get().size());
    }

    public void testTransportFailureKeepsTarget() {
        // Transport failure → fail-open → target kept
        mockCanMatchException(new TransportException("connection lost"));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, targets, new byte[] { 1 }, "datafusion", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(1, result.get().size());
    }

    /** Responses may arrive out of order; survivors must keep input order. */
    public void testOriginalOrderPreserved() {
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget first = target("idx", 0);
        ExecutionTarget second = target("idx", 1);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(first, second),
            new byte[] { 1 },
            "datafusion",
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(2, result.get().size());
        assertSame(first, result.get().get(0));
        assertSame(second, result.get().get(1));
    }

    /** Shards 0,2,4 match and 1,3 prune; survivors are [0,2,4] in order. */
    public void testFiveShardsThreeMatchTwoPruned() {
        mockCanMatchResponseSequence(true, false, true, false, true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget t0 = target("idx", 0);
        ExecutionTarget t1 = target("idx", 1);
        ExecutionTarget t2 = target("idx", 2);
        ExecutionTarget t3 = target("idx", 3);
        ExecutionTarget t4 = target("idx", 4);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(t0, t1, t2, t3, t4),
            new byte[] { 1 },
            "datafusion",
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(3, result.get().size());
        assertSame(t0, result.get().get(0));
        assertSame(t2, result.get().get(1));
        assertSame(t4, result.get().get(2));
    }

    public void testAllTargetsPrunedKeepsFirstTarget() {
        mockCanMatchResponse(false);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget first = target("idx", 0);
        List<ExecutionTarget> targets = List.of(first, target("idx", 1));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, targets, new byte[] { 1 }, "datafusion", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertNotNull(result.get());
        assertEquals("all pruned → keep exactly one", 1, result.get().size());
        assertSame("the kept target is the first in original order", first, result.get().get(0));
    }

    public void testSingleTargetMatch() {
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget only = target("idx", 0);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, List.of(only), new byte[] { 1 }, "datafusion", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(1, result.get().size());
        assertSame(only, result.get().get(0));
    }

    /** A single target that prunes is force-kept — there must always be at least one shard. */
    public void testSingleTargetPrunedIsStillKept() {
        mockCanMatchResponse(false);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        ExecutionTarget only = target("idx", 0);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, List.of(only), new byte[] { 1 }, "datafusion", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertNotNull(result.get());
        assertEquals(1, result.get().size());
        assertSame(only, result.get().get(0));
    }

    /** The backendId is forwarded on the request to the data node. */
    public void testBackendIdPassedInRequest() {
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, targets, new byte[] { 1 }, "my-backend", ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(1, result.get().size());
    }

    // --- sort-bounds ordering (stage 2) ---

    /**
     * A sort spec with no filters must still check the shards: nothing is pruned, but the round-trip
     * runs to collect the bounds that order the dispatch.
     */
    public void testSortSpecWithoutFiltersStillChecksShards() {
        mockCanMatchResponseWithBounds(true, bounds(10L, 20L));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0), target("idx", 1));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, targets, new byte[0], "datafusion", descending(), ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals("nothing may be pruned when there are no filters", 2, result.get().size());
        verify(transportService, times(2)).sendRequest(
            any(DiscoveryNode.class),
            eq(AnalyticsCanMatchAction.NAME),
            any(TransportRequest.class),
            any(TransportResponseHandler.class)
        );
    }

    /** No filters AND no sort spec — nothing to learn, so the round-trip must be skipped. */
    public void testNoFiltersAndNoSortSpecSkipsTheRoundTrip() {
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        List<ExecutionTarget> targets = List.of(target("idx", 0));
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(phase, targets, new byte[0], "datafusion", null, ActionListener.wrap(result::set, e -> fail(e.getMessage())));

        assertEquals(targets, result.get());
        verify(transportService, never()).sendRequest(
            any(DiscoveryNode.class),
            any(String.class),
            any(),
            any(TransportResponseHandler.class)
        );
    }

    public void testDescendingOrdersByMaxDescending() {
        ExecutionTarget t0 = target("idx", 0);
        ExecutionTarget t1 = target("idx", 1);
        ExecutionTarget t2 = target("idx", 2);
        // DESC orders by max, so t2 (highest max) runs first.
        mockCanMatchBoundsSequence(bounds(0L, 50L), bounds(0L, 80L), bounds(0L, 100L));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(t0, t1, t2),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(List.of(t2, t1, t0), result.get());
    }

    public void testAscendingOrdersByMinAscending() {
        ExecutionTarget t0 = target("idx", 0);
        ExecutionTarget t1 = target("idx", 1);
        ExecutionTarget t2 = target("idx", 2);
        mockCanMatchBoundsSequence(bounds(300L, 999L), bounds(100L, 999L), bounds(200L, 999L));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(t0, t1, t2),
            new byte[] { 1 },
            "datafusion",
            ascending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals("ASC orders by min ascending", List.of(t1, t2, t0), result.get());
    }

    /** Shards without bounds can never be eliminated, so they sort last (like vanilla's nullsLast). */
    public void testShardsWithoutBoundsSortLast() {
        ExecutionTarget withoutBounds = target("idx", 0);
        ExecutionTarget low = target("idx", 1);
        ExecutionTarget high = target("idx", 2);
        mockCanMatchBoundsSequence(null, bounds(0L, 40L), bounds(0L, 90L));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(withoutBounds, low, high),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(List.of(high, low, withoutBounds), result.get());
    }

    /**
     * Ordering compares raw longs and ignores {@code valueKind}, so a millis and a nanos shard over the
     * same instants sort by scale alone. Harmless: {@code setupSortGate} builds no gate when kinds
     * disagree, so dispatch order never changes results.
     */
    public void testMixedValueKindsOrderByRawValue() {
        ExecutionTarget millisShard = target("logs-millis", 0);
        ExecutionTarget nanosShard = target("logs-nanos", 1);
        // The same instant range, expressed at two scales.
        mockCanMatchBoundsSequence(
            new ShardSortBounds(1_700_000_000_000L, 1_700_000_060_000L, false, ShardSortBounds.VALUE_KIND_INT64_MILLIS),
            new ShardSortBounds(1_700_000_000_000_000_000L, 1_700_000_060_000_000_000L, false, ShardSortBounds.VALUE_KIND_INT64_NANOS)
        );
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(millisShard, nanosShard),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals("nanos sorts first on scale alone — same instants", List.of(nanosShard, millisShard), result.get());
    }

    /** Pruning and ordering are independent: pruned shards drop out, survivors get ordered. */
    public void testPrunedShardsExcludedAndSurvivorsOrdered() {
        ExecutionTarget t0 = target("idx", 0);
        ExecutionTarget t1 = target("idx", 1);
        ExecutionTarget t2 = target("idx", 2);
        mockCanMatchSequence(
            new Reply(true, bounds(0L, 30L)),
            new Reply(false, null),          // pruned — bounds must not influence anything
            new Reply(true, bounds(0L, 70L))
        );
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(t0, t1, t2),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(List.of(t2, t0), result.get());
    }

    /** Fail-open shards have no bounds, so they must not be promoted ahead of measured ones. */
    public void testTransportFailureKeepsTargetAndSortsItLast() {
        ExecutionTarget failing = target("idx", 0);
        ExecutionTarget healthy = target("idx", 1);
        mockCanMatchSequence(new Reply(new TransportException("connection lost")), new Reply(true, bounds(0L, 5L)));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(failing, healthy),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(List.of(healthy, failing), result.get());
    }

    /** The sort column has to reach the data node or it cannot fold anything. */
    public void testSortColumnForwardedInRequest() {
        mockCanMatchResponseWithBounds(true, bounds(1L, 2L));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(target("idx", 0)),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        ArgumentCaptor<TransportRequest> captor = ArgumentCaptor.forClass(TransportRequest.class);
        verify(transportService).sendRequest(
            any(DiscoveryNode.class),
            eq(AnalyticsCanMatchAction.NAME),
            captor.capture(),
            any(TransportResponseHandler.class)
        );
        AnalyticsCanMatchRequest sent = (AnalyticsCanMatchRequest) captor.getValue();
        assertEquals("@timestamp", sent.getSortColumn());
    }

    /** With no sort spec, no column is requested — the data node skips the fold entirely. */
    public void testNoSortColumnWhenSpecAbsent() {
        mockCanMatchResponse(true);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(target("idx", 0)),
            new byte[] { 1 },
            "datafusion",
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        ArgumentCaptor<TransportRequest> captor = ArgumentCaptor.forClass(TransportRequest.class);
        verify(transportService).sendRequest(
            any(DiscoveryNode.class),
            eq(AnalyticsCanMatchAction.NAME),
            captor.capture(),
            any(TransportResponseHandler.class)
        );
        assertNull(((AnalyticsCanMatchRequest) captor.getValue()).getSortColumn());
    }

    /** All pruned → force-keep still applies, and ordering must not disturb it. */
    public void testAllPrunedWithSortSpecStillKeepsFirstTarget() {
        ExecutionTarget first = target("idx", 0);
        mockCanMatchResponseWithBounds(false, null);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(first, target("idx", 1)),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(1, result.get().size());
        assertSame(first, result.get().get(0));
    }

    /** A matching shard with no statistics is kept but sorted after every measured shard. */
    public void testShardWithoutStatisticsIsKeptAndSortedLast() {
        ExecutionTarget noStats = target("idx", 0);
        ExecutionTarget measured = target("idx", 1);
        mockCanMatchSequence(new Reply(true, null), new Reply(true, bounds(0L, 10L)));
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(noStats, measured),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals(List.of(measured, noStats), result.get());
    }

    /** Every shard lacking bounds (e.g. a keyword sort) → no reorder, all kept, correct results. */
    public void testNoShardHasBoundsKeepsInputOrder() {
        ExecutionTarget t0 = target("idx", 0);
        ExecutionTarget t1 = target("idx", 1);
        ExecutionTarget t2 = target("idx", 2);
        mockCanMatchBoundsSequence(null, null, null);
        CanMatchPreFilterPhase phase = new CanMatchPreFilterPhase(transportService);
        AtomicReference<List<ExecutionTarget>> result = new AtomicReference<>();

        survivorsOf(
            phase,
            List.of(t0, t1, t2),
            new byte[] { 1 },
            "datafusion",
            descending(),
            ActionListener.wrap(result::set, e -> fail(e.getMessage()))
        );

        assertEquals("nothing to order by → input order preserved", List.of(t0, t1, t2), result.get());
    }

    // --- helpers ---

    /** {@link CanMatchPreFilterPhase#checkShards} with only the survivor list kept — what most tests assert on. */
    private static void survivorsOf(
        CanMatchPreFilterPhase phase,
        List<ExecutionTarget> targets,
        byte[] filterBytes,
        String backendId,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        survivorsOf(phase, targets, filterBytes, backendId, null, listener);
    }

    private static void survivorsOf(
        CanMatchPreFilterPhase phase,
        List<ExecutionTarget> targets,
        byte[] filterBytes,
        String backendId,
        SortSpec sortSpec,
        ActionListener<List<ExecutionTarget>> listener
    ) {
        phase.checkShards(
            targets,
            filterBytes,
            backendId,
            sortSpec,
            ActionListener.map(listener, CanMatchPreFilterPhase.ShardCheckResult::targets)
        );
    }

    private static SortSpec descending() {
        return new SortSpec("@timestamp", true, 10);
    }

    private static SortSpec ascending() {
        return new SortSpec("@timestamp", false, 10);
    }

    private static ShardSortBounds bounds(long min, long max) {
        return new ShardSortBounds(min, max, false, ShardSortBounds.VALUE_KIND_INT64);
    }

    /** One scripted can-match reply: either a response pair, or a transport failure. */
    private record Reply(boolean canMatch, ShardSortBounds bounds, TransportException failure) {
        Reply(boolean canMatch, ShardSortBounds bounds) {
            this(canMatch, bounds, null);
        }

        Reply(TransportException failure) {
            this(false, null, failure);
        }
    }

    @SuppressWarnings("unchecked")
    private void mockCanMatchResponseWithBounds(boolean canMatch, ShardSortBounds bounds) {
        doAnswer(inv -> {
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            handler.handleResponse(new AnalyticsCanMatchResponse(canMatch, bounds));
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                eq(AnalyticsCanMatchAction.NAME),
                any(TransportRequest.class),
                any(TransportResponseHandler.class)
            );
    }

    /** Scripts per-shard bounds in dispatch order; all shards match. */
    private void mockCanMatchBoundsSequence(ShardSortBounds... boundsPerShard) {
        Reply[] replies = new Reply[boundsPerShard.length];
        for (int i = 0; i < boundsPerShard.length; i++) {
            replies[i] = new Reply(true, boundsPerShard[i]);
        }
        mockCanMatchSequence(replies);
    }

    @SuppressWarnings("unchecked")
    private void mockCanMatchSequence(Reply... replies) {
        final int[] callCount = { 0 };
        doAnswer(inv -> {
            TransportResponseHandler<AnalyticsCanMatchResponse> handler = inv.getArgument(3);
            Reply reply = callCount[0] < replies.length ? replies[callCount[0]] : new Reply(true, null);
            callCount[0]++;
            if (reply.failure() != null) {
                handler.handleException(reply.failure());
            } else {
                handler.handleResponse(new AnalyticsCanMatchResponse(reply.canMatch(), reply.bounds()));
            }
            return null;
        }).when(transportService)
            .sendRequest(
                any(DiscoveryNode.class),
                eq(AnalyticsCanMatchAction.NAME),
                any(TransportRequest.class),
                any(TransportResponseHandler.class)
            );
    }

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
