/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.search.TotalHits;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchTask;
import org.opensearch.arrow.search.ArrowSearchPlugin;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.scan.ArrowScanBatchResponse;
import org.opensearch.arrow.search.scan.ArrowScanSpec;
import org.opensearch.arrow.search.scan.ArrowShardStreamRequest;
import org.opensearch.arrow.search.scan.ShardSummary;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.Transport;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArrowSearchExecutionTests extends OpenSearchTestCase {

    private static final List<Column> COLUMNS = List.of(new Column("l", Column.ColumnType.INT64, Set.of("long")));
    private static final Schema SCHEMA = Column.toSchema(COLUMNS);
    private static final Object END = new Object();
    /** The memory a relayed batch may take: a row of max_row_size (3MB), rounded up as Arrow allocates it. */
    private static final long BATCH_MEMORY = 4 << 20;

    private BufferAllocator root;
    private BufferAllocator dataNode;
    private BufferAllocator relay;
    private final CoordinatorBudget budget = new CoordinatorBudget(() -> Long.MAX_VALUE);
    private final CoordinatorBudget.Reservation reservation = budget.reserve(64 << 20, 64 << 20);
    private final FakeSink sink = new FakeSink();
    private final BlockingQueue<Opened> opened = new LinkedBlockingQueue<>();
    private final List<String> cancelledChildren = new CopyOnWriteArrayList<>();
    /** The nodes of the streams still opening, as the last cancel of the search's tasks sees them. */
    private volatile Supplier<Set<String>> openingNodes;
    private final List<Thread> relays = new CopyOnWriteArrayList<>();
    private SearchTask task;

    @Before
    public void createAllocators() {
        root = new RootAllocator();
        dataNode = root.newChildAllocator("data-node", 0, Long.MAX_VALUE);
        relay = root.newChildAllocator("relay", 0, Long.MAX_VALUE);
        task = new SearchTask(1, "transport", ArrowSearchAction.NAME, () -> "test", TaskId.EMPTY_TASK_ID, Map.of());
    }

    @After
    public void closeAllocators() throws Exception {
        for (Thread thread : relays) {
            thread.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse("relay " + thread + " did not end", thread.isAlive());
        }
        assertEquals("relayed batches are released", 0, relay.getAllocatedMemory());
        assertEquals("received batches are released", 0, dataNode.getAllocatedMemory());
        // Once the search ends, no relayed batch still holds its reservation.
        reservation.endSearch();
        assertTrue("relayed batches return the reservation", reservation.isReleased());
        assertEquals(0, budget.reserved());
        relay.close();
        dataNode.close();
        root.close();
    }

    /** A sink that keeps the rows it was sent, and can fail sends. */
    private static class FakeSink implements ArrowBatchSink {
        final List<Long> rows = new CopyOnWriteArrayList<>();
        final AtomicReference<Exception> failed = new AtomicReference<>();
        volatile Exception sendFailure;

        @Override
        public void sendSchema(Schema schema) {}

        @Override
        public void sendBatch(VectorSchemaRoot batch) throws Exception {
            try (batch) {
                if (sendFailure != null) {
                    throw sendFailure;
                }
                BigIntVector vector = (BigIntVector) batch.getVector(0);
                for (int i = 0; i < batch.getRowCount(); i++) {
                    rows.add(vector.get(i));
                }
            }
        }

        @Override
        public void complete(SearchResponse summary) {}

        @Override
        public void fail(Exception failure) {
            failed.compareAndSet(null, failure);
        }

        @Override
        public void reset() {}

        @Override
        public void setOnCancel(Runnable onCancel) {}
    }

    /** A shard stream whose batches the test scripts. */
    private class FakeStream implements StreamTransportResponse<ArrowScanBatchResponse> {
        final BlockingQueue<Object> items = new LinkedBlockingQueue<>();
        volatile boolean cancelled;
        volatile boolean closed;

        FakeStream batch(long... values) {
            items.add(new ArrowScanBatchResponse(rootOf(values), null));
            return this;
        }

        FakeStream last(long... values) {
            items.add(new ArrowScanBatchResponse(rootOf(values), new ShardSummary(values.length, false, false).encode()));
            items.add(END);
            return this;
        }

        FakeStream fail(RuntimeException e) {
            items.add(e);
            return this;
        }

        @Override
        public ArrowScanBatchResponse nextResponse() {
            final Object item;
            try {
                item = items.poll(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            assertNotNull("the test did not script the stream", item);
            if (item == END) {
                items.add(END);
                return null;
            }
            if (item instanceof RuntimeException e) {
                throw e;
            }
            return (ArrowScanBatchResponse) item;
        }

        @Override
        public void cancel(String reason, Throwable cause) {
            cancelled = true;
            items.add(new StreamException(StreamErrorCode.CANCELLED, reason));
        }

        @Override
        public void close() {
            closed = true;
            for (Object item : items) {
                if (item instanceof ArrowScanBatchResponse response) {
                    response.getRoot().close();
                }
            }
            items.clear();
        }
    }

    private record Opened(DiscoveryNode node, ArrowShardStreamRequest request, StreamTransportResponseHandler<
        ArrowScanBatchResponse> handler) {
        /** Delivers the stream on a thread of its own, as the transport does. */
        Thread deliver(FakeStream stream, List<Thread> threads) {
            Thread thread = Thread.ofVirtual().start(() -> handler.handleStreamResponse(stream));
            threads.add(thread);
            return thread;
        }
    }

    /** Records a shard's outcome, failing if it is reported twice. */
    private static final class ShardListener implements ActionListener<SearchPhaseResult> {
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(1);
        volatile ArrowShardResult result;
        volatile Exception failure;

        @Override
        public void onResponse(SearchPhaseResult response) {
            result = (ArrowShardResult) response;
            complete();
        }

        @Override
        public void onFailure(Exception e) {
            failure = e;
            complete();
        }

        private void complete() {
            assertEquals("a shard completes exactly once", 1, calls.incrementAndGet());
            done.countDown();
        }

        void await() throws InterruptedException {
            assertTrue("the shard did not complete", done.await(30, TimeUnit.SECONDS));
        }
    }

    private VectorSchemaRoot rootOf(long... values) {
        VectorSchemaRoot batch = VectorSchemaRoot.create(SCHEMA, dataNode);
        batch.allocateNew();
        BigIntVector vector = (BigIntVector) batch.getVector(0);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i]);
        }
        batch.setRowCount(values.length);
        return batch;
    }

    private ArrowSearchExecution execution(int maxOpenStreams, long rowLimit, boolean allowPartial) {
        return execution(maxOpenStreams, rowLimit, allowPartial, sink, COLUMNS);
    }

    private ArrowSearchExecution execution(
        int maxOpenStreams,
        long rowLimit,
        boolean allowPartial,
        ArrowBatchSink batchSink,
        List<Column> columns
    ) {
        ArrowScanSpec spec = new ArrowScanSpec(columns, 1 << 20, 64 << 10, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1));
        return new ArrowSearchExecution(
            task,
            batchSink,
            spec,
            this::open,
            ArrowSearchPlugin.EXECUTOR,
            Runnable::run,
            relay,
            reservation,
            BATCH_MEMORY,
            maxOpenStreams,
            () -> maxOpenStreams,
            rowLimit,
            allowPartial,
            (reason, opening) -> {
                cancelledChildren.add(reason);
                openingNodes = opening;
            }
        );
    }

    private void open(
        DiscoveryNode node,
        ArrowShardStreamRequest request,
        Task parent,
        StreamTransportResponseHandler<ArrowScanBatchResponse> handler
    ) {
        assertSame(task, parent);
        opened.add(new Opened(node, request, handler));
    }

    private Opened nextOpened() throws InterruptedException {
        Opened next = opened.poll(30, TimeUnit.SECONDS);
        assertNotNull("no stream was opened", next);
        return next;
    }

    private static DiscoveryNode node(String id, boolean servesStreams) {
        DiscoveryNode node = new DiscoveryNode(
            id,
            buildNewFakeTransportAddress(),
            servesStreams ? Map.of(ArrowSearchPlugin.NODE_ATTRIBUTE, "true") : Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        return servesStreams ? new DiscoveryNode(node, buildNewFakeTransportAddress()) : node;
    }

    private static SearchShardTarget target(int shard, String nodeId) {
        return new SearchShardTarget(nodeId, new ShardId(new Index("idx", "uuid"), shard), null, OriginalIndices.NONE);
    }

    private static Transport.Connection connection(DiscoveryNode node) {
        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getNode()).thenReturn(node);
        return connection;
    }

    private ShardListener start(ArrowSearchExecution execution, int shard, DiscoveryNode node) {
        ShardListener listener = new ShardListener();
        execution.executeOnShardTarget(task, target(shard, node.getId()), connection(node), mock(ShardSearchRequest.class), listener);
        return listener;
    }

    public void testRelayedBatchesHoldTheReservationUntilTheSinkClosesThem() throws Exception {
        List<VectorSchemaRoot> unread = new CopyOnWriteArrayList<>();
        FakeSink holdingSink = new FakeSink() {
            @Override
            public void sendBatch(VectorSchemaRoot batch) {
                // Hold the batch, as gRPC holds the batches its client has not read yet.
                unread.add(batch);
            }
        };
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true, holdingSink, COLUMNS);
        ShardListener shard = start(execution, 0, node("n1", true));
        nextOpened().deliver(new FakeStream().batch(1, 2).last(3), relays);
        shard.await();
        assertEquals(2, unread.size());

        assertTrue("the search ended, but its client has not read every batch", reservation.endSearch());
        // Each unread batch counts as the memory a batch may take.
        assertEquals(2 * BATCH_MEMORY, budget.reserved());
        unread.get(0).close();
        assertEquals(BATCH_MEMORY, budget.reserved());
        unread.get(1).close();
        assertEquals(0, budget.reserved());
    }

    public void testRelaysEveryBatchAndCountsRows() throws Exception {
        ArrowSearchExecution execution = execution(2, Long.MAX_VALUE, true);
        ShardListener first = start(execution, 0, node("n1", true));
        ShardListener second = start(execution, 1, node("n2", true));
        nextOpened().deliver(new FakeStream().batch(1, 2).last(3), relays);
        nextOpened().deliver(new FakeStream().last(4, 5), relays);
        first.await();
        second.await();
        assertEquals(3, first.result.rows());
        assertEquals(2, second.result.rows());
        assertEquals(Set.of(1L, 2L, 3L, 4L, 5L), Set.copyOf(sink.rows));

        InternalSearchResponse response = execution.buildResponse(List.of(first.result, second.result));
        assertEquals(new TotalHits(5, TotalHits.Relation.EQUAL_TO), response.hits().getTotalHits());
        assertEquals(0, response.hits().getHits().length);
        assertFalse(response.timedOut());
        assertNull(sink.failed.get());
        assertEquals(List.of(), cancelledChildren);
    }

    public void testFailureBeforeAnyRowTriesTheNextCopy() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener listener = start(execution, 0, node("n1", true));
        StreamException unavailable = new StreamException(StreamErrorCode.UNAVAILABLE, "shard closing");
        nextOpened().deliver(new FakeStream().batch().fail(unavailable), relays);
        listener.await();
        assertSame(unavailable, listener.failure);
        assertTrue(execution.shouldTryNextCopy(target(0, "n1"), unavailable));
        assertEquals(ArrowSearchExecution.Stop.NONE, execution.stopReason());
    }

    public void testFailureAfterRowsIsNeverRetriedAndFailsTheSearch() throws Exception {
        ArrowSearchExecution execution = execution(2, Long.MAX_VALUE, true);
        ShardListener failing = start(execution, 0, node("n1", true));
        ShardListener other = start(execution, 1, node("n2", true));
        Opened first = nextOpened();
        Opened second = nextOpened();
        FakeStream otherStream = new FakeStream().batch(10);
        second.deliver(otherStream, relays);
        StreamException crash = new StreamException(StreamErrorCode.INTERNAL, "node left");
        first.deliver(new FakeStream().batch(1, 2).fail(crash), relays);
        failing.await();
        assertSame(crash, failing.failure);
        assertFalse("rows already reached the client", execution.shouldTryNextCopy(target(0, "n1"), crash));

        execution.onShardGroupFailure(0, target(0, "n1"), crash);
        assertEquals(ArrowSearchExecution.Stop.FAILED, execution.stopReason());
        other.await();
        assertTrue("the other streams are cancelled", otherStream.cancelled);
        assertSame(crash, sink.failed.get());
        assertEquals(1, cancelledChildren.size());
        assertSame(crash, expectThrows(StreamException.class, () -> execution.buildResponse(List.of())));
        assertSame(crash, execution.failureFor(new IllegalStateException("partial shard failures")));
    }

    public void testBadRequestFailsTheSearchWithoutRetry() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener listener = start(execution, 0, node("n1", true));
        StreamException badRequest = new StreamException(StreamErrorCode.INVALID_ARGUMENT, "field [tags] has [2] values");
        nextOpened().handler().handleException(badRequest);
        listener.await();
        assertFalse(execution.shouldTryNextCopy(target(0, "n1"), badRequest));
        execution.onShardGroupFailure(0, target(0, "n1"), badRequest);
        assertEquals(ArrowSearchExecution.Stop.FAILED, execution.stopReason());
        assertSame(badRequest, sink.failed.get());
    }

    public void testPartialResultsDecideWhetherAShardFailureEndsTheSearch() throws Exception {
        StreamException unavailable = new StreamException(StreamErrorCode.UNAVAILABLE, "no copy left");
        ArrowSearchExecution lenient = execution(1, Long.MAX_VALUE, true);
        lenient.onShardGroupFailure(0, target(0, "n1"), unavailable);
        assertEquals(ArrowSearchExecution.Stop.NONE, lenient.stopReason());

        ArrowSearchExecution strict = execution(1, Long.MAX_VALUE, false);
        strict.onShardGroupFailure(0, target(0, "n1"), unavailable);
        assertEquals(ArrowSearchExecution.Stop.FAILED, strict.stopReason());
    }

    public void testRowLimitCutsTheStreamThatReachesIt() throws Exception {
        ArrowSearchExecution execution = execution(2, 5, true);
        ShardListener first = start(execution, 0, node("n1", true));
        ShardListener second = start(execution, 1, node("n2", true));
        Opened a = nextOpened();
        Opened b = nextOpened();
        a.deliver(new FakeStream().last(1, 2, 3), relays).join();
        FakeStream streamB = new FakeStream().batch(4, 5, 6, 7);
        b.deliver(streamB, relays);
        first.await();
        second.await();
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), sink.rows);
        assertEquals(ArrowSearchExecution.Stop.ROW_LIMIT, execution.stopReason());
        assertTrue(streamB.cancelled);
        assertFalse("shard 0 finished before the limit", first.result.truncated());
        assertTrue(second.result.truncated());
        assertEquals(2, second.result.rows());

        InternalSearchResponse response = execution.buildResponse(List.of(first.result, second.result));
        assertEquals(new TotalHits(5, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), response.hits().getTotalHits());
        assertTrue(response.terminatedEarly());
        assertNull("reaching the limit is a success", sink.failed.get());
        assertEquals("shard streams still opening are cancelled through their tasks", 1, cancelledChildren.size());
    }

    public void testRowLimitCutsListColumnsWithTheirValues() throws Exception {
        List<Column> listColumns = List.of(new Column("tags", Column.ColumnType.INT64, Set.of("long"), true));
        AtomicReference<VectorSchemaRoot> relayedBatch = new AtomicReference<>();
        FakeSink capturing = new FakeSink() {
            @Override
            public void sendBatch(VectorSchemaRoot batch) {
                relayedBatch.set(batch);
            }
        };
        ArrowSearchExecution execution = execution(1, 2, true, capturing, listColumns);
        ShardListener listener = start(execution, 0, node("n1", true));

        VectorSchemaRoot batch = VectorSchemaRoot.create(Column.toSchema(listColumns), dataNode);
        batch.allocateNew();
        ListVector tags = (ListVector) batch.getVector(0);
        BigIntVector items = (BigIntVector) tags.getDataVector();
        long item = 0;
        for (int row = 0; row < 4; row++) {
            int start = tags.startNewValue(row);
            for (int v = 0; v < 3; v++) {
                items.setSafe(start + v, item++);
            }
            tags.endValue(row, 3);
        }
        batch.setRowCount(4);
        FakeStream stream = new FakeStream();
        stream.items.add(new ArrowScanBatchResponse(batch, null));
        nextOpened().deliver(stream, relays).join();
        listener.await();

        try (VectorSchemaRoot relayed = relayedBatch.get()) {
            assertEquals(2, relayed.getRowCount());
            ListVector relayedTags = (ListVector) relayed.getVector(0);
            assertEquals("only the values of the rows kept remain", 6, relayedTags.getDataVector().getValueCount());
            assertEquals(List.of(0L, 1L, 2L), relayedTags.getObject(0));
            assertEquals(List.of(3L, 4L, 5L), relayedTags.getObject(1));
        }
    }

    public void testRelayedShardFailureFailsTheSearchAtOnce() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener listener = start(execution, 0, node("n1", true));
        StreamException crash = new StreamException(StreamErrorCode.INTERNAL, "node left");
        nextOpened().deliver(new FakeStream().batch(1).fail(crash), relays);
        listener.await();
        // The search fails when the shard completes, without waiting for the shard group to fail.
        assertEquals(ArrowSearchExecution.Stop.FAILED, execution.stopReason());
        assertSame(crash, sink.failed.get());
        execution.stop(ArrowSearchExecution.Stop.ROW_LIMIT, null);
        assertFalse("a relayed shard is never retried", execution.shouldTryNextCopy(target(0, "n1"), crash));
    }

    public void testRowLimitAbandonsQueuedShards() throws Exception {
        ArrowSearchExecution execution = execution(1, 3, true);
        ShardListener open = start(execution, 0, node("n1", true));
        ShardListener queued = start(execution, 1, node("n2", true));
        ShardListener alsoQueued = start(execution, 2, node("n1", true));
        nextOpened().deliver(new FakeStream().batch(1, 2, 3, 4, 5), relays);
        open.await();
        queued.await();
        alsoQueued.await();
        assertEquals(List.of(1L, 2L, 3L), sink.rows);
        assertTrue(open.result.truncated());
        assertTrue("a shard the limit stopped before it streamed succeeds", queued.result.truncated());
        assertEquals(0, queued.result.rows());
        assertTrue(alsoQueued.result.truncated());
        assertNull("queued shards never open", opened.poll(100, TimeUnit.MILLISECONDS));
        ShardListener late = start(execution, 3, node("n3", true));
        late.await();
        assertTrue("shards submitted after the limit succeed without opening", late.result.truncated());
    }

    public void testShardsQueueBehindTheStreamLimit() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener first = start(execution, 0, node("n1", true));
        ShardListener second = start(execution, 1, node("n2", true));
        Opened a = nextOpened();
        assertNull("the second shard waits for a free stream", opened.poll(100, TimeUnit.MILLISECONDS));
        a.deliver(new FakeStream().last(1), relays);
        first.await();
        nextOpened().deliver(new FakeStream().last(2), relays);
        second.await();
        assertEquals(List.of(1L, 2L), sink.rows);
    }

    public void testCancellationStopsOpenAndQueuedShards() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener open = start(execution, 0, node("n1", true));
        ShardListener queued = start(execution, 1, node("n2", true));
        FakeStream stream = new FakeStream().batch(1);
        nextOpened().deliver(stream, relays);

        assertTrue(execution.stop(ArrowSearchExecution.Stop.CANCELLED, null));
        assertFalse("the first reason wins", execution.stop(ArrowSearchExecution.Stop.FAILED, new IllegalStateException()));
        open.await();
        queued.await();
        assertTrue(stream.cancelled);
        assertNotNull(open.failure);
        assertTrue(queued.failure.toString(), queued.failure instanceof TaskCancelledException);
        assertTrue(sink.failed.get() instanceof TaskCancelledException);
        assertEquals(1, cancelledChildren.size());
        expectThrows(TaskCancelledException.class, () -> execution.buildResponse(List.of()));
        assertNull("no stream opens after the search stopped", opened.poll(100, TimeUnit.MILLISECONDS));
    }

    public void testStreamArrivingAfterTheStopIsCancelledAtOnce() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener listener = start(execution, 0, node("n1", true));
        Opened pending = nextOpened();
        execution.stop(ArrowSearchExecution.Stop.CANCELLED, null);
        FakeStream late = new FakeStream().batch(1, 2).last(3);
        pending.deliver(late, relays).join();
        listener.await();
        assertTrue(late.cancelled);
        assertTrue(late.closed);
        assertEquals(List.of(), sink.rows);
    }

    public void testStreamStillOpeningWhenTheSearchStopsIsCancelledUntilItArrives() throws Exception {
        ArrowSearchExecution execution = execution(2, 1, true);
        ShardListener first = start(execution, 0, node("n1", true));
        ShardListener opening = start(execution, 1, node("n2", true));
        Opened a = nextOpened();
        Opened b = nextOpened();
        a.deliver(new FakeStream().batch(1), relays);
        first.await();
        assertEquals(ArrowSearchExecution.Stop.ROW_LIMIT, execution.stopReason());
        assertEquals(1, cancelledChildren.size());
        assertEquals("the stream on n2 sent no batch yet, so only its task can be cancelled", Set.of("n2"), openingNodes.get());

        FakeStream late = new FakeStream().batch(2).last(3);
        b.deliver(late, relays).join();
        opening.await();
        assertTrue(late.cancelled);
        assertEquals(Set.of(), openingNodes.get());
        assertEquals(List.of(1L), sink.rows);
    }

    public void testStreamArrivingAfterTheStopCompletesThoughItsCancelFails() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener listener = start(execution, 0, node("n1", true));
        ShardListener queued = start(execution, 1, node("n1", true));
        Opened pending = nextOpened();
        execution.stop(ArrowSearchExecution.Stop.CANCELLED, null);
        FakeStream late = new FakeStream() {
            @Override
            public void cancel(String reason, Throwable cause) {
                // The stream transport closes a cancelled stream, and the close can fail.
                super.cancel(reason, cause);
                close();
                throw new StreamException(StreamErrorCode.INTERNAL, "failed to close the flight stream");
            }
        }.batch(1).last(2);
        pending.deliver(late, relays).join();
        listener.await();
        queued.await();
        assertTrue(late.cancelled);
        assertNotNull(listener.failure);
        assertEquals(Set.of(), openingNodes.get());
        assertEquals(List.of(), sink.rows);
    }

    public void testNodeWithoutShardStreamsFailsOverBeforeOpening() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        DiscoveryNode old = new DiscoveryNode(
            "old-name",
            "old",
            buildNewFakeTransportAddress(),
            Map.of("zone", "z1"),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        ShardListener listener = start(execution, 0, old);
        listener.await();
        StreamException failure = (StreamException) listener.failure;
        assertEquals(StreamErrorCode.UNAVAILABLE, failure.getErrorCode());
        // The failure reaches the client, which must not learn the node's addresses and attributes.
        assertEquals("node [old-name][old] does not serve Arrow search shard streams", failure.getMessage());
        assertTrue(execution.shouldTryNextCopy(target(0, "old"), failure));
        assertNull(opened.poll(100, TimeUnit.MILLISECONDS));
    }

    public void testClientThatStopsReadingFailsTheSearch() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener listener = start(execution, 0, node("n1", true));
        OpenSearchStatusException idle = new OpenSearchStatusException("the client did not read", RestStatus.REQUEST_TIMEOUT);
        sink.sendFailure = idle;
        nextOpened().deliver(new FakeStream().batch(1).last(2), relays);
        listener.await();
        assertEquals(ArrowSearchExecution.Stop.FAILED, execution.stopReason());
        assertSame(idle, execution.failureFor(new IllegalStateException()));
    }

    public void testStreamEndingWithoutSummaryFails() throws Exception {
        ArrowSearchExecution execution = execution(1, Long.MAX_VALUE, true);
        ShardListener listener = start(execution, 0, node("n1", true));
        FakeStream stream = new FakeStream().batch(1);
        stream.items.add(END);
        nextOpened().deliver(stream, relays);
        listener.await();
        assertEquals(StreamErrorCode.INTERNAL, ((StreamException) listener.failure).getErrorCode());
    }

    public void testManyShardsCompleteOnceUnderConcurrentStops() throws Exception {
        ArrowSearchExecution execution = execution(randomIntBetween(1, 4), randomLongBetween(1, 50), true);
        int shards = randomIntBetween(5, 20);
        List<ShardListener> listeners = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            listeners.add(start(execution, s, node("n" + (s % 3), true)));
        }
        Random random = new Random(randomLong());
        Thread feeder = Thread.ofVirtual().start(() -> {
            try {
                Opened next;
                while ((next = opened.poll(500, TimeUnit.MILLISECONDS)) != null) {
                    FakeStream stream = new FakeStream();
                    for (int b = random.nextInt(4); b > 0; b--) {
                        stream.batch(random.nextInt(100), random.nextInt(100));
                    }
                    if (random.nextBoolean()) {
                        stream.last(1);
                    } else {
                        stream.fail(new StreamException(StreamErrorCode.UNAVAILABLE, "gone"));
                    }
                    next.deliver(stream, relays);
                }
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        if (randomBoolean()) {
            execution.stop(ArrowSearchExecution.Stop.CANCELLED, null);
        }
        for (ShardListener listener : listeners) {
            listener.await();
        }
        feeder.join();
    }
}
