/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.grpc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.search.Column;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.StreamPayload;
import org.opensearch.transport.grpc.spi.StreamSearchResponseChannel;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class GrpcArrowBatchSinkTests extends OpenSearchTestCase {

    private static final Schema SCHEMA = Column.toSchema(List.of(new Column("l", Column.ColumnType.INT64, Set.of("long"))));

    /** A channel whose readiness and cancellation the test controls. */
    private static final class FakeChannel implements StreamSearchResponseChannel {
        final AtomicBoolean ready = new AtomicBoolean();
        final AtomicBoolean cancelled = new AtomicBoolean();
        final List<StreamPayload> sent = new CopyOnWriteArrayList<>();
        final AtomicReference<SearchResponse> completed = new AtomicReference<>();
        final AtomicReference<Exception> failed = new AtomicReference<>();
        final AtomicInteger resets = new AtomicInteger();
        volatile Runnable onReady;
        volatile Runnable onCancel;

        @Override
        public boolean isReady() {
            return ready.get() && cancelled.get() == false;
        }

        @Override
        public void setOnReadyHandler(Runnable onReady) {
            this.onReady = onReady;
        }

        @Override
        public void setOnCancelHandler(Runnable onCancel) {
            this.onCancel = onCancel;
            if (cancelled.get()) {
                onCancel.run();
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public void sendPayload(StreamPayload payload) {
            sent.add(payload);
        }

        @Override
        public void complete(SearchResponse summary) {
            completed.compareAndSet(null, summary);
        }

        @Override
        public void fail(Exception failure) {
            failed.compareAndSet(null, failure);
        }

        @Override
        public void reset() {
            resets.incrementAndGet();
        }

        void becomeReady() {
            ready.set(true);
            onReady.run();
        }

        void cancelByClient() {
            cancelled.set(true);
            onCancel.run();
        }
    }

    private BufferAllocator allocator;
    private FakeChannel channel;
    private final List<Runnable> forked = new CopyOnWriteArrayList<>();
    private volatile TimeValue idleTimeout = TimeValue.timeValueMinutes(1);
    private GrpcArrowBatchSink sink;

    @Before
    public void createSink() {
        allocator = new RootAllocator();
        channel = new FakeChannel();
        sink = new GrpcArrowBatchSink(channel, () -> idleTimeout, forked::add);
    }

    @After
    public void closeAllocator() {
        channel.sent.forEach(StreamPayload::release);
        assertEquals("every batch is released", 0, allocator.getAllocatedMemory());
        allocator.close();
    }

    private VectorSchemaRoot batch() {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        root.allocateNew();
        root.setRowCount(1);
        return root;
    }

    private Thread sendInBackground(AtomicReference<Exception> failure, CountDownLatch done) {
        return Thread.ofVirtual().start(() -> {
            try {
                sink.sendBatch(batch());
            } catch (Exception e) {
                failure.set(e);
            } finally {
                done.countDown();
            }
        });
    }

    public void testSchemaIsSentOnceTheCallIsReady() throws Exception {
        channel.ready.set(true);
        sink.sendSchema(SCHEMA);
        assertEquals(1, channel.sent.size());
    }

    public void testBatchWaitsUntilTheCallIsReady() throws Exception {
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread producer = sendInBackground(failure, done);

        assertFalse("must not send before the call is ready", done.await(300, TimeUnit.MILLISECONDS));
        assertEquals(0, channel.sent.size());

        channel.becomeReady();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        producer.join();
        assertNull(failure.get());
        assertEquals(1, channel.sent.size());
    }

    public void testClientThatStopsReadingTimesOut() throws Exception {
        idleTimeout = TimeValue.timeValueMillis(100);
        OpenSearchStatusException e = expectThrows(OpenSearchStatusException.class, () -> sink.sendBatch(batch()));
        assertEquals(RestStatus.REQUEST_TIMEOUT, e.status());
        // Later senders fail at once instead of each waiting for the timeout again.
        channel.ready.set(true);
        assertSame(e, expectThrows(OpenSearchStatusException.class, () -> sink.sendBatch(batch())));
        assertEquals(0, channel.sent.size());
    }

    public void testFailWakesWaitingSendersAndReleasesTheirBatches() throws Exception {
        int producers = randomIntBetween(1, 4);
        List<AtomicReference<Exception>> failures = new ArrayList<>();
        CountDownLatch done = new CountDownLatch(producers);
        for (int i = 0; i < producers; i++) {
            AtomicReference<Exception> failure = new AtomicReference<>();
            failures.add(failure);
            sendInBackground(failure, done);
        }
        assertFalse(done.await(200, TimeUnit.MILLISECONDS));

        IllegalStateException cause = new IllegalStateException("shard failed");
        sink.fail(cause);

        assertTrue(done.await(10, TimeUnit.SECONDS));
        for (AtomicReference<Exception> failure : failures) {
            assertSame(cause, failure.get());
        }
        assertSame(cause, channel.failed.get());
        assertEquals(0, channel.sent.size());
    }

    public void testBatchAfterCompleteIsReleased() throws Exception {
        channel.ready.set(true);
        sink.sendSchema(SCHEMA);
        sink.complete(null);
        expectThrows(IllegalStateException.class, () -> sink.sendBatch(batch()));
        assertEquals(0, allocator.getAllocatedMemory());
        assertNull(channel.failed.get());
    }

    public void testResetResetsTheCallAndEndsSends() throws Exception {
        channel.ready.set(true);
        sink.sendSchema(SCHEMA);
        sink.complete(null);
        sink.reset();
        assertEquals(1, channel.resets.get());
        expectThrows(IllegalStateException.class, () -> sink.sendBatch(batch()));
    }

    public void testSummaryOfASearchThatNeverSentItsSchemaFails() {
        // For example, the security plugin's filter-level document-level security ran the request as a regular search.
        sink.complete(null);
        OpenSearchStatusException e = (OpenSearchStatusException) channel.failed.get();
        assertNotNull(e);
        assertEquals(RestStatus.NOT_IMPLEMENTED, e.status());
        assertNull(channel.completed.get());
    }

    public void testClientCancellationEndsSendsAndNotifiesOffTheGrpcThread() throws Exception {
        AtomicInteger notified = new AtomicInteger();
        sink.setOnCancel(notified::incrementAndGet);
        channel.cancelByClient();
        assertEquals("the callback must not run on the gRPC thread", 0, notified.get());
        assertEquals(1, forked.size());
        forked.forEach(Runnable::run);
        assertEquals(1, notified.get());

        channel.ready.set(true);
        expectThrows(TaskCancelledException.class, () -> sink.sendBatch(batch()));
    }

    public void testCallbackRegisteredAfterCancellationRuns() {
        channel.cancelByClient();
        AtomicInteger notified = new AtomicInteger();
        sink.setOnCancel(notified::incrementAndGet);
        forked.forEach(Runnable::run);
        assertEquals(1, notified.get());
    }

    public void testCancellationBeforeTheSinkExistsIsSeen() {
        FakeChannel cancelledChannel = new FakeChannel();
        cancelledChannel.cancelled.set(true);
        GrpcArrowBatchSink lateSink = new GrpcArrowBatchSink(cancelledChannel, () -> idleTimeout, Runnable::run);
        AtomicInteger notified = new AtomicInteger();
        lateSink.setOnCancel(notified::incrementAndGet);
        assertEquals(1, notified.get());
        expectThrows(TaskCancelledException.class, () -> lateSink.sendBatch(batch()));
    }
}
