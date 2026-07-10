/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link StreamTransportService.LocalStreamChannel} — the in-process streaming channel
 * used by the local-node streaming optimization. Exercises the producer/consumer handoff, ordering,
 * completion, error propagation, and the bounded-queue backpressure directly, without a cluster.
 *
 * <p>All producer work runs on a single-thread {@link ExecutorService} that is shut down and awaited in
 * {@code tearDown}, so no threads leak past a test (OpenSearchTestCase's thread-leak detector fails the
 * suite otherwise).
 */
public class LocalStreamChannelTests extends OpenSearchTestCase {

    private ExecutorService producerExec;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        producerExec = Executors.newSingleThreadExecutor();
    }

    @Override
    public void tearDown() throws Exception {
        producerExec.shutdownNow();
        assertTrue("producer thread must terminate", producerExec.awaitTermination(10, TimeUnit.SECONDS));
        super.tearDown();
    }

    /** Minimal TransportResponse carrying an int so batches are distinguishable and order-checkable. */
    private static final class IntResponse extends TransportResponse {
        final int value;

        IntResponse(int value) {
            this.value = value;
        }

        @Override
        public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) {}
    }

    private DiscoveryNode node() {
        return new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
    }

    /** Default queue depth matching {@link StreamTransportService#STREAM_TRANSPORT_LOCAL_QUEUE_DEPTH_SETTING}. */
    private static final int DEFAULT_DEPTH = 4;

    private StreamTransportService.LocalStreamChannel newChannel() {
        return newChannel(DEFAULT_DEPTH);
    }

    private StreamTransportService.LocalStreamChannel newChannel(int queueDepth) {
        return new StreamTransportService.LocalStreamChannel(node(), 1L, "internal:test/stream", queueDepth);
    }

    public void testBatchesDrainInOrderThenEndOfStream() throws Exception {
        StreamTransportService.LocalStreamChannel channel = newChannel();
        StreamTransportResponse<IntResponse> stream = channel.responseStream();

        Future<?> producer = producerExec.submit(() -> {
            for (int i = 0; i < 3; i++) {
                channel.sendResponseBatch(new IntResponse(i));
            }
            channel.completeStream();
        });

        List<Integer> got = new ArrayList<>();
        IntResponse r;
        while ((r = stream.nextResponse()) != null) {
            got.add(r.value);
        }
        producer.get(10, TimeUnit.SECONDS);
        assertEquals(List.of(0, 1, 2), got);
        assertNull("stream is exhausted", stream.nextResponse());
    }

    public void testErrorPropagatesToConsumer() throws Exception {
        StreamTransportService.LocalStreamChannel channel = newChannel();
        StreamTransportResponse<IntResponse> stream = channel.responseStream();

        Future<?> producer = producerExec.submit(() -> {
            channel.sendResponseBatch(new IntResponse(42));
            channel.sendResponse(new IllegalStateException("boom"));
        });

        assertEquals(42, stream.nextResponse().value);
        TransportException te = expectThrows(TransportException.class, stream::nextResponse);
        assertTrue("error must surface to the consumer", te.getCause() != null || te.getMessage().contains("boom"));
        producer.get(10, TimeUnit.SECONDS);
    }

    public void testBackpressureBlocksProducerWhenQueueFull() throws Exception {
        // Default depth (4); a producer sending 8 with no consumer must block after filling the queue,
        // then unblock and complete once the consumer drains. Bounded waits everywhere so a genuine
        // deadlock surfaces as a test failure quickly rather than hanging to the suite timeout.
        assertBackpressureBlocksAtDepth(DEFAULT_DEPTH, 8);
    }

    public void testConfiguredQueueDepthGovernsBackpressure() throws Exception {
        // The configured depth is what bounds in-flight batches: depth 1 blocks the producer after a
        // single un-drained batch; depth 6 lets it stage 6 before blocking. Proves the setting value
        // (not a hardcoded constant) drives the backpressure point.
        assertBackpressureBlocksAtDepth(1, 5);
        assertBackpressureBlocksAtDepth(6, 12);
    }

    /**
     * A producer sending {@code total} batches with no consumer must block once {@code depth} batches
     * are queued, then unblock and deliver all of them once the consumer drains.
     */
    private void assertBackpressureBlocksAtDepth(int depth, int total) throws Exception {
        StreamTransportService.LocalStreamChannel channel = newChannel(depth);
        AtomicInteger sent = new AtomicInteger(0);
        Future<?> producer = producerExec.submit(() -> {
            for (int i = 0; i < total; i++) {
                channel.sendResponseBatch(new IntResponse(i));
                sent.incrementAndGet();
            }
            channel.completeStream();
        });

        // Producer fills the bounded queue up to `depth` and blocks well short of `total`.
        assertBusy(() -> assertTrue("producer should enqueue up to depth " + depth, sent.get() >= depth), 5, TimeUnit.SECONDS);
        Thread.sleep(200);
        int blockedAt = sent.get();
        assertTrue(
            "producer must block once the depth-" + depth + " queue is full (sent=" + blockedAt + " < " + total + ")",
            blockedAt < total
        );
        // It must not have staged more than the queue can hold (depth) plus the one it is blocked on.
        assertTrue("producer must not stage beyond depth+1 (sent=" + blockedAt + ", depth=" + depth + ")", blockedAt <= depth + 1);

        // Drain — the producer unblocks and finishes.
        StreamTransportResponse<IntResponse> stream = channel.responseStream();
        int count = 0;
        while (stream.nextResponse() != null) {
            count++;
        }
        producer.get(10, TimeUnit.SECONDS);
        assertEquals("all batches drain once the consumer reads", total, count);
        assertEquals(total, sent.get());
    }

    public void testConsumerCancelStopsStreamAndDropsBatches() throws IOException {
        StreamTransportService.LocalStreamChannel channel = newChannel();
        StreamTransportResponse<IntResponse> stream = channel.responseStream();

        channel.sendResponseBatch(new IntResponse(1));
        channel.sendResponseBatch(new IntResponse(2));
        stream.cancel("consumer done", null);

        assertNull("no delivery after cancel", stream.nextResponse());
        channel.sendResponseBatch(new IntResponse(3)); // dropped, must not throw
        stream.close();
    }

    public void testSingleSendResponseDeliversOneBatchThenEnds() throws Exception {
        StreamTransportService.LocalStreamChannel channel = newChannel();
        StreamTransportResponse<IntResponse> stream = channel.responseStream();

        Future<?> producer = producerExec.submit(() -> channel.sendResponse(new IntResponse(7)));

        assertEquals(7, stream.nextResponse().value);
        assertNull("sendResponse(TransportResponse) completes the stream", stream.nextResponse());
        producer.get(10, TimeUnit.SECONDS);
    }

    /** A TransportResponse whose buffers must be freed; records whether the channel closed it. */
    private static final class CloseableResponse extends TransportResponse implements java.io.Closeable {
        final AtomicInteger closes;

        CloseableResponse(AtomicInteger closes) {
            this.closes = closes;
        }

        @Override
        public void writeTo(org.opensearch.core.common.io.stream.StreamOutput out) {}

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    public void testCancelReleasesUndeliveredCloseableBatches() {
        StreamTransportService.LocalStreamChannel channel = newChannel();
        StreamTransportResponse<CloseableResponse> stream = channel.responseStream();
        AtomicInteger closes = new AtomicInteger(0);

        channel.sendResponseBatch(new CloseableResponse(closes));
        channel.sendResponseBatch(new CloseableResponse(closes));
        // Consumer never drains these; cancel must free their off-heap buffers so they don't leak.
        stream.cancel("consumer abandoned", null);
        assertEquals("both undelivered batches must be closed on cancel", 2, closes.get());

        // A batch the producer sends after cancel is also released immediately, not queued.
        channel.sendResponseBatch(new CloseableResponse(closes));
        assertEquals("post-cancel batch must be released too", 3, closes.get());
    }

    public void testCloseReleasesUndrainedCloseableBatches() throws IOException {
        StreamTransportService.LocalStreamChannel channel = newChannel();
        StreamTransportResponse<CloseableResponse> stream = channel.responseStream();
        AtomicInteger closes = new AtomicInteger(0);

        channel.sendResponseBatch(new CloseableResponse(closes));
        channel.sendResponseBatch(new CloseableResponse(closes));
        // Consumer takes one (and owns closing it itself — not counted here), then closes the stream;
        // the remaining undrained batch must be released by close().
        CloseableResponse taken = stream.nextResponse();
        assertNotNull(taken);
        stream.close();
        assertEquals("the one undrained batch must be closed by stream.close()", 1, closes.get());
    }

    public void testDeliveredBatchIsNotClosedByChannel() throws IOException {
        // A batch the consumer takes is the consumer's to close; the channel must not close it,
        // otherwise ownership double-frees. Draining then completing leaves the taken batch untouched.
        StreamTransportService.LocalStreamChannel channel = newChannel();
        StreamTransportResponse<CloseableResponse> stream = channel.responseStream();
        AtomicInteger closes = new AtomicInteger(0);

        channel.sendResponseBatch(new CloseableResponse(closes));
        channel.completeStream();

        CloseableResponse taken = stream.nextResponse();
        assertNotNull(taken);
        assertNull("end of stream", stream.nextResponse());
        stream.close();
        assertEquals("channel must not close a batch the consumer already took", 0, closes.get());
    }

    public void testCancelDuringProducerPutReleasesStrandedBatch() throws Exception {
        // Exercises the race window: producer passes the consumerCancelled pre-check in
        // sendResponseBatch and enqueues a batch, then cancel() drains the queue. The producer's
        // post-put re-check must detect the cancel and release the stranded batch.
        StreamTransportService.LocalStreamChannel channel = newChannel(1);
        StreamTransportResponse<CloseableResponse> stream = channel.responseStream();
        AtomicInteger closes = new AtomicInteger(0);

        // Fill the queue to capacity so the next put will block until we drain.
        CloseableResponse firstBatch = new CloseableResponse(closes);
        channel.sendResponseBatch(firstBatch);

        // On a separate thread, send another batch — it will block on the full queue.
        CloseableResponse raceBatch = new CloseableResponse(closes);
        Future<?> producer = producerExec.submit(() -> channel.sendResponseBatch(raceBatch));

        // Give the producer thread time to enter queue.put() and block.
        Thread.sleep(200);

        // Cancel: drains the queue (releasing firstBatch). The producer's put then succeeds, but
        // the post-put re-check sees consumerCancelled and removes+releases raceBatch.
        stream.cancel("test cancel", null);

        producer.get(10, TimeUnit.SECONDS);
        // firstBatch released by cancel's drain, raceBatch released by producer's post-put check.
        assertEquals("both batches must be released", 2, closes.get());
    }

    public void testErrorOverridesPendingCompletion() throws Exception {
        // completeStream() wins the CAS and blocks on a full queue. A subsequent error must still
        // surface to the consumer rather than being silently lost.
        StreamTransportService.LocalStreamChannel channel = newChannel(1);
        StreamTransportResponse<IntResponse> stream = channel.responseStream();

        // Fill the queue so completeStream() will block.
        channel.sendResponseBatch(new IntResponse(1));

        // completeStream() on the producer thread — blocks because queue is full (depth=1).
        Future<?> completer = producerExec.submit(channel::completeStream);
        Thread.sleep(200);

        // Fire an error while completion is pending. enqueueTerminalError force-drains to insert the
        // error sentinel, so the original batch may be dropped — that's the error-path contract
        // (surfacing the error promptly takes priority over delivering remaining batches).
        channel.sendResponse(new IllegalStateException("late error"));

        // The consumer must see the error (possibly after the batch, or immediately if the batch was
        // dropped by the force-drain). Drain until we hit the terminal.
        TransportException te = null;
        try {
            IntResponse r;
            while ((r = stream.nextResponse()) != null) {
                // May or may not see the original batch depending on timing.
            }
            fail("expected TransportException from the error override");
        } catch (TransportException e) {
            te = e;
        }
        assertNotNull("error must surface to the consumer", te);
        assertTrue(
            "error message must reference the late error",
            te.getMessage().contains("late error") || (te.getCause() != null && te.getCause().getMessage().contains("late error"))
        );
        completer.get(10, TimeUnit.SECONDS);
    }
}
