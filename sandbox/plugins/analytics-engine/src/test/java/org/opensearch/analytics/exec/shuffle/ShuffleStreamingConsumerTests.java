/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager.AdmitResult;
import org.opensearch.analytics.spi.CloseableIterator;
import org.opensearch.analytics.spi.ShuffleSlots;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pipelined-shuffle consumer semantics: the drain runs CONCURRENTLY with producers instead of after a
 * barrier, so residency is bounded by the in-flight window rather than by partition size.
 *
 * <p>These pin the properties that replaced the old buffer-all invariants:
 * <ul>
 *   <li>a drain can start and consume before any sender has reported {@code isLast};</li>
 *   <li>the EOF sentinel — not a latch — is what ends the stream, including for an empty partition;</li>
 *   <li>a full in-flight window applies backpressure as a RETRYABLE reject (never a blocked transport
 *       thread, never a silent drop);</li>
 *   <li>data admitted after end-of-stream fails LOUD (it would otherwise be lost rows);</li>
 *   <li>a terminal/cancel unblocks a parked drain and makes it fail rather than report a clean end of
 *       stream (which would let a join emit results from truncated input).</li>
 * </ul>
 */
public class ShuffleStreamingConsumerTests extends OpenSearchTestCase {

    private static final String Q = "q-stream";
    private static final String LEFT = ShuffleSlots.LEFT;

    private static byte[] chunk(int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (i % 127);
        }
        return b;
    }

    /**
     * The core behavioural change: chunks are readable BEFORE any {@code isLast}. Under the old
     * barrier model {@code awaitReady} had to return first, which is exactly what forced the whole
     * partition to be resident.
     */
    public void testDrainYieldsChunksBeforeAnySenderIsDone() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(2, -1);

        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(16)));

        try (CloseableIterator<byte[]> it = buf.drain(LEFT, 5_000)) {
            assertTrue("chunk must be visible without waiting for isLast", it.hasNext());
            assertEquals(16, it.next().length);
        }
        // No sender has completed, so the stream is deliberately NOT terminated yet.
        assertFalse(buf.isEofEnqueued(LEFT));
    }

    /** All declared senders done => EOF sentinel published => the stream ends cleanly. */
    public void testEofSentinelTerminatesStream() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(8)));
        buf.senderDone(LEFT);
        assertTrue("all senders done must publish EOF", buf.isEofEnqueued(LEFT));

        List<byte[]> got = new ArrayList<>();
        try (CloseableIterator<byte[]> it = buf.drain(LEFT, 5_000)) {
            while (it.hasNext()) {
                got.add(it.next());
            }
        }
        assertEquals(1, got.size());
        assertEquals(8, got.get(0).length);
    }

    /**
     * An empty partition must terminate immediately once its senders are done — this is the path that
     * lets a consumer register an empty table instead of hanging.
     */
    public void testEmptyPartitionTerminatesWithoutBlocking() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);
        buf.senderDone(LEFT);

        try (CloseableIterator<byte[]> it = buf.drain(LEFT, 5_000)) {
            assertFalse("empty partition must report end-of-stream, not block", it.hasNext());
        }
    }

    /**
     * The in-flight window is the residency bound. Once it is full admission must return the RETRYABLE
     * reject — not accept (unbounded growth), not throw (a retryable condition must not fail a query),
     * and not block a transport thread.
     */
    public void testFullInFlightWindowRejectsRetryably() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setStreamWindowBytes(100);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(60)));
        assertEquals("second chunk overflows the 100-byte window", AdmitResult.REJECT_RETRY, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(60)));
        assertEquals(60L, buf.queuedBytes(LEFT));
    }

    /** Draining frees the window, so a previously-rejected producer can proceed. This is the pacing loop. */
    public void testDrainingFreesTheWindow() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setStreamWindowBytes(100);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(60)));
        assertEquals(AdmitResult.REJECT_RETRY, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(60)));

        try (CloseableIterator<byte[]> it = buf.drain(LEFT, 5_000)) {
            assertTrue(it.hasNext());
            it.next(); // consume 60 bytes -> window has room again
        }
        assertEquals(0L, buf.queuedBytes(LEFT));
        assertEquals("window freed by the drain must admit the retry", AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(60)));
    }

    /**
     * Data after end-of-stream is a producer close-ordering bug and would be silently lost rows, so it
     * must fail loud. This replaces the old "admitted to an already-draining buffer" check, which can no
     * longer mean "too late" now that add and drain are concurrent by design.
     */
    public void testDataAfterEndOfStreamFailsLoud() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);
        buf.senderDone(LEFT);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> mgr.tryAdmit(Q, 0, 0, LEFT, chunk(4)));
        assertTrue("message must name the end-of-stream cause: " + e.getMessage(), e.getMessage().contains("after end-of-stream"));
    }

    /**
     * A drain that starts before its producers must NOT be treated as "too late to add" — the whole
     * point of pipelining. Guards against reintroducing the old {@code isDraining()} rejection.
     */
    public void testAdmitAfterDrainStartedStillAccepted() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        try (CloseableIterator<byte[]> ignored = buf.drain(LEFT, 100)) {
            assertTrue("drain must have marked the buffer as draining", buf.isDraining());
            assertEquals(
                "a concurrent drain must not reject admission under pipelining",
                AdmitResult.ACCEPTED,
                mgr.tryAdmit(Q, 0, 0, LEFT, chunk(8))
            );
        }
    }

    /**
     * Clearing a query must wake a parked drain and make it THROW. A clean end-of-stream here would let
     * the consumer close its native sender normally and the join would emit results from truncated
     * input — silent wrong answers.
     */
    public void testClearForQueryUnblocksParkedDrainWithFailure() throws Exception {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try (CloseableIterator<byte[]> it = buf.drain(LEFT, 30_000)) {
                parked.countDown();
                it.hasNext(); // parks: no data, no EOF
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                finished.countDown();
            }
        }, "test-shuffle-drain");
        consumer.setDaemon(true);
        consumer.start();

        assertTrue(parked.await(10, TimeUnit.SECONDS));
        mgr.clearForQuery(Q);

        assertTrue("clearForQuery must unblock the parked drain promptly", finished.await(10, TimeUnit.SECONDS));
        assertNotNull("an aborted drain must fail, never report a clean end-of-stream", thrown.get());
        assertTrue(thrown.get() instanceof IllegalStateException);
        assertTrue(thrown.get().getMessage(), thrown.get().getMessage().contains("aborted"));
    }

    /**
     * An UNDER-DECLARED {@code expectedSenders} must fail both ends loudly — never silently truncate.
     *
     * <p>This is the test whose absence let a wrong-answer bug reach the cluster. EOF is published when
     * {@code doneCount} reaches the declared count, so if the count is too low, EOF fires while a
     * straggling producer still has rows, the consumer terminates early, and the join returns fewer rows
     * with an HTTP 200. Every other test in this class declares the count correctly by construction, so
     * none of them could see it. At sf=10 this produced 18,215 late admits on a single worker while the
     * query "succeeded" with a wrong result.
     *
     * <p>Two producers, but only ONE declared. After the first reports isLast the slot is at end-of-stream;
     * the second producer's data must then (a) fail the producer, and (b) poison the consumer's stream so
     * a live drain cannot finish on truncated input.
     */
    public void testUnderDeclaredExpectedSendersFailsLoudlyNotSilently() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1); // WRONG: two producers will actually ship

        // Producer 1: data then isLast -> publishes EOF (correctly, per the declared count).
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(32)));
        buf.senderDone(LEFT);
        assertTrue(buf.isEofEnqueued(LEFT));

        // Producer 2 still has rows. This must NOT be quietly dropped.
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> mgr.tryAdmit(Q, 0, 0, LEFT, chunk(32)));
        assertTrue("must name end-of-stream: " + e.getMessage(), e.getMessage().contains("after end-of-stream"));

        // And the CONSUMER must fail rather than report a clean end after the one chunk it did see —
        // the producer-side throw alone can lose the race if the consumer already finished.
        IllegalStateException consumerFailure = expectThrows(IllegalStateException.class, () -> {
            try (CloseableIterator<byte[]> it = buf.drain(LEFT, 5_000)) {
                while (it.hasNext()) {
                    it.next();
                }
            }
        });
        assertTrue(
            "consumer must abort, not finish cleanly: " + consumerFailure.getMessage(),
            consumerFailure.getMessage().contains("aborted")
        );
    }

    /** A stalled producer must surface as a failure, never as a short read. */
    public void testStalledProducerTimesOutRatherThanUnderDelivering() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        try (CloseableIterator<byte[]> it = buf.drain(LEFT, 50)) {
            IllegalStateException e = expectThrows(IllegalStateException.class, it::hasNext);
            assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
        }
    }

    /**
     * Residency stays bounded by the window across a partition far larger than it — the property the
     * whole change exists for. Producer and consumer run concurrently; the queued bytes must never
     * exceed the window even though total volume is ~40x it.
     */
    public void testResidencyStaysBoundedAcrossALargePartition() throws Exception {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        final int window = 4096;
        mgr.setStreamWindowBytes(window);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        final int chunks = 200;
        final int chunkSize = 800; // 160_000 bytes total, ~39x the window
        AtomicReference<Throwable> producerError = new AtomicReference<>();
        AtomicReference<Long> peak = new AtomicReference<>(0L);

        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < chunks; i++) {
                    // Mirror ShuffleSenderRetry: a retryable reject is retried, not failed.
                    while (mgr.tryAdmit(Q, 0, 0, LEFT, chunk(chunkSize)) == AdmitResult.REJECT_RETRY) {
                        Thread.onSpinWait();
                    }
                    peak.updateAndGet(p -> Math.max(p, buf.queuedBytes(LEFT)));
                }
                buf.senderDone(LEFT);
            } catch (Throwable t) {
                producerError.set(t);
                buf.senderDone(LEFT); // never leave the consumer parked
            }
        }, "test-shuffle-producer");
        producer.setDaemon(true);
        producer.start();

        int received = 0;
        try (CloseableIterator<byte[]> it = buf.drain(LEFT, 30_000)) {
            while (it.hasNext()) {
                assertEquals(chunkSize, it.next().length);
                received++;
            }
        }
        producer.join(TimeUnit.SECONDS.toMillis(30));

        assertNull(producerError.get());
        assertEquals("every chunk must be delivered exactly once", chunks, received);
        assertTrue("peak queued bytes " + peak.get() + " must stay within the in-flight window " + window, peak.get() <= window);
    }
}
