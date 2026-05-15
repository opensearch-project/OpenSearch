/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ShuffleBufferManagerTests extends OpenSearchTestCase {

    public void testGetOrCreateReturnsStableBuffer() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer a = mgr.getOrCreateBuffer("q1", 0, 0);
        ShuffleBufferManager.ShuffleBuffer b = mgr.getOrCreateBuffer("q1", 0, 0);
        assertSame("same key must return same buffer", a, b);

        ShuffleBufferManager.ShuffleBuffer c = mgr.getOrCreateBuffer("q1", 1, 0);
        assertNotSame("different stageId must yield a different buffer", a, c);
    }

    /**
     * Codex review P1: buffers must be keyed by partitionIndex too, otherwise two partitions of the
     * same stage that happen to land on the same worker node would share a buffer and leak rows +
     * {@code isLast} markers across partitions. This test pins the correct key shape and the
     * resulting isolation.
     */
    public void testDifferentPartitionsSameStageHaveDistinctBuffers() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer p0 = mgr.getOrCreateBuffer("q1", 0, 0);
        ShuffleBufferManager.ShuffleBuffer p1 = mgr.getOrCreateBuffer("q1", 0, 1);
        assertNotSame("partition 0 and partition 1 must not share a buffer", p0, p1);

        // Feed data into each partition and confirm isolation — p0 sees only its own bytes.
        p0.tryAddData("left", new byte[] { 10, 20 });
        p1.tryAddData("left", new byte[] { 99 });
        assertEquals(2L, p0.getCurrentBytes());
        assertEquals(1L, p1.getCurrentBytes());
    }

    public void testRemoveBufferDeletesIt() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.getOrCreateBuffer("q1", 0, 0);
        assertNotNull(mgr.getBuffer("q1", 0, 0));
        mgr.removeBuffer("q1", 0, 0);
        assertNull(mgr.getBuffer("q1", 0, 0));
    }

    public void testBackpressureRejectWhenCapExceeded() {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer(/* maxBytes */ 10);
        assertTrue(buffer.tryAddData("left", new byte[] { 1, 2, 3, 4, 5 }));
        assertEquals(5L, buffer.getCurrentBytes());
        assertTrue(buffer.tryAddData("left", new byte[] { 1, 2, 3, 4, 5 }));
        assertEquals(10L, buffer.getCurrentBytes());
        // Next add pushes over cap — must be rejected.
        assertFalse(buffer.tryAddData("right", new byte[] { 1 }));
        assertEquals("current bytes must not increase on reject", 10L, buffer.getCurrentBytes());
        assertEquals(1L, buffer.getRejectedCount());
    }

    public void testAwaitReadyCompletesWhenBothSidesDone() throws Exception {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer(Long.MAX_VALUE);
        buffer.setExpectedSenders(2, 1);
        // Two left senders, one right sender. Mark them done from another thread, then await.
        CountDownLatch startGate = new CountDownLatch(1);
        Thread senderThread = new Thread(() -> {
            try {
                startGate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            buffer.senderDone("left");
            buffer.senderDone("left");
            buffer.senderDone("right");
        });
        senderThread.start();
        startGate.countDown();
        assertTrue("awaitReady must complete after all senders signal done", buffer.awaitReady(5_000));
        senderThread.join(1_000);
    }

    public void testAwaitReadyTimesOut() throws Exception {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer(Long.MAX_VALUE);
        buffer.setExpectedSenders(1, 1);
        // Mark left done but leave right pending — short timeout must return false.
        buffer.senderDone("left");
        assertFalse("awaitReady must time out when right side never completes", buffer.awaitReady(100));
    }

    public void testCompletionTriggeredEvenIfSendersFinishBeforeExpectedSet() throws Exception {
        // Edge case: senders may signal done before setExpectedSenders is called. The implementation
        // re-checks after set, so the buffer must transition ready.
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer(Long.MAX_VALUE);
        buffer.senderDone("left");
        buffer.senderDone("right");
        buffer.setExpectedSenders(1, 1);
        assertTrue(buffer.awaitReady(1_000));
    }
}
