/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-node registry of shuffle buffers for hash-shuffle joins. One {@link ShuffleBuffer} per
 * (queryId, targetStageId, partitionIndex).
 *
 * <p>Ported from OLAP's {@code ShuffleManager}
 * ({@code opensearch-olap/.../transport/ShuffleManager.java}). OLAP keys by (queryId, stageId)
 * because its dispatch assigns exactly one shuffle partition per node. analytics-engine supports
 * configurations where a single node hosts multiple partitions of the same stage (small clusters,
 * over-provisioned {@code plugins.analytics.shuffle_partitions}), so partitionIndex must be part
 * of the key — otherwise two partitions' left/right payloads and {@code isLast} markers would
 * land in the same buffer and the join would read cross-partition rows + see false completion.
 *
 * <p>Each {@link ShuffleBuffer} tracks per-side sender completion via {@code CountDownLatch}es
 * and enforces a per-partition byte cap — senders must retry with exponential backoff when
 * {@link ShuffleBuffer#tryAddData} returns {@code false}. The consumer calls
 * {@link ShuffleBuffer#awaitReady} before draining.
 *
 * @opensearch.internal
 */
public class ShuffleBufferManager {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleBufferManager.class);

    private final Map<String, ShuffleBuffer> buffers = new ConcurrentHashMap<>();
    private volatile long bufferMaxBytes = Long.MAX_VALUE;

    /** Configure the per-buffer cap for newly-created buffers. Existing buffers keep their cap. */
    public void setBufferMaxBytes(long bufferMaxBytes) {
        this.bufferMaxBytes = bufferMaxBytes;
    }

    public long getBufferMaxBytes() {
        return bufferMaxBytes;
    }

    public ShuffleBuffer getOrCreateBuffer(String queryId, int targetStageId, int partitionIndex) {
        return buffers.computeIfAbsent(key(queryId, targetStageId, partitionIndex), k -> new ShuffleBuffer(bufferMaxBytes));
    }

    public ShuffleBuffer getBuffer(String queryId, int targetStageId, int partitionIndex) {
        return buffers.get(key(queryId, targetStageId, partitionIndex));
    }

    public void removeBuffer(String queryId, int targetStageId, int partitionIndex) {
        buffers.remove(key(queryId, targetStageId, partitionIndex));
    }

    private static String key(String queryId, int targetStageId, int partitionIndex) {
        return queryId + ":" + targetStageId + ":" + partitionIndex;
    }

    /**
     * Per-partition shuffle buffer. Thread-safe — multiple senders may call {@link #tryAddData}
     * concurrently; one consumer thread drains via {@link #getLeftData} / {@link #getRightData}
     * after {@link #awaitReady} returns.
     */
    public static class ShuffleBuffer {
        private final List<byte[]> leftData = Collections.synchronizedList(new ArrayList<>());
        private final List<byte[]> rightData = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger leftDoneCount = new AtomicInteger();
        private final AtomicInteger rightDoneCount = new AtomicInteger();
        private volatile int expectedLeftSenders = -1;
        private volatile int expectedRightSenders = -1;
        private final CountDownLatch leftReady = new CountDownLatch(1);
        private final CountDownLatch rightReady = new CountDownLatch(1);
        private final AtomicLong currentBytes = new AtomicLong();
        private final AtomicLong rejectedCount = new AtomicLong();
        private final long maxBytes;

        public ShuffleBuffer(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        public void setExpectedSenders(int leftSenders, int rightSenders) {
            this.expectedLeftSenders = leftSenders;
            this.expectedRightSenders = rightSenders;
            checkCompletion("left");
            checkCompletion("right");
        }

        /**
         * Attempts to add {@code data} to the named side. Returns {@code false} if the buffer's
         * byte cap would be exceeded — the sender must retry with exponential backoff.
         */
        public boolean tryAddData(String side, byte[] data) {
            int size = data == null ? 0 : data.length;
            long newTotal = currentBytes.addAndGet(size);
            if (newTotal > maxBytes) {
                currentBytes.addAndGet(-size);
                rejectedCount.incrementAndGet();
                return false;
            }
            if ("left".equals(side)) {
                leftData.add(data);
            } else {
                rightData.add(data);
            }
            return true;
        }

        public long getCurrentBytes() {
            return currentBytes.get();
        }

        public long getMaxBytes() {
            return maxBytes;
        }

        public long getRejectedCount() {
            return rejectedCount.get();
        }

        public void senderDone(String side) {
            if ("left".equals(side)) {
                leftDoneCount.incrementAndGet();
                checkCompletion("left");
            } else {
                rightDoneCount.incrementAndGet();
                checkCompletion("right");
            }
        }

        private void checkCompletion(String side) {
            if ("left".equals(side)) {
                if (expectedLeftSenders >= 0 && leftDoneCount.get() >= expectedLeftSenders) {
                    leftReady.countDown();
                }
            } else {
                if (expectedRightSenders >= 0 && rightDoneCount.get() >= expectedRightSenders) {
                    rightReady.countDown();
                }
            }
        }

        /** Wait for both sides' senders to complete. Returns {@code false} on timeout. */
        public boolean awaitReady(long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            long remaining = timeoutMillis;
            if (!leftReady.await(remaining, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Shuffle left side timed out: received {}/{} senders", leftDoneCount.get(), expectedLeftSenders);
                return false;
            }
            remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return false;
            if (!rightReady.await(remaining, TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Shuffle right side timed out: received {}/{} senders", rightDoneCount.get(), expectedRightSenders);
                return false;
            }
            return true;
        }

        public List<byte[]> getLeftData() {
            return leftData;
        }

        public List<byte[]> getRightData() {
            return rightData;
        }

        public int getExpectedLeftSenders() {
            return expectedLeftSenders;
        }

        public int getExpectedRightSenders() {
            return expectedRightSenders;
        }
    }
}
