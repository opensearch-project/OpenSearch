/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.blobstore.BlobContainer;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

/**
 * A sequential {@link InputStream} over a remote blob that downloads the blob as multiple
 * byte-range parts in parallel while presenting the bytes strictly in order.
 *
 * <p>The blob is split into fixed-size parts. The part that the reader is currently consuming
 * is streamed directly from the blob store without buffering. Parts ahead of the reader are
 * prefetched on the supplied {@link Executor} into heap buffers, subject to a node-wide
 * {@link ParallelDownloadPermits} budget so that total buffered memory stays bounded by
 * {@code permits * partSize}.
 *
 * <p>The reader never waits on work that has not started: if it reaches a part whose prefetch
 * task is still queued it claims the part and reads it itself, and the queued task becomes a
 * no-op. When no permits are available the stream degrades gracefully to a plain sequential
 * range-by-range download. This makes the stream safe to drive from the same thread pool that
 * executes the prefetch tasks.
 *
 * <p>Because the bytes are presented sequentially, the stream can be wrapped in a
 * {@link RemoteIndexInput} and consumed by the regular {@code Directory#copyFrom} path, so
 * recovery statistics wrappers, checksum verification on the {@code IndexOutput} side and any
 * local directory-level encryption continue to work unchanged.
 *
 * @opensearch.internal
 */
public final class ParallelPartInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(ParallelPartInputStream.class);

    private final BlobContainer blobContainer;
    private final String blobName;
    private final long fileLength;
    private final long partSize;
    private final int numParts;
    private final Executor executor;
    private final ParallelDownloadPermits permits;
    private final UnaryOperator<InputStream> rateLimiter;

    /** Prefetches keyed by part index. Only contains parts that hold a permit. */
    private final Map<Integer, Prefetch> prefetches = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // The following fields are only touched by the (single) reader thread.
    private int nextPart = 0;
    private int highestScheduledPart = -1;
    private InputStream current;
    private long currentRemaining;
    private Prefetch currentPrefetch;

    /**
     * @param blobContainer container to read ranges from
     * @param blobName      name of the blob within the container
     * @param fileLength    total length of the blob in bytes
     * @param partSize      size of each byte-range part
     * @param executor      executor used to run prefetch tasks
     * @param permits       node-wide budget bounding the number of prefetched parts in flight
     * @param rateLimiter   wraps each part stream, e.g. for download rate limiting
     */
    public ParallelPartInputStream(
        BlobContainer blobContainer,
        String blobName,
        long fileLength,
        long partSize,
        Executor executor,
        ParallelDownloadPermits permits,
        UnaryOperator<InputStream> rateLimiter
    ) {
        if (fileLength < 0) {
            throw new IllegalArgumentException("fileLength must be >= 0, got " + fileLength);
        }
        if (partSize <= 0) {
            throw new IllegalArgumentException("partSize must be > 0, got " + partSize);
        }
        this.blobContainer = blobContainer;
        this.blobName = blobName;
        this.fileLength = fileLength;
        this.partSize = partSize;
        this.numParts = Math.toIntExact((fileLength + partSize - 1) / partSize);
        this.executor = executor;
        this.permits = permits;
        this.rateLimiter = rateLimiter;
    }

    public int getNumberOfParts() {
        return numParts;
    }

    @Override
    public int read() throws IOException {
        final byte[] b = new byte[1];
        final int n = read(b, 0, 1);
        return n < 0 ? -1 : (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if (len == 0) {
            return 0;
        }
        while (true) {
            if (current == null) {
                if (nextPart >= numParts) {
                    return -1;
                }
                openNextPart();
            }
            final int toRead = (int) Math.min(len, currentRemaining);
            final int n = current.read(b, off, toRead);
            if (n < 0) {
                throw new EOFException(
                    "Unexpected end of stream while reading part "
                        + (nextPart - 1)
                        + " of blob ["
                        + blobName
                        + "]: "
                        + currentRemaining
                        + " bytes remaining"
                );
            }
            if (n == 0) {
                // InputStream#read(byte[], int, int) may only return 0 when len == 0, and toRead >= 1 here.
                // Fail rather than spin on a non-conforming stream.
                throw new IOException("Blob stream for [" + blobName + "] returned 0 bytes for a " + toRead + "-byte read");
            }
            currentRemaining -= n;
            if (currentRemaining == 0) {
                finishCurrentPart();
            }
            return n;
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        IOException failure = null;
        try {
            finishCurrentPart();
        } catch (IOException e) {
            failure = e;
        }
        // Return every permit this stream holds. A part that has not started is cancelled and released now. A part
        // whose task is already running still owns a buffer of up to partSize bytes until it completes, so its permit
        // is released only when the future completes (immediately if it already has) -- that keeps the node-wide
        // permits * partSize bound exact instead of letting the buffer briefly outlive its permit.
        for (Prefetch prefetch : prefetches.values()) {
            if (prefetch.owner.compareAndSet(Owner.NONE, Owner.CANCELLED)) {
                prefetch.releasePermit();
            } else {
                prefetch.future.whenComplete((bytes, t) -> prefetch.releasePermit());
            }
        }
        prefetches.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("Stream for blob [" + blobName + "] is closed");
        }
    }

    private void openNextPart() throws IOException {
        final int partIndex = nextPart++;
        final long position = partPosition(partIndex);
        final long length = partLength(partIndex);

        Prefetch prefetch = prefetches.remove(partIndex);
        if (prefetch != null && prefetch.owner.compareAndSet(Owner.NONE, Owner.READER)) {
            // The prefetch task has not started yet: steal the part and read it ourselves.
            // The task will observe the ownership change and do nothing.
            prefetch.releasePermit();
            prefetch = null;
        }

        if (prefetch != null) {
            // Owned by a running (or finished) prefetch task: wait for its buffer.
            try {
                current = new ByteArrayInputStream(prefetch.future.get());
                currentPrefetch = prefetch;
            } catch (InterruptedException e) {
                prefetch.releasePermit();
                Thread.currentThread().interrupt();
                final InterruptedIOException iioe = new InterruptedIOException(
                    "Interrupted while waiting for part " + partIndex + " of blob [" + blobName + "]"
                );
                iioe.initCause(e);
                throw iioe;
            } catch (ExecutionException e) {
                prefetch.releasePermit();
                throw unwrap(e.getCause(), partIndex);
            }
        } else {
            current = openRange(position, length);
        }
        currentRemaining = length;
        schedulePrefetches();
    }

    private void finishCurrentPart() throws IOException {
        if (current != null) {
            final InputStream toClose = current;
            current = null;
            try {
                toClose.close();
            } finally {
                if (currentPrefetch != null) {
                    currentPrefetch.releasePermit();
                    currentPrefetch = null;
                }
            }
        }
    }

    /**
     * Schedules prefetches for the parts following the one being read, in increasing order,
     * for as long as permits are available. Never blocks.
     */
    private void schedulePrefetches() {
        if (permits == null) {
            return;
        }
        // Do not read further ahead than the budget would ever allow.
        final int lookaheadLimit = Math.min(numParts, nextPart + permits.getMaxPermits());
        for (int partIndex = Math.max(nextPart, highestScheduledPart + 1); partIndex < lookaheadLimit; partIndex++) {
            if (permits.tryAcquire() == false) {
                return;
            }
            final Prefetch prefetch = new Prefetch(partIndex);
            prefetches.put(partIndex, prefetch);
            highestScheduledPart = partIndex;
            try {
                executor.execute(prefetch);
            } catch (RejectedExecutionException e) {
                prefetches.remove(partIndex);
                prefetch.owner.set(Owner.CANCELLED);
                prefetch.releasePermit();
                final int rejectedPart = partIndex;
                logger.debug(() -> new ParameterizedMessage("Prefetch of part {} for blob [{}] rejected", rejectedPart, blobName), e);
                return;
            }
        }
    }

    private InputStream openRange(long position, long length) throws IOException {
        return rateLimiter.apply(blobContainer.readBlob(blobName, position, length));
    }

    /** Byte offset of a part. The index is widened explicitly so the product is computed in long arithmetic. */
    private long partPosition(int partIndex) {
        return (long) partIndex * partSize;
    }

    /** Length of a part; only the last part can be shorter than {@code partSize}. */
    private long partLength(int partIndex) {
        return Math.min(partSize, fileLength - partPosition(partIndex));
    }

    /**
     * Reads one part fully into a single, exactly-sized buffer. The length is known up front, so the buffer is
     * allocated once and filled in place; {@code InputStream#readNBytes} would instead accumulate 16 KB chunks and
     * copy them into a final array, briefly holding roughly twice the part size on heap per prefetch.
     */
    private byte[] readRange(long position, long length) throws IOException {
        final byte[] bytes = new byte[Math.toIntExact(length)];
        try (InputStream in = openRange(position, length)) {
            int offset = 0;
            while (offset < bytes.length) {
                final int n = in.read(bytes, offset, bytes.length - offset);
                if (n < 0) {
                    throw new EOFException(
                        "Expected " + length + " bytes at offset " + position + " of blob [" + blobName + "] but read " + offset
                    );
                }
                offset += n;
            }
        }
        return bytes;
    }

    private IOException unwrap(Throwable cause, int partIndex) {
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return new IOException("Failed to prefetch part " + partIndex + " of blob [" + blobName + "]", cause);
    }

    private enum Owner {
        NONE,
        PREFETCHER,
        READER,
        CANCELLED
    }

    /**
     * A single read-ahead part. Holds exactly one permit from the time it is scheduled until
     * {@link #releasePermit()} is called, which happens once the reader has consumed the buffer,
     * the reader has stolen the part, or the stream is closed.
     */
    private final class Prefetch implements Runnable {
        private final int partIndex;
        private final CompletableFuture<byte[]> future = new CompletableFuture<>();
        private final AtomicReference<Owner> owner = new AtomicReference<>(Owner.NONE);
        private final AtomicBoolean permitReleased = new AtomicBoolean(false);

        Prefetch(int partIndex) {
            this.partIndex = partIndex;
        }

        @Override
        public void run() {
            if (owner.compareAndSet(Owner.NONE, Owner.PREFETCHER) == false) {
                // Stolen by the reader or cancelled before we started.
                releasePermit();
                return;
            }
            try {
                final byte[] bytes = readRange(partPosition(partIndex), partLength(partIndex));
                if (closed.get()) {
                    // Nobody will consume this; drop the buffer immediately.
                    future.completeExceptionally(new IOException("Stream for blob [" + blobName + "] is closed"));
                } else {
                    future.complete(bytes);
                }
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }

        void releasePermit() {
            if (permitReleased.compareAndSet(false, true)) {
                permits.release();
            }
        }
    }
}
