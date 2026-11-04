/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.io.PathUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ParallelPartInputStreamTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Path path;
    private String blobName;
    private byte[] blobData;
    private CountingBlobContainer container;

    @Before
    public void setup() throws IOException {
        threadPool = new TestThreadPool(getTestName());
        path = PathUtils.get(createTempDir().toString());
        blobName = "segment_" + randomAlphaOfLength(8);
        // Between a handful of bytes and ~1.5MB so that we exercise 1..N parts with the part sizes used below.
        blobData = randomByteArrayOfLength(randomIntBetween(1, 3 << 19));
        Files.write(path.resolve(blobName), blobData);
        container = new CountingBlobContainer(new FsBlobContainer(new FsBlobStore(8 * 1024, path, false), BlobPath.cleanPath(), path));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    private Executor executor() {
        return threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY);
    }

    public void testReadsBytesInOrderAcrossParts() throws IOException {
        final long partSize = randomIntBetween(1, 128) * 1024L;
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(randomIntBetween(1, 8));
        final byte[] read;
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                container,
                blobName,
                blobData.length,
                partSize,
                executor(),
                permits,
                UnaryOperator.identity()
            )
        ) {
            assertEquals((blobData.length + partSize - 1) / partSize, in.getNumberOfParts());
            read = readFully(in, randomIntBetween(1, 64 * 1024));
            assertEquals(-1, in.read());
        }
        assertArrayEquals(blobData, read);
        assertEquals("every part fetched exactly once", (blobData.length + partSize - 1) / partSize, container.rangeReads.get());
        assertEquals("all permits returned", permits.getMaxPermits(), permits.availablePermits());
    }

    public void testSingleByteReads() throws IOException {
        final long partSize = 1024;
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(2);
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                container,
                blobName,
                blobData.length,
                partSize,
                executor(),
                permits,
                UnaryOperator.identity()
            )
        ) {
            for (byte expected : blobData) {
                assertEquals(expected & 0xFF, in.read());
            }
            assertEquals(-1, in.read());
        }
        assertEquals(permits.getMaxPermits(), permits.availablePermits());
    }

    public void testZeroPermitsDegradesToSequentialRangeReads() throws IOException {
        final long partSize = 4096;
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(0);
        final Executor rejecting = r -> { throw new AssertionError("no prefetch expected without permits"); };
        final byte[] read;
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                container,
                blobName,
                blobData.length,
                partSize,
                rejecting,
                permits,
                UnaryOperator.identity()
            )
        ) {
            read = readFully(in, 777);
        }
        assertArrayEquals(blobData, read);
        assertEquals((blobData.length + partSize - 1) / partSize, container.rangeReads.get());
    }

    public void testPrefetchNeverExceedsPermitBudget() throws Exception {
        final long partSize = 1024;
        final int budget = randomIntBetween(1, 4);
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(budget);
        // Hold every prefetch task open until we have observed the count; the reader must still make progress
        // on its own part in the meantime (it reads that part directly, without a permit).
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger gatedPrefetches = new AtomicInteger();
        final Executor gated = r -> executor().execute(() -> {
            gatedPrefetches.incrementAndGet();
            try {
                assertTrue(release.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            r.run();
        });
        final byte[] read;
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                container,
                blobName,
                blobData.length,
                partSize,
                gated,
                permits,
                UnaryOperator.identity()
            )
        ) {
            if (in.getNumberOfParts() > 1) {
                // Reading the first part schedules prefetches for the following parts, capped by the budget.
                final byte[] first = new byte[(int) Math.min(partSize, blobData.length)];
                int off = 0;
                while (off < first.length) {
                    off += in.read(first, off, first.length - off);
                }
                final int expectedInFlight = Math.min(budget, in.getNumberOfParts() - 1);
                assertBusy(() -> assertThat(gatedPrefetches.get(), equalTo(expectedInFlight)));
                assertThat(permits.availablePermits(), equalTo(budget - expectedInFlight));
                release.countDown();
                final byte[] rest = readFully(in, 4096);
                read = new byte[first.length + rest.length];
                System.arraycopy(first, 0, read, 0, first.length);
                System.arraycopy(rest, 0, read, first.length, rest.length);
            } else {
                release.countDown();
                read = readFully(in, 4096);
            }
        }
        assertArrayEquals(blobData, read);
        assertEquals(budget, permits.availablePermits());
    }

    public void testReaderStealsQueuedPrefetch() throws IOException {
        // An executor that never runs anything: every prefetch stays queued, so the reader must claim each part
        // itself and the download must still complete with exactly one range read per part.
        final long partSize = 2048;
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(3);
        final List<Runnable> queued = new ArrayList<>();
        final Executor neverRuns = queued::add;
        final byte[] read;
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                container,
                blobName,
                blobData.length,
                partSize,
                neverRuns,
                permits,
                UnaryOperator.identity()
            )
        ) {
            read = readFully(in, 1000);
        }
        assertArrayEquals(blobData, read);
        assertEquals((blobData.length + partSize - 1) / partSize, container.rangeReads.get());
        // Stolen tasks must be no-ops when they eventually run, and must not double-release permits.
        assertEquals(3, permits.availablePermits());
        queued.forEach(Runnable::run);
        assertEquals(3, permits.availablePermits());
        assertEquals("stolen tasks must not fetch", (blobData.length + partSize - 1) / partSize, container.rangeReads.get());
    }

    public void testRejectedExecutionFallsBackToDirectRead() throws IOException {
        final long partSize = 2048;
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(2);
        final Executor rejecting = r -> { throw new RejectedExecutionException("full"); };
        final byte[] read;
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                container,
                blobName,
                blobData.length,
                partSize,
                rejecting,
                permits,
                UnaryOperator.identity()
            )
        ) {
            read = readFully(in, 3000);
        }
        assertArrayEquals(blobData, read);
        assertEquals(2, permits.availablePermits());
    }

    public void testPrefetchFailurePropagatesAndReleasesPermits() throws Exception {
        assumeTrue("need at least two parts", blobData.length > 1);
        final long partSize = Math.max(1, blobData.length / 2);
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(4);
        final IOException boom = new IOException("boom");
        // Fail every range read that does not start at offset 0, i.e. every prefetched part.
        final BlobContainer failing = new FilterBlobContainer(container) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                if (position > 0) {
                    throw boom;
                }
                return super.readBlob(name, position, length);
            }
        };
        final ParallelPartInputStream in = new ParallelPartInputStream(
            failing,
            blobName,
            blobData.length,
            partSize,
            executor(),
            permits,
            UnaryOperator.identity()
        );
        final IOException e = expectThrows(IOException.class, () -> readFully(in, 512));
        assertSame(boom, e);
        in.close();
        assertBusy(() -> assertEquals(4, permits.availablePermits()));
    }

    public void testShortRangeReadIsDetected() throws Exception {
        assumeTrue("need at least two parts", blobData.length > 1);
        final long partSize = Math.max(1, blobData.length / 2);
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(4);
        // Truncate every range stream by one byte.
        final BlobContainer truncating = new FilterBlobContainer(container) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                return org.opensearch.common.io.Streams.limitStream(super.readBlob(name, position, length), length - 1);
            }
        };
        final ParallelPartInputStream in = new ParallelPartInputStream(
            truncating,
            blobName,
            blobData.length,
            partSize,
            executor(),
            permits,
            UnaryOperator.identity()
        );
        expectThrows(EOFException.class, () -> readFully(in, 512));
        in.close();
        // prefetches still running at close() return their permit when they complete
        assertBusy(() -> assertEquals(4, permits.availablePermits()));
    }

    public void testCloseReleasesOutstandingPermits() throws Exception {
        assumeTrue("need at least three parts", blobData.length > 2);
        final long partSize = Math.max(1, blobData.length / 3);
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(8);
        final CountDownLatch release = new CountDownLatch(1);
        final Executor gated = r -> executor().execute(() -> {
            try {
                assertTrue(release.await(30, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            r.run();
        });
        final ParallelPartInputStream in = new ParallelPartInputStream(
            container,
            blobName,
            blobData.length,
            partSize,
            gated,
            permits,
            UnaryOperator.identity()
        );
        assertThat(in.read(), greaterThanOrEqualTo(0)); // opens part 0 and schedules prefetches
        assertThat(permits.availablePermits(), lessThanOrEqualTo(8 - 1));
        in.close();
        // The gated tasks never started, so they are cancelled outright and every permit comes back at once.
        assertEquals("close must hand back every permit the stream held", 8, permits.availablePermits());
        release.countDown();
        expectThrows(IOException.class, () -> in.read());
        // Tasks that were still gated must not disturb the budget once they run.
        assertBusy(() -> assertEquals(8, permits.availablePermits()));
    }

    public void testCloseKeepsPermitOfRunningPrefetchUntilItsBufferIsGone() throws Exception {
        assumeTrue("need at least two parts", blobData.length > 1);
        final long partSize = Math.max(1, blobData.length / 2);
        final ParallelDownloadPermits permits = new ParallelDownloadPermits(4);
        // Block inside the range read of every prefetched part, i.e. AFTER the task has claimed ownership.
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final BlobContainer slow = new FilterBlobContainer(container) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                if (position > 0) {
                    entered.countDown();
                    try {
                        assertTrue(release.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                }
                return super.readBlob(name, position, length);
            }
        };
        final ParallelPartInputStream in = new ParallelPartInputStream(
            slow,
            blobName,
            blobData.length,
            partSize,
            executor(),
            permits,
            UnaryOperator.identity()
        );
        assertThat(in.read(), greaterThanOrEqualTo(0));
        final int expectedInFlight = Math.min(4, in.getNumberOfParts() - 1);
        assertTrue("a prefetch task must be running inside readBlob", entered.await(30, TimeUnit.SECONDS));
        assertEquals(4 - expectedInFlight, permits.availablePermits());
        in.close();
        // At least the task blocked in readBlob is running and may still allocate its buffer, so its permit must
        // remain held: the permits * partSize bound has to stay exact through a close race.
        assertThat(permits.availablePermits(), lessThanOrEqualTo(3));
        release.countDown();
        assertBusy(() -> assertEquals(4, permits.availablePermits()));
    }

    public void testZeroLengthReadFromBlobStreamFailsFast() throws IOException {
        final long partSize = 4096;
        final BlobContainer broken = new FilterBlobContainer(container) {
            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return child;
            }

            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                return new FilterInputStream(super.readBlob(name, position, length)) {
                    @Override
                    public int read(byte[] b, int off, int len) {
                        return 0; // violates the InputStream contract for len > 0
                    }
                };
            }
        };
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                broken,
                blobName,
                blobData.length,
                partSize,
                executor(),
                new ParallelDownloadPermits(0),
                UnaryOperator.identity()
            )
        ) {
            final IOException e = expectThrows(IOException.class, () -> in.read(new byte[16], 0, 16));
            assertThat(e.getMessage(), containsString("returned 0 bytes"));
        }
    }

    public void testRateLimiterWrapsEveryPart() throws IOException {
        final long partSize = 1024;
        final AtomicInteger wrapped = new AtomicInteger();
        final UnaryOperator<InputStream> counting = s -> {
            wrapped.incrementAndGet();
            return s;
        };
        try (
            ParallelPartInputStream in = new ParallelPartInputStream(
                container,
                blobName,
                blobData.length,
                partSize,
                executor(),
                new ParallelDownloadPermits(4),
                counting
            )
        ) {
            assertArrayEquals(blobData, readFully(in, 4096));
        }
        assertEquals((blobData.length + partSize - 1) / partSize, wrapped.get());
        assertThat(wrapped.get(), greaterThan(0));
    }

    private static byte[] readFully(InputStream in, int bufferSize) throws IOException {
        final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        final byte[] buf = new byte[bufferSize];
        int n;
        while ((n = in.read(buf, 0, buf.length)) != -1) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    /** Counts range reads issued against the delegate. */
    private static final class CountingBlobContainer extends FilterBlobContainer {
        final AtomicInteger rangeReads = new AtomicInteger();

        CountingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return child;
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            rangeReads.incrementAndGet();
            return new FilterInputStream(super.readBlob(blobName, position, length)) {
            };
        }
    }
}
