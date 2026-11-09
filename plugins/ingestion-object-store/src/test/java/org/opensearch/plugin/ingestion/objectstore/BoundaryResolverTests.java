/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BoundaryResolver} backed by a real {@link FsBlobStore}.
 */
public class BoundaryResolverTests extends OpenSearchTestCase {

    private static final long MAX_RECORD_BYTES = 10 * 1024 * 1024;

    private BlobContainer container;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Path dataDir = createTempDir().resolve("data");
        Files.createDirectories(dataDir);
        container = new FsBlobStore(8192, dataDir, false).blobContainer(BlobPath.cleanPath());
    }

    public void testTallyRecordsOneReadAndTheBytesScanned() throws Exception {
        long size = write("t.ndjson", "aaaa\nbbbb\n");
        StoreReadTally tally = new StoreReadTally();
        assertThat(BoundaryResolver.resolveLineStart(container, "t.ndjson", 2, size, MAX_RECORD_BYTES, tally), is(5L));
        assertThat(tally.getRequests(), is(1L));
        assertThat(tally.bytesRead(), greaterThan(0L));
    }

    public void testTallyAccumulatesAcrossResolutions() throws Exception {
        long size = write("t.ndjson", "aaaa\nbbbb\n");
        StoreReadTally tally = new StoreReadTally();
        BoundaryResolver.resolveLineStart(container, "t.ndjson", 2, size, MAX_RECORD_BYTES, tally);
        BoundaryResolver.resolveLineStart(container, "t.ndjson", 7, size, MAX_RECORD_BYTES, tally);
        assertThat(tally.getRequests(), is(2L));
    }

    public void testTallyRecordsNothingWhenTheResolverShortCircuits() throws Exception {
        long size = write("t.ndjson", "aaaa\n");
        StoreReadTally tally = new StoreReadTally();
        BoundaryResolver.resolveLineStart(container, "t.ndjson", 0, size, MAX_RECORD_BYTES, tally);
        BoundaryResolver.resolveLineStart(container, "t.ndjson", size, size, MAX_RECORD_BYTES, tally);
        assertThat(tally.getRequests(), is(0L));
        assertThat(tally.bytesRead(), is(0L));
    }

    public void testTallyIsChargedEvenWhenResolutionFails() throws Exception {
        long size = write("t.ndjson", "aaaaaaaa");
        StoreReadTally tally = new StoreReadTally();
        expectThrows(
            UnresolvableBoundaryException.class,
            () -> BoundaryResolver.resolveLineStart(container, "t.ndjson", 2, size + 100, MAX_RECORD_BYTES, tally)
        );
        assertThat(tally.getRequests(), is(1L));
    }

    public void testNullTallyRejected() throws Exception {
        long size = write("t.ndjson", "aaaa\n");
        expectThrows(
            NullPointerException.class,
            () -> BoundaryResolver.resolveLineStart(container, "t.ndjson", 2, size, MAX_RECORD_BYTES, null)
        );
    }

    private long write(String name, String content) throws Exception {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (InputStream in = new ByteArrayInputStream(bytes)) {
            container.writeBlob(name, in, bytes.length, true);
        }
        return bytes.length;
    }

    public void testOffsetZeroResolvesToZero() throws Exception {
        long size = write("a", "aaaa\nbbbb\ncccc\n");
        assertThat(BoundaryResolver.resolveLineStart(container, "a", 0, size, MAX_RECORD_BYTES), is(0L));
    }

    public void testTruncatedObjectIsRejected() throws Exception {
        write("trunc", "aaaa");
        UnresolvableBoundaryException e = expectThrows(
            UnresolvableBoundaryException.class,
            () -> BoundaryResolver.resolveLineStart(container, "trunc", 1, 100, MAX_RECORD_BYTES)
        );
        assertThat(e.getMessage(), containsString("the object was truncated"));
    }

    public void testMidLineResolvesToNextLineStart() throws Exception {
        long size = write("a", "aaaa\nbbbb\ncccc\n");
        assertThat(BoundaryResolver.resolveLineStart(container, "a", 2, size, MAX_RECORD_BYTES), is(5L));
    }

    public void testExactLineStartSkipsThatLine() throws Exception {
        long size = write("a", "aaaa\nbbbb\ncccc\n");
        assertThat(BoundaryResolver.resolveLineStart(container, "a", 5, size, MAX_RECORD_BYTES), is(10L));
    }

    public void testNoNewlineAfterOffsetResolvesToObjectSize() throws Exception {
        long size = write("a", "aaaa\nbb");
        assertThat(BoundaryResolver.resolveLineStart(container, "a", 6, size, MAX_RECORD_BYTES), is(size));
    }

    public void testLargeNoNewlineRegionScansMultipleBuffersToObjectSize() throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("head\n");
        for (int i = 0; i < 20000; i++) {
            builder.append('x');
        }
        long size = write("big", builder.toString());
        assertThat(BoundaryResolver.resolveLineStart(container, "big", 5, size, MAX_RECORD_BYTES), is(size));
    }

    public void testNewlineFoundAfterMultipleBufferReads() throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("head\n");
        for (int i = 0; i < 20000; i++) {
            builder.append('x');
        }
        builder.append('\n');
        builder.append("tail");
        String content = builder.toString();
        long size = write("big2", content);
        long expected = content.indexOf('\n', 5) + 1L;
        assertThat(BoundaryResolver.resolveLineStart(container, "big2", 5, size, MAX_RECORD_BYTES), is(expected));
    }

    public void testOffsetAtOrBeyondObjectSizeResolvesToObjectSize() throws Exception {
        long size = write("a", "aaaa\nbbbb\n");
        assertThat(BoundaryResolver.resolveLineStart(container, "a", size, size, MAX_RECORD_BYTES), is(size));
        assertThat(BoundaryResolver.resolveLineStart(container, "a", size + 100, size, MAX_RECORD_BYTES), is(size));
    }

    public void testNegativeOffsetResolvesToZero() throws Exception {
        long size = write("a", "aaaa\n");
        assertThat(BoundaryResolver.resolveLineStart(container, "a", -5, size, MAX_RECORD_BYTES), is(0L));
    }

    public void testReadFailurePropagatesAndClosesStream() throws Exception {
        BlobContainer failing = mock(BlobContainer.class);
        AtomicBoolean closed = new AtomicBoolean();
        InputStream throwingStream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("read boom");
            }

            @Override
            public int read(byte[] b) throws IOException {
                throw new IOException("read boom");
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        when(failing.readBlob(anyString(), anyLong(), anyLong())).thenReturn(throwingStream);
        StoreReadTally tally = new StoreReadTally();
        IOException e = expectThrows(
            IOException.class,
            () -> BoundaryResolver.resolveLineStart(failing, "a", 2, 10, MAX_RECORD_BYTES, tally)
        );
        assertThat(e.getMessage(), is("read boom"));
        assertThat(e, is(not(instanceOf(UnresolvableBoundaryException.class))));
        assertThat("the stream must be closed when the read fails", closed.get(), is(true));
        assertThat("the attempted request must still be charged", tally.getRequests(), is(1L));
    }

    public void testNoNewlineWithinTheScanBoundIsRejected() throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("head\n");
        for (int i = 0; i < 200; i++) {
            builder.append('x');
        }
        builder.append('\n');
        long size = write("unsplittable", builder.toString());
        UnresolvableBoundaryException e = expectThrows(
            UnresolvableBoundaryException.class,
            () -> BoundaryResolver.resolveLineStart(container, "unsplittable", 5, size, 32)
        );
        assertThat(e.getMessage(), containsString("has no newline within [32]"));
    }

    public void testNewlineAtExactlyTheScanBoundIsFound() throws Exception {
        long size = write("inside", "head\nxxxxxxx\ntail\n");
        assertThat(BoundaryResolver.resolveLineStart(container, "inside", 5, size, 8), is(13L));
    }

    public void testNewlineBeyondTheScanBoundIsRejected() throws Exception {
        long size = write("outside", "head\nxxxxxxx\ntail\n");
        UnresolvableBoundaryException e = expectThrows(
            UnresolvableBoundaryException.class,
            () -> BoundaryResolver.resolveLineStart(container, "outside", 5, size, 7)
        );
        assertThat(e.getMessage(), containsString("is not splittable ndjson"));
    }

    public void testUnterminatedTailShorterThanTheBoundResolvesToObjectSize() throws Exception {
        long size = write("tail", "head\nxx");
        assertThat(BoundaryResolver.resolveLineStart(container, "tail", 5, size, 1024), is(size));
    }

    public void testNonPositiveScanBoundRejected() throws Exception {
        long size = write("bound", "head\ntail\n");
        IllegalArgumentException zero = expectThrows(
            IllegalArgumentException.class,
            () -> BoundaryResolver.resolveLineStart(container, "bound", 5, size, 0)
        );
        assertThat(zero.getMessage(), containsString("maxScanBytes must be greater than 0"));
        IllegalArgumentException negative = expectThrows(
            IllegalArgumentException.class,
            () -> BoundaryResolver.resolveLineStart(container, "bound", 5, size, -1)
        );
        assertThat(negative.getMessage(), containsString("maxScanBytes must be greater than 0"));
    }
}
