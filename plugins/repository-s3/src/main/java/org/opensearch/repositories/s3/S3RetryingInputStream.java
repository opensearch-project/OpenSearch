/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.repositories.s3.utils.HttpRangeUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper around an S3 object that will retry the {@link GetObjectRequest} if the download fails part-way through, resuming from where
 * the failure occurred. This should be handled by the SDK but it isn't today. This should be revisited in the future (e.g. before removing
 * the {@code LegacyESVersion#V_7_0_0} version constant) and removed when the SDK handles retries itself.
 *
 * See https://github.com/aws/aws-sdk-java/issues/856 for the related SDK issue
 */
class S3RetryingInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(S3RetryingInputStream.class);

    static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private final S3BlobStore blobStore;
    private final String blobKey;
    private final long start;
    private final long end;
    private final int maxAttempts;
    private final List<IOException> failures;

    private ResponseInputStream<GetObjectResponse> currentStream;
    private final AtomicBoolean isStreamAborted = new AtomicBoolean();
    private long currentStreamLastOffset;
    private int attempt = 1;
    private long currentOffset;
    private boolean closed;
    private boolean eof;

    S3RetryingInputStream(S3BlobStore blobStore, String blobKey) throws IOException {
        this(blobStore, blobKey, 0, Long.MAX_VALUE - 1);
    }

    // both start and end are inclusive bounds, following the definition in GetObjectRequest.setRange
    S3RetryingInputStream(S3BlobStore blobStore, String blobKey, long start, long end) throws IOException {
        if (start < 0L) {
            throw new IllegalArgumentException("start must be non-negative");
        }
        if (end < start || end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("end must be >= start and not Long.MAX_VALUE");
        }
        this.blobStore = blobStore;
        this.blobKey = blobKey;
        this.maxAttempts = blobStore.getMaxRetries() + 1;
        this.failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
        this.start = start;
        this.end = end;
        openStream();
    }

    private void openStream() throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final GetObjectRequest.Builder getObjectRequest = GetObjectRequest.builder()
                .bucket(blobStore.bucket())
                .key(blobKey)
                .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().getObjectMetricPublisher));
            if (currentOffset > 0 || start > 0 || end < Long.MAX_VALUE - 1) {
                assert start + currentOffset <= end : "requesting beyond end, start = "
                    + start
                    + " offset="
                    + currentOffset
                    + " end="
                    + end;
                getObjectRequest.range(HttpRangeUtils.toHttpRangeHeader(Math.addExact(start, currentOffset), end));
            }
            final ResponseInputStream<GetObjectResponse> getObjectResponseInputStream = SocketAccess.doPrivileged(
                () -> clientReference.get().getObject(getObjectRequest.build())
            );
            this.currentStreamLastOffset = Math.addExact(
                Math.addExact(start, currentOffset),
                getStreamLength(getObjectResponseInputStream.response())
            );
            this.currentStream = getObjectResponseInputStream;
            this.isStreamAborted.set(false);
        } catch (final SdkException e) {
            if (e instanceof S3Exception) {
                if (404 == ((S3Exception) e).statusCode()) {
                    throw addSuppressedExceptions(new NoSuchFileException("Blob object [" + blobKey + "] not found: " + e.getMessage()));
                }
            }
            throw addSuppressedExceptions(e);
        }
    }

    private long getStreamLength(final GetObjectResponse getObjectResponse) {
        try {
            // Returns the content range of the object if response contains the Content-Range header.
            if (getObjectResponse.contentRange() != null) {
                final Tuple<Long, Long> s3ResponseRange = HttpRangeUtils.fromHttpRangeHeader(getObjectResponse.contentRange());
                assert s3ResponseRange.v2() >= s3ResponseRange.v1() : s3ResponseRange.v2() + " vs " + s3ResponseRange.v1();
                assert s3ResponseRange.v1() == start + currentOffset : "Content-Range start value ["
                    + s3ResponseRange.v1()
                    + "] exceeds start ["
                    + start
                    + "] + current offset ["
                    + currentOffset
                    + ']';
                assert s3ResponseRange.v2() == end : "Content-Range end value [" + s3ResponseRange.v2() + "] exceeds end [" + end + ']';
                return s3ResponseRange.v2() - s3ResponseRange.v1() + 1L;
            }
            return getObjectResponse.contentLength();
        } catch (Exception e) {
            assert false : e;
            return Long.MAX_VALUE - 1L; // assume a large stream so that the underlying stream is aborted on closing, unless eof is reached
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int result = currentStream.read();
                if (result == -1) {
                    eof = true;
                    return -1;
                }
                currentOffset += 1;
                return result;
            } catch (IOException e) {
                reopenStreamOrFail(e);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int bytesRead = currentStream.read(b, off, len);
                if (bytesRead == -1) {
                    eof = true;
                    return -1;
                }
                currentOffset += bytesRead;
                return bytesRead;
            } catch (IOException e) {
                reopenStreamOrFail(e);
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using S3RetryingInputStream after close";
            throw new IllegalStateException("using S3RetryingInputStream after close");
        }
    }

    private void reopenStreamOrFail(IOException e) throws IOException {
        if (attempt >= maxAttempts) {
            logger.debug(
                new ParameterizedMessage(
                    "failed reading [{}/{}] at offset [{}], attempt [{}] of [{}], giving up",
                    blobStore.bucket(),
                    blobKey,
                    start + currentOffset,
                    attempt,
                    maxAttempts
                ),
                e
            );
            throw addSuppressedExceptions(e);
        }
        logger.debug(
            new ParameterizedMessage(
                "failed reading [{}/{}] at offset [{}], attempt [{}] of [{}], retrying",
                blobStore.bucket(),
                blobKey,
                start + currentOffset,
                attempt,
                maxAttempts
            ),
            e
        );
        attempt += 1;
        if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
            failures.add(e);
        }
        maybeAbort(currentStream);
        IOUtils.closeWhileHandlingException(currentStream);
        openStream();
    }

    @Override
    public void close() throws IOException {
        maybeAbort(currentStream);
        try {
            currentStream.close();
        } finally {
            closed = true;
        }
    }

    /**
     * Abort the {@link ResponseInputStream} if it wasn't read completely at the time this method is called,
     * suppressing all thrown exceptions.
     */
    private void maybeAbort(ResponseInputStream<GetObjectResponse> stream) {
        if (isEof() || isAborted()) {
            return;
        }
        try {
            if (start + currentOffset < currentStreamLastOffset) {
                stream.abort();
                isStreamAborted.compareAndSet(false, true);
            }
        } catch (Exception e) {
            logger.warn("Failed to abort stream before closing", e);
        }
    }

    @Override
    public long skip(long n) {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    private <T extends Exception> T addSuppressedExceptions(T e) {
        for (IOException failure : failures) {
            e.addSuppressed(failure);
        }
        return e;
    }

    // package-private for tests
    boolean isEof() {
        return eof || start + currentOffset == currentStreamLastOffset;
    }

    // package-private for tests
    boolean isAborted() {
        return isStreamAborted.get();
    }
}
