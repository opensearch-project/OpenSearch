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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.repositories.blobstore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.opensearch.common.Nullable;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.Streams;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.test.OpenSearchTestCase;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@SuppressForbidden(reason = "use a http server")
public abstract class AbstractBlobContainerRetriesTestCase extends OpenSearchTestCase {

    private static final long MAX_RANGE_VAL = Long.MAX_VALUE - 1;

    protected HttpServer httpServer;

    @Before
    public void setUp() throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        httpServer.stop(0);
        super.tearDown();
    }

    protected abstract String downloadStorageEndpoint(String blob);

    protected abstract String bytesContentType();

    protected abstract Class<? extends Exception> unresponsiveExceptionType();

    protected abstract BlobContainer createBlobContainer(
        @Nullable Integer maxRetries,
        @Nullable TimeValue readTimeout,
        @Nullable Boolean disableChunkedEncoding,
        @Nullable ByteSizeValue bufferSize
    );

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5), null, null, null);
        final long position = randomLongBetween(0, MAX_RANGE_VAL);
        final int length = randomIntBetween(1, Math.toIntExact(Math.min(Integer.MAX_VALUE, MAX_RANGE_VAL - position)));
        final Exception exception = expectThrows(NoSuchFileException.class, () -> {
            if (randomBoolean()) {
                Streams.readFully(blobContainer.readBlob("read_nonexistent_blob"));
            } else {
                Streams.readFully(blobContainer.readBlob("read_nonexistent_blob", 0, 1));
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("blob object [read_nonexistent_blob] not found"));
        assertThat(
            expectThrows(
                NoSuchFileException.class,
                () -> Streams.readFully(blobContainer.readBlob("read_nonexistent_blob", position, length))
            ).getMessage().toLowerCase(Locale.ROOT),
            containsString("blob object [read_nonexistent_blob] not found")
        );
    }

    public void testReadBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint("read_blob_max_retries"), exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                final int rangeStart = getRangeStart(exchange);
                assertThat(rangeStart, lessThan(bytes.length));
                exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                exchange.sendResponseHeaders(HttpStatus.SC_OK, bytes.length - rangeStart);
                exchange.getResponseBody().write(bytes, rangeStart, bytes.length - rangeStart);
                exchange.close();
                return;
            }
            if (randomBoolean()) {
                exchange.sendResponseHeaders(
                    randomFrom(
                        HttpStatus.SC_INTERNAL_SERVER_ERROR,
                        HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        HttpStatus.SC_GATEWAY_TIMEOUT
                    ),
                    -1
                );
            } else if (randomBoolean()) {
                sendIncompleteContent(exchange, bytes);
            }
            if (randomBoolean()) {
                exchange.close();
            }
        });

        final TimeValue readTimeout = TimeValue.timeValueSeconds(between(1, 3));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);
        try (InputStream inputStream = blobContainer.readBlob("read_blob_max_retries")) {
            final int readLimit;
            final InputStream wrappedStream;
            if (randomBoolean()) {
                // read stream only partly
                readLimit = randomIntBetween(0, bytes.length);
                wrappedStream = Streams.limitStream(inputStream, readLimit);
            } else {
                readLimit = bytes.length;
                wrappedStream = inputStream;
            }
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(wrappedStream));
            logger.info("maxRetries={}, readLimit={}, byteSize={}, bytesRead={}", maxRetries, readLimit, bytes.length, bytesRead.length);
            assertArrayEquals(Arrays.copyOfRange(bytes, 0, readLimit), bytesRead);
            if (readLimit < bytes.length) {
                // we might have completed things based on an incomplete response, and we're happy with that
            } else {
                assertTrue(countDown.isCountedDown());
            }
        }
    }

    public void testReadRangeBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint("read_range_blob_max_retries"), exchange -> {
            Streams.readFully(exchange.getRequestBody());
            if (countDown.countDown()) {
                final int rangeStart = getRangeStart(exchange);
                assertThat(rangeStart, lessThan(bytes.length));
                assertTrue(getRangeEnd(exchange).isPresent());
                final int rangeEnd = getRangeEnd(exchange).get();
                assertThat(rangeEnd, greaterThanOrEqualTo(rangeStart));
                // adapt range end to be compliant to https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
                final int effectiveRangeEnd = Math.min(bytes.length - 1, rangeEnd);
                final int length = (effectiveRangeEnd - rangeStart) + 1;
                exchange.getResponseHeaders().add("Content-Type", bytesContentType());
                exchange.sendResponseHeaders(HttpStatus.SC_OK, length);
                exchange.getResponseBody().write(bytes, rangeStart, length);
                exchange.close();
                return;
            }
            if (randomBoolean()) {
                exchange.sendResponseHeaders(
                    randomFrom(
                        HttpStatus.SC_INTERNAL_SERVER_ERROR,
                        HttpStatus.SC_BAD_GATEWAY,
                        HttpStatus.SC_SERVICE_UNAVAILABLE,
                        HttpStatus.SC_GATEWAY_TIMEOUT
                    ),
                    -1
                );
            } else if (randomBoolean()) {
                sendIncompleteContent(exchange, bytes);
            }
            if (randomBoolean()) {
                exchange.close();
            }
        });

        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 500));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);
        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(0, randomBoolean() ? bytes.length : Integer.MAX_VALUE);
        try (InputStream inputStream = blobContainer.readBlob("read_range_blob_max_retries", position, length)) {
            final int readLimit;
            final InputStream wrappedStream;
            if (randomBoolean()) {
                // read stream only partly
                readLimit = randomIntBetween(0, length);
                wrappedStream = Streams.limitStream(inputStream, readLimit);
            } else {
                readLimit = length;
                wrappedStream = inputStream;
            }
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(wrappedStream));
            logger.info(
                "maxRetries={}, position={}, length={}, readLimit={}, byteSize={}, bytesRead={}",
                maxRetries,
                position,
                length,
                readLimit,
                bytes.length,
                bytesRead.length
            );
            assertArrayEquals(Arrays.copyOfRange(bytes, position, Math.min(bytes.length, position + readLimit)), bytesRead);
            if (readLimit == 0 || (readLimit < length && readLimit == bytesRead.length)) {
                // we might have completed things based on an incomplete response, and we're happy with that
            } else {
                assertTrue(countDown.isCountedDown());
            }
        }
    }

    public void testReadBlobWithReadTimeouts() {
        final int maxRetries = randomInt(5);
        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 200));
        final BlobContainer blobContainer = createBlobContainer(maxRetries, readTimeout, null, null);

        // HTTP server does not send a response
        httpServer.createContext(downloadStorageEndpoint("read_blob_unresponsive"), exchange -> {});

        Exception exception = expectThrows(
            unresponsiveExceptionType(),
            () -> Streams.readFully(blobContainer.readBlob("read_blob_unresponsive"))
        );
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
        assertThat(exception.getCause(), instanceOf(SocketTimeoutException.class));

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent();
        httpServer.createContext(downloadStorageEndpoint("read_blob_incomplete"), exchange -> sendIncompleteContent(exchange, bytes));

        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(1, randomBoolean() ? bytes.length : Integer.MAX_VALUE);
        exception = expectThrows(Exception.class, () -> {
            try (
                InputStream stream = randomBoolean()
                    ? blobContainer.readBlob("read_blob_incomplete")
                    : blobContainer.readBlob("read_blob_incomplete", position, length)
            ) {
                Streams.readFully(stream);
            }
        });
        assertThat(
            exception,
            either(instanceOf(SocketTimeoutException.class)).or(instanceOf(ConnectionClosedException.class))
                .or(instanceOf(RuntimeException.class))
        );
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            either(containsString("read timed out")).or(containsString("premature end of chunk coded message body: closing chunk expected"))
                .or(containsString("Read timed out"))
                .or(containsString("unexpected end of file from server"))
        );
        assertThat(exception.getSuppressed().length, equalTo(maxRetries));
    }

    public void testReadBlobWithNoHttpResponse() {
        final TimeValue readTimeout = TimeValue.timeValueMillis(between(100, 200));
        final BlobContainer blobContainer = createBlobContainer(randomInt(5), readTimeout, null, null);

        // HTTP server closes connection immediately
        httpServer.createContext(downloadStorageEndpoint("read_blob_no_response"), HttpExchange::close);

        Exception exception = expectThrows(unresponsiveExceptionType(), () -> {
            if (randomBoolean()) {
                Streams.readFully(blobContainer.readBlob("read_blob_no_response"));
            } else {
                Streams.readFully(blobContainer.readBlob("read_blob_no_response", 0, 1));
            }
        });
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            either(containsString("the target server failed to respond")).or(containsString("unexpected end of file from server"))
        );
    }

    public void testReadBlobWithPrematureConnectionClose() {
        final int maxRetries = randomInt(20);
        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, null, null);

        // HTTP server sends a partial response
        final byte[] bytes = randomBlobContent(1);
        httpServer.createContext(downloadStorageEndpoint("read_blob_incomplete"), exchange -> {
            sendIncompleteContent(exchange, bytes);
            exchange.close();
        });

        final Exception exception = expectThrows(Exception.class, () -> {
            try (
                InputStream stream = randomBoolean()
                    ? blobContainer.readBlob("read_blob_incomplete", 0, 1)
                    : blobContainer.readBlob("read_blob_incomplete")
            ) {
                Streams.readFully(stream);
            }
        });
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            either(containsString("premature end of chunk coded message body: closing chunk expected")).or(
                containsString("premature end of content-length delimited message body")
            ).or(containsString("connection closed prematurely"))
        );
        assertThat(exception.getSuppressed().length, equalTo(Math.min(10, maxRetries)));
    }

    protected static byte[] randomBlobContent() {
        return randomBlobContent(1);
    }

    protected static byte[] randomBlobContent(int minSize) {
        return randomByteArrayOfLength(randomIntBetween(minSize, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");

    protected static Tuple<Long, Long> getRange(HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
        if (rangeHeader == null) {
            return Tuple.tuple(0L, MAX_RANGE_VAL);
        }

        final Matcher matcher = RANGE_PATTERN.matcher(rangeHeader);
        assertTrue(rangeHeader + " matches expected pattern", matcher.matches());
        long rangeStart = Long.parseLong(matcher.group(1));
        long rangeEnd = Long.parseLong(matcher.group(2));
        assertThat(rangeStart, lessThanOrEqualTo(rangeEnd));
        return Tuple.tuple(rangeStart, rangeEnd);
    }

    protected static int getRangeStart(HttpExchange exchange) {
        return Math.toIntExact(getRange(exchange).v1());
    }

    protected static Optional<Integer> getRangeEnd(HttpExchange exchange) {
        final long rangeEnd = getRange(exchange).v2();
        if (rangeEnd == MAX_RANGE_VAL) {
            return Optional.empty();
        }
        return Optional.of(Math.toIntExact(rangeEnd));
    }

    protected void sendIncompleteContent(HttpExchange exchange, byte[] bytes) throws IOException {
        final int rangeStart = getRangeStart(exchange);
        assertThat(rangeStart, lessThan(bytes.length));
        final Optional<Integer> rangeEnd = getRangeEnd(exchange);
        final int length;
        if (rangeEnd.isPresent()) {
            // adapt range end to be compliant to https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
            final int effectiveRangeEnd = Math.min(rangeEnd.get(), bytes.length - 1);
            length = effectiveRangeEnd - rangeStart + 1;
        } else {
            length = bytes.length - rangeStart;
        }
        exchange.getResponseHeaders().add("Content-Type", bytesContentType());
        exchange.sendResponseHeaders(HttpStatus.SC_OK, length);
        int minSend = Math.min(0, length - 1);
        final int bytesToSend = randomIntBetween(minSend, length - 1);
        if (bytesToSend > 0) {
            exchange.getResponseBody().write(bytes, rangeStart, bytesToSend);
        }
        if (randomBoolean()) {
            exchange.getResponseBody().flush();
        }
    }
}
