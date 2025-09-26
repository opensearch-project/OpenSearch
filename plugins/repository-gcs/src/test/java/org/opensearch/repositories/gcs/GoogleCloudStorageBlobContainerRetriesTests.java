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

package org.opensearch.repositories.gcs;

import com.sun.net.httpserver.HttpHandler;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.apache.hc.core5.http.HttpStatus;
import org.opensearch.common.Nullable;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.Streams;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.opensearch.repositories.blobstore.OpenSearchMockAPIBasedRepositoryIntegTestCase;
import org.opensearch.rest.RestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import fixture.gcs.ContentHttpHeadersParser;
import fixture.gcs.FakeOAuth2HttpHandler;
import org.threeten.bp.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.repositories.blobstore.OpenSearchBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.opensearch.repositories.gcs.TestUtils.createServiceAccount;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static fixture.gcs.GoogleCloudStorageHttpHandler.parseMultipartRequestBody;

@SuppressForbidden(reason = "use a http server")
public class GoogleCloudStorageBlobContainerRetriesTests extends AbstractBlobContainerRetriesTestCase {

    private String httpServerUrl() {
        assertThat(httpServer, notNullValue());
        InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    @Override
    protected String downloadStorageEndpoint(String blob) {
        return "/download/storage/v1/b/bucket/o/" + blob;
    }

    @Override
    protected String bytesContentType() {
        return "application/octet-stream";
    }

    @Override
    protected Class<? extends Exception> unresponsiveExceptionType() {
        return StorageException.class;
    }

    @Override
    protected BlobContainer createBlobContainer(
        final @Nullable Integer maxRetries,
        final @Nullable TimeValue readTimeout,
        final @Nullable Boolean disableChunkedEncoding,
        final @Nullable ByteSizeValue bufferSize
    ) {
        final Settings.Builder clientSettings = Settings.builder();
        final String client = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        clientSettings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl());
        clientSettings.put(TOKEN_URI_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl() + "/token");
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(client).getKey(), readTimeout);
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(client).getKey(), createServiceAccount(random()));
        clientSettings.setSecureSettings(secureSettings);

        final GoogleCloudStorageService service = new GoogleCloudStorageService() {
            @Override
            StorageOptions createStorageOptions(
                final GoogleCloudStorageClientSettings clientSettings,
                final HttpTransportOptions httpTransportOptions
            ) {
                StorageOptions options = super.createStorageOptions(clientSettings, httpTransportOptions);
                RetrySettings.Builder retrySettingsBuilder = RetrySettings.newBuilder()
                    .setTotalTimeout(options.getRetrySettings().getTotalTimeout())
                    .setInitialRetryDelay(Duration.ofMillis(10L))
                    .setRetryDelayMultiplier(1.0d)
                    .setMaxRetryDelay(Duration.ofSeconds(1L))
                    .setInitialRpcTimeout(Duration.ofSeconds(1))
                    .setRpcTimeoutMultiplier(options.getRetrySettings().getRpcTimeoutMultiplier())
                    .setMaxRpcTimeout(Duration.ofSeconds(1));
                if (maxRetries != null) {
                    retrySettingsBuilder.setMaxAttempts(maxRetries + 1);
                }
                return options.toBuilder()
                    .setHost(options.getHost())
                    .setCredentials(options.getCredentials())
                    .setRetrySettings(retrySettingsBuilder.build())
                    .setStorageRetryStrategy(new GoogleShouldRetryStorageStrategy())
                    .build();
            }
        };
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(clientSettings.build()));

        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
        final GoogleCloudStorageBlobStore blobStore = new GoogleCloudStorageBlobStore(
            "bucket",
            client,
            "repo",
            service,
            randomIntBetween(1, 8) * 1024
        );

        return new GoogleCloudStorageBlobContainer(BlobPath.cleanPath(), blobStore);
    }

    public void testReadLargeBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final AtomicInteger countDown = new AtomicInteger(maxRetries);

        // SDK reads in 2 MB chunks so we use twice that to simulate 2 chunks
        final byte[] bytes = randomBytes(1 << 22);
        httpServer.createContext("/download/storage/v1/b/bucket/o/large_blob_retries", exchange -> {
            Streams.readFully(exchange.getRequestBody());
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            final Tuple<Long, Long> range = getRange(exchange);
            final int offset = Math.toIntExact(range.v1());
            final byte[] chunk = Arrays.copyOfRange(bytes, offset, Math.toIntExact(Math.min(range.v2() + 1, bytes.length)));
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), chunk.length);
            if (randomBoolean() && countDown.decrementAndGet() >= 0) {
                exchange.getResponseBody().write(chunk, 0, chunk.length - 1);
                exchange.close();
                return;
            }
            exchange.getResponseBody().write(chunk);
            exchange.close();
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, null, null);
        try (InputStream inputStream = blobContainer.readBlob("large_blob_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
        }
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomIntBetween(2, 10);
        final CountDown countDown = new CountDown(maxRetries);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            assertThat(exchange.getRequestURI().getQuery(), containsString("uploadType=multipart"));
            if (countDown.countDown()) {
                Optional<Tuple<String, BytesReference>> content = parseMultipartRequestBody(exchange.getRequestBody());
                assertThat(content.isPresent(), is(true));
                assertThat(content.get().v1(), equalTo("write_blob_max_retries"));
                if (Objects.deepEquals(bytes, BytesReference.toBytes(content.get().v2()))) {
                    byte[] response = String.format("""
                        {"bucket":"bucket","name":"%s"}""", content.get().v1()).getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                } else {
                    exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
                }
                return;
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                    exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
                }
            }
        }));

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, null, null);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteBlobWithReadTimeouts() {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = createBlobContainer(1, readTimeout, null, null);

        // HTTP server does not send a response
        httpServer.createContext("/upload/storage/v1/b/bucket/o", exchange -> {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, bytes.length - 1)]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                }
            }
        });

        Exception exception = expectThrows(StorageException.class, () -> {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                blobContainer.writeBlob("write_blob_timeout", stream, bytes.length, false);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));

        assertThat(exception.getCause(), instanceOf(SocketTimeoutException.class));
        assertThat(exception.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
    }

    public void testWriteLargeBlob() throws IOException {
        final int defaultChunkSize = Math.toIntExact(ByteSizeValue.parseBytesSizeValue("16mb", "aaa").getBytes());
        final int nbChunks = randomIntBetween(3, 5);
        final int lastChunkSize = randomIntBetween(1, defaultChunkSize - 1);
        final int totalChunks = nbChunks + 1;
        final byte[] data = randomBytes(defaultChunkSize * nbChunks + lastChunkSize);
        assertThat(data.length, greaterThan(GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE));

        logger.debug(
            "resumable upload is composed of [{}] total chunks ([{}] chunks of length [{}] and last chunk of length [{}]",
            totalChunks,
            nbChunks,
            defaultChunkSize,
            lastChunkSize
        );

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger countInits = new AtomicInteger(nbErrors);
        final AtomicInteger countUploads = new AtomicInteger(nbErrors * totalChunks);
        final AtomicBoolean allow410Gone = new AtomicBoolean(randomBoolean());
        final AtomicBoolean allowReadTimeout = new AtomicBoolean(rarely());
        final AtomicInteger bytesReceived = new AtomicInteger();
        final int wrongChunk = randomIntBetween(1, totalChunks);

        final AtomicReference<String> sessionUploadId = new AtomicReference<>(UUIDs.randomBase64UUID());
        logger.debug("starting with resumable upload id [{}]", sessionUploadId.get());

        httpServer.createContext("/upload/storage/v1/b/bucket/o", safeHandler(exchange -> {
            final BytesReference requestBody = Streams.readFully(exchange.getRequestBody());

            final Map<String, String> params = new HashMap<>();
            RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
            assertThat(params.get("uploadType"), equalTo("resumable"));

            if ("POST".equals(exchange.getRequestMethod())) {
                assertThat(params.get("name"), equalTo("write_large_blob"));
                if (countInits.decrementAndGet() <= 0) {
                    byte[] response = requestBody.utf8ToString().getBytes(UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.getResponseHeaders()
                        .add(
                            "Location",
                            httpServerUrl() + "/upload/storage/v1/b/bucket/o?uploadType=resumable&upload_id=" + sessionUploadId.get()
                        );
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                    exchange.getResponseBody().write(response);
                    return;
                }
                if (allowReadTimeout.get()) {
                    assertThat(wrongChunk, greaterThan(0));
                    return;
                }
            } else if ("PUT".equals(exchange.getRequestMethod())) {
                final String uploadId = params.get("upload_id");
                if (uploadId.equals(sessionUploadId.get()) == false) {
                    logger.debug("session id [{}] is gone", uploadId);
                    assertThat(wrongChunk, greaterThan(0));
                    exchange.sendResponseHeaders(HttpStatus.SC_GONE, -1);
                    return;
                }

                if (countUploads.get() == (wrongChunk * nbErrors)) {
                    if (allowReadTimeout.compareAndSet(true, false)) {
                        assertThat(wrongChunk, greaterThan(0));
                        return;
                    }
                    if (allow410Gone.compareAndSet(true, false)) {
                        final String newUploadId = UUIDs.randomBase64UUID(random());
                        logger.debug("chunk [{}] gone, updating session ids [{} -> {}]", wrongChunk, sessionUploadId.get(), newUploadId);
                        sessionUploadId.set(newUploadId);

                        // we must reset the counters because the whole object upload will be retried
                        countInits.set(nbErrors);
                        countUploads.set(nbErrors * totalChunks);
                        bytesReceived.set(0);

                        exchange.sendResponseHeaders(HttpStatus.SC_GONE, -1);
                        return;
                    }
                }

                final String contentRangeHeaderValue = exchange.getRequestHeaders().getFirst("Content-Range");
                final var contentRange = ContentHttpHeadersParser.parseContentRangeHeader(contentRangeHeaderValue);
                assertNotNull("Invalid content range header: " + contentRangeHeaderValue, contentRange);

                if (!contentRange.hasRange()) {
                    // Content-Range: */... is a status check
                    // https://cloud.google.com/storage/docs/performing-resumable-uploads#status-check
                    final int receivedSoFar = bytesReceived.get();
                    if (receivedSoFar > 0) {
                        exchange.getResponseHeaders().add("Range", String.format("bytes=0-%s", receivedSoFar));
                    }
                    exchange.getResponseHeaders().add("Content-Length", "0");
                    exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                    return;
                }

                if (countUploads.decrementAndGet() % 2 == 0) {
                    assertThat(Math.toIntExact(requestBody.length()), anyOf(equalTo(defaultChunkSize), equalTo(lastChunkSize)));
                    final int rangeStart = Math.toIntExact(contentRange.start());
                    final int rangeEnd = Math.toIntExact(contentRange.end());
                    assertThat(rangeEnd + 1 - rangeStart, equalTo(Math.toIntExact(requestBody.length())));
                    assertThat(new BytesArray(data, rangeStart, rangeEnd - rangeStart + 1), is(requestBody));
                    bytesReceived.updateAndGet(existing -> Math.max(existing, rangeEnd));

                    if (contentRange.size() != null) {
                        exchange.getResponseHeaders().add("x-goog-stored-content-length", String.valueOf(bytesReceived.get() + 1));
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                        return;
                    } else {
                        exchange.getResponseHeaders().add("Range", String.format("bytes=%s-%s", rangeStart, rangeEnd));
                        exchange.getResponseHeaders().add("Content-Length", "0");
                        exchange.sendResponseHeaders(308 /* Resume Incomplete */, -1);
                        return;
                    }
                }
            }

            if (randomBoolean()) {
                exchange.sendResponseHeaders(HttpStatus.SC_INTERNAL_SERVER_ERROR, -1);
            }
        }));

        final TimeValue readTimeout = allowReadTimeout.get() ? TimeValue.timeValueSeconds(3) : null;

        final BlobContainer blobContainer = createBlobContainer(nbErrors + 1, readTimeout, null, null);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
            blobContainer.writeBlob("write_large_blob", stream, data.length, false);
        }

        assertThat(countInits.get(), equalTo(0));
        assertThat(countUploads.get(), equalTo(0));
        assertThat(allow410Gone.get(), is(false));
    }

    private HttpHandler safeHandler(HttpHandler handler) {
        final HttpHandler loggingHandler = OpenSearchMockAPIBasedRepositoryIntegTestCase.wrap(handler, logger);
        return exchange -> {
            try {
                loggingHandler.handle(exchange);
            } finally {
                exchange.close();
            }
        };
    }
}
