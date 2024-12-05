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

package org.opensearch.repositories.azure;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.SuppressForbidden;
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
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import fixture.azure.AzureHttpHandler;
import reactor.core.scheduler.Schedulers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;
import static org.opensearch.repositories.blobstore.OpenSearchBlobStoreRepositoryIntegTestCase.randomBytes;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * This class tests how a {@link AzureBlobContainer} and its underlying SDK client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerRetriesTests extends OpenSearchTestCase {

    private static final long MAX_RANGE_VAL = Long.MAX_VALUE - 1L;

    private HttpServer httpServer;
    private ThreadPool threadPool;
    private AzureStorageService service;

    @Before
    public void setUp() throws Exception {
        threadPool = new TestThreadPool(getTestClass().getName(), AzureRepositoryPlugin.executorBuilder());
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/", (exchange) -> {
            exchange.sendResponseHeaders(404, 0L);
            exchange.close();
        });
        httpServer.start();
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        if (service != null) {
            service.close();
            service = null;
        }

        httpServer.stop(0);
        super.tearDown();
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    @AfterClass
    public static void shutdownSchedulers() {
        Schedulers.shutdownNow();
    }

    private BlobContainer createBlobContainer(final int maxRetries) {
        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + InetAddresses.toUriString(address.getAddress())
            + ":"
            + address.getPort()
            + "/";
        clientSettings.put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        clientSettings.put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueMillis(5000));

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "account");
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(UTF_8));
        secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);
        clientSettings.setSecureSettings(secureSettings);

        service = new AzureStorageService(clientSettings.build()) {
            @Override
            RequestRetryOptions createRetryPolicy(final AzureStorageSettings azureStorageSettings, String secondaryHost) {
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    azureStorageSettings.getMaxRetries(),
                    5,
                    10L,
                    100L,
                    secondaryHost
                );
            }

            @Override
            ParallelTransferOptions getBlobRequestOptionsForWriteBlob() {
                return new ParallelTransferOptions().setMaxSingleUploadSizeLong(ByteSizeUnit.MB.toBytes(1));
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder().put(CONTAINER_SETTING.getKey(), "container").put(ACCOUNT_SETTING.getKey(), clientName).build()
        );

        return new AzureBlobContainer(BlobPath.cleanPath(), new AzureBlobStore(repositoryMetadata, service, threadPool), threadPool);
    }

    public void testReadNonexistentBlobThrowsNoSuchFileException() {
        final BlobContainer blobContainer = createBlobContainer(between(1, 5));
        final Exception exception = expectThrows(NoSuchFileException.class, () -> {
            if (randomBoolean()) {
                blobContainer.readBlob("read_nonexistent_blob");
            } else {
                final long position = randomLongBetween(0, MAX_RANGE_VAL - 1L);
                final long length = randomLongBetween(1, MAX_RANGE_VAL - position);
                blobContainer.readBlob("read_nonexistent_blob", position, length);
            }
        });
        assertThat(exception.getMessage().toLowerCase(Locale.ROOT), containsString("404"));
    }

    public void testReadBlobWithRetries() throws Exception {
        // The request retry policy counts the first attempt as retry, so we need to
        // account for that and increase the max retry count by one.
        final int maxRetries = randomIntBetween(2, 6);
        final CountDown countDownHead = new CountDown(maxRetries - 1);
        final CountDown countDownGet = new CountDown(maxRetries - 1);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/container/read_blob_max_retries", exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    if (countDownHead.countDown()) {
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders().add("Content-Length", String.valueOf(bytes.length));
                        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                        return;
                    }
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    if (countDownGet.countDown()) {
                        final int rangeStart = getRangeStart(exchange);
                        assertThat(rangeStart, lessThan(bytes.length));
                        final int length = bytes.length - rangeStart;
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                        exchange.getResponseHeaders().add("Content-Range", "bytes " + rangeStart + "-" + bytes.length + "/" + bytes.length);
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                        exchange.getResponseBody().write(bytes, rangeStart, length);
                        return;
                    }
                }
                if (randomBoolean()) {
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                }
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        try (InputStream inputStream = blobContainer.readBlob("read_blob_max_retries")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
            // No more getProperties() calls in BlobClientBase::openInputStream(), HEAD should not be invoked
            assertThat(countDownHead.isCountedDown(), is(false));
            assertThat(countDownGet.isCountedDown(), is(true));
        }
    }

    public void testReadRangeBlobWithRetries() throws Exception {
        // The request retry policy counts the first attempt as retry, so we need to
        // account for that and increase the max retry count by one.
        final int maxRetries = randomIntBetween(2, 6);
        final CountDown countDownGet = new CountDown(maxRetries - 1);
        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/container/read_range_blob_max_retries", exchange -> {
            try {
                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                    return;
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    if (countDownGet.countDown()) {
                        final int rangeStart = getRangeStart(exchange);
                        assertThat(rangeStart, lessThan(bytes.length));
                        final Optional<Integer> rangeEnd = getRangeEnd(exchange);
                        assertThat(rangeEnd.isPresent(), is(true));
                        assertThat(rangeEnd.get(), greaterThanOrEqualTo(rangeStart));
                        final int length = (rangeEnd.get() - rangeStart) + 1;
                        assertThat(length, lessThanOrEqualTo(bytes.length - rangeStart));
                        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                        exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                        exchange.getResponseHeaders()
                            .add("Content-Range", "bytes " + rangeStart + "-" + rangeEnd.get() + "/" + bytes.length);
                        exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                        exchange.getResponseBody().write(bytes, rangeStart, length);
                        return;
                    }
                }
                if (randomBoolean()) {
                    AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                }
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        final int position = randomIntBetween(0, bytes.length - 1);
        final int length = randomIntBetween(1, bytes.length - position);
        try (InputStream inputStream = blobContainer.readBlob("read_range_blob_max_retries", position, length)) {
            final byte[] bytesRead = BytesReference.toBytes(Streams.readFully(inputStream));
            assertArrayEquals(Arrays.copyOfRange(bytes, position, Math.min(bytes.length, position + length)), bytesRead);
            assertThat(countDownGet.isCountedDown(), is(true));
        }
    }

    public void testWriteBlobWithRetries() throws Exception {
        // The request retry policy counts the first attempt as retry, so we need to
        // account for that and increase the max retry count by one.
        final int maxRetries = randomIntBetween(2, 6);
        final CountDown countDown = new CountDown(maxRetries - 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/container/write_blob_max_retries", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");

                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
                        exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    } else {
                        AzureHttpHandler.sendError(exchange, RestStatus.BAD_REQUEST);
                    }
                    exchange.close();
                    return;
                }

                if (randomBoolean()) {
                    if (randomBoolean()) {
                        Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                    } else {
                        Streams.readFully(exchange.getRequestBody());
                        AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
                    }
                }
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
            blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteLargeBlob() throws Exception {
        // The request retry policy counts the first attempt as retry, so we need to
        // account for that and increase the max retry count by one.
        final int maxRetries = randomIntBetween(3, 6);

        final int nbBlocks = randomIntBetween(1, 2);
        final byte[] data = randomBytes(BlobClient.BLOB_DEFAULT_UPLOAD_BLOCK_SIZE * nbBlocks);

        final int nbErrors = 2; // we want all requests to fail at least once
        final AtomicInteger countDownUploads = new AtomicInteger(nbErrors * nbBlocks);
        final CountDown countDownComplete = new CountDown(nbErrors);

        final Map<String, BytesReference> blocks = new ConcurrentHashMap<>();
        httpServer.createContext("/container/write_large_blob", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                final Map<String, String> params = new HashMap<>();
                if (exchange.getRequestURI().getQuery() != null) {
                    RestUtils.decodeQueryString(exchange.getRequestURI().getQuery(), 0, params);
                }

                final String blockId = params.get("blockid");
                if (Strings.hasText(blockId) && (countDownUploads.decrementAndGet() % 2 == 0)) {
                    blocks.put(blockId, Streams.readFully(exchange.getRequestBody()));
                    exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    exchange.close();
                    return;
                }

                final String complete = params.get("comp");
                if ("blocklist".equals(complete) && (countDownComplete.countDown())) {
                    final String blockList = Streams.copyToString(new InputStreamReader(exchange.getRequestBody(), UTF_8));
                    final List<String> blockUids = Arrays.stream(blockList.split("<Latest>"))
                        .filter(line -> line.contains("</Latest>"))
                        .map(line -> line.substring(0, line.indexOf("</Latest>")))
                        .collect(Collectors.toList());

                    final ByteArrayOutputStream blob = new ByteArrayOutputStream();
                    for (String blockUid : blockUids) {
                        BytesReference block = blocks.remove(blockUid);
                        assert block != null;
                        block.writeTo(blob);
                    }
                    assertArrayEquals(data, blob.toByteArray());
                    exchange.getResponseHeaders().add("x-ms-request-server-encrypted", "false");
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                    exchange.close();
                    return;
                }
            }

            if (randomBoolean()) {
                Streams.readFully(exchange.getRequestBody());
                AzureHttpHandler.sendError(exchange, randomFrom(RestStatus.INTERNAL_SERVER_ERROR, RestStatus.SERVICE_UNAVAILABLE));
            }
            exchange.close();
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries);
        try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", data), data.length)) {
            blobContainer.writeBlob("write_large_blob", stream, data.length, false);
        }
        assertThat(countDownUploads.get(), equalTo(0));
        assertThat(countDownComplete.isCountedDown(), is(true));
        assertThat(blocks.isEmpty(), is(true));
    }

    public void testRetryUntilFail() throws IOException {
        final AtomicBoolean requestReceived = new AtomicBoolean(false);
        httpServer.createContext("/container/write_blob_max_retries", exchange -> {
            try {
                if (requestReceived.compareAndSet(false, true)) {
                    throw new AssertionError("Should not receive two requests");
                } else {
                    exchange.sendResponseHeaders(RestStatus.CREATED.getStatus(), -1);
                }
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(randomIntBetween(2, 5));
        try (InputStream stream = new InputStream() {

            @Override
            public int read() throws IOException {
                throw new IOException("foo");
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public void reset() {
                throw new AssertionError("should not be called");
            }
        }) {
            final IOException ioe = expectThrows(
                IOException.class,
                () -> blobContainer.writeBlob("write_blob_max_retries", stream, randomIntBetween(1, 128), randomBoolean())
            );
            assertThat(ioe.getMessage(), is("foo"));
        }
    }

    private static byte[] randomBlobContent() {
        return randomByteArrayOfLength(randomIntBetween(1, frequently() ? 512 : 1 << 20)); // rarely up to 1mb
    }

    private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=([0-9]+)-([0-9]+)$");

    private static Tuple<Long, Long> getRanges(HttpExchange exchange) {
        final String rangeHeader = exchange.getRequestHeaders().getFirst("X-ms-range");
        if (rangeHeader == null) {
            return Tuple.tuple(0L, MAX_RANGE_VAL);
        }

        final Matcher matcher = RANGE_PATTERN.matcher(rangeHeader);
        assertTrue(rangeHeader + " matches expected pattern", matcher.matches());
        final long rangeStart = Long.parseLong(matcher.group(1));
        final long rangeEnd = Long.parseLong(matcher.group(2));
        assertThat(rangeStart, lessThanOrEqualTo(rangeEnd));
        return Tuple.tuple(rangeStart, rangeEnd);
    }

    private static int getRangeStart(HttpExchange exchange) {
        return Math.toIntExact(getRanges(exchange).v1());
    }

    private static Optional<Integer> getRangeEnd(HttpExchange exchange) {
        final long rangeEnd = getRanges(exchange).v2();
        if (rangeEnd == MAX_RANGE_VAL) {
            return Optional.empty();
        }
        return Optional.of(Math.toIntExact(rangeEnd));
    }

}
