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

import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.Nullable;
import org.opensearch.common.StreamContext;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.VerifyingMultiStreamBlobContainer;
import org.opensearch.common.blobstore.stream.write.StreamContextSupplier;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.hash.MessageDigests;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.io.Streams;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.opensearch.repositories.blobstore.ZeroInputStream;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.io.SdkDigestInputStream;
import software.amazon.awssdk.utils.internal.Base16;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.opensearch.repositories.s3.S3ClientSettings.DISABLE_CHUNKED_ENCODING;
import static org.opensearch.repositories.s3.S3ClientSettings.ENDPOINT_SETTING;
import static org.opensearch.repositories.s3.S3ClientSettings.REGION;
import static org.opensearch.repositories.s3.S3ClientSettings.MAX_RETRIES_SETTING;
import static org.opensearch.repositories.s3.S3ClientSettings.READ_TIMEOUT_SETTING;

/**
 * This class tests how a {@link S3BlobContainer} and its underlying AWS S3 client are retrying requests when reading or writing blobs.
 */
@SuppressForbidden(reason = "use a http server")
public class S3BlobContainerRetriesTests extends AbstractBlobContainerRetriesTestCase implements ConfigPathSupport {

    private S3Service service;
    private String previousOpenSearchPathConf;
    private S3AsyncService asyncService;
    private ExecutorService futureCompletionService;
    private ExecutorService streamReaderService;
    private AsyncTransferEventLoopGroup transferNIOGroup;

    @Before
    public void setUp() throws Exception {
        previousOpenSearchPathConf = SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        service = new S3Service(configPath());
        asyncService = new S3AsyncService(configPath());

        futureCompletionService = Executors.newSingleThreadExecutor();
        streamReaderService = Executors.newSingleThreadExecutor();
        transferNIOGroup = new AsyncTransferEventLoopGroup(1);

        // needed by S3AsyncService
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(service, asyncService);

        streamReaderService.shutdown();
        futureCompletionService.shutdown();
        IOUtils.close(transferNIOGroup);

        if (previousOpenSearchPathConf != null) {
            SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", previousOpenSearchPathConf));
        } else {
            SocketAccess.doPrivileged(() -> System.clearProperty("opensearch.path.conf"));
        }
        super.tearDown();
    }

    @Override
    protected String downloadStorageEndpoint(String blob) {
        return "/bucket/" + blob;
    }

    @Override
    protected String bytesContentType() {
        return "text/plain; charset=utf-8";
    }

    @Override
    protected Class<? extends Exception> unresponsiveExceptionType() {
        return SdkClientException.class;
    }

    @Override
    protected VerifyingMultiStreamBlobContainer createBlobContainer(
        final @Nullable Integer maxRetries,
        final @Nullable TimeValue readTimeout,
        final @Nullable Boolean disableChunkedEncoding,
        final @Nullable ByteSizeValue bufferSize
    ) {
        final Settings.Builder clientSettings = Settings.builder();
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
        clientSettings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        clientSettings.put(REGION.getConcreteSettingForNamespace(clientName).getKey(), "region");

        if (maxRetries != null) {
            clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries);
        }
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), readTimeout);
        }
        if (disableChunkedEncoding != null) {
            clientSettings.put(DISABLE_CHUNKED_ENCODING.getConcreteSettingForNamespace(clientName).getKey(), disableChunkedEncoding);
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "access");
        secureSettings.setString(S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "secret");
        clientSettings.setSecureSettings(secureSettings);
        service.refreshAndClearCache(S3ClientSettings.load(clientSettings.build(), configPath()));
        asyncService.refreshAndClearCache(S3ClientSettings.load(clientSettings.build(), configPath()));

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            S3Repository.TYPE,
            Settings.builder().put(S3Repository.CLIENT_NAME.getKey(), clientName).build()
        );

        AsyncExecutorContainer asyncExecutorContainer = new AsyncExecutorContainer(
            futureCompletionService,
            streamReaderService,
            transferNIOGroup
        );

        return new S3BlobContainer(
            BlobPath.cleanPath(),
            new S3BlobStore(
                service,
                asyncService,
                true,
                "bucket",
                S3Repository.SERVER_SIDE_ENCRYPTION_SETTING.getDefault(Settings.EMPTY),
                bufferSize == null ? S3Repository.BUFFER_SIZE_SETTING.getDefault(Settings.EMPTY) : bufferSize,
                S3Repository.CANNED_ACL_SETTING.getDefault(Settings.EMPTY),
                S3Repository.STORAGE_CLASS_SETTING.getDefault(Settings.EMPTY),
                repositoryMetadata,
                new AsyncTransferManager(
                    S3Repository.PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING.getDefault(Settings.EMPTY).getBytes(),
                    asyncExecutorContainer.getStreamReader(),
                    asyncExecutorContainer.getStreamReader()
                ),
                asyncExecutorContainer,
                asyncExecutorContainer
            )
        ) {
            @Override
            public InputStream readBlob(String blobName) throws IOException {
                return new AssertingInputStream(super.readBlob(blobName), blobName);
            }

            @Override
            public InputStream readBlob(String blobName, long position, long length) throws IOException {
                return new AssertingInputStream(super.readBlob(blobName, position, length), blobName, position, length);
            }
        };
    }

    public void testWriteBlobWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/bucket/write_blob_max_retries", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery() == null) {
                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                    } else {
                        exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
                    }
                    exchange.close();
                    return;
                }

                if (randomBoolean()) {
                    if (randomBoolean()) {
                        Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                    } else {
                        Streams.readFully(exchange.getRequestBody());
                        exchange.sendResponseHeaders(
                            randomFrom(
                                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                                HttpStatus.SC_BAD_GATEWAY,
                                HttpStatus.SC_SERVICE_UNAVAILABLE,
                                HttpStatus.SC_GATEWAY_TIMEOUT
                            ),
                            -1
                        );
                    }
                }
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, true, null);
        try (InputStream stream = new ByteArrayInputStream(bytes)) {
            blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteBlobByStreamsWithRetries() throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final byte[] bytes = randomBlobContent();
        httpServer.createContext("/bucket/write_blob_by_streams_max_retries", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery() == null) {
                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                    } else {
                        exchange.sendResponseHeaders(HttpStatus.SC_BAD_REQUEST, -1);
                    }
                    exchange.close();
                    return;
                }

                if (randomBoolean()) {
                    if (randomBoolean()) {
                        Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.max(1, bytes.length - 1))]);
                    } else {
                        Streams.readFully(exchange.getRequestBody());
                        exchange.sendResponseHeaders(
                            randomFrom(
                                HttpStatus.SC_INTERNAL_SERVER_ERROR,
                                HttpStatus.SC_BAD_GATEWAY,
                                HttpStatus.SC_SERVICE_UNAVAILABLE,
                                HttpStatus.SC_GATEWAY_TIMEOUT
                            ),
                            -1
                        );
                    }
                }
                exchange.close();
            }
        });

        final VerifyingMultiStreamBlobContainer blobContainer = createBlobContainer(maxRetries, null, true, null);
        List<InputStream> openInputStreams = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        ActionListener<Void> completionListener = ActionListener.wrap(resp -> { countDownLatch.countDown(); }, ex -> {
            exceptionRef.set(ex);
            countDownLatch.countDown();
        });
        blobContainer.asyncBlobUpload(new WriteContext("write_blob_by_streams_max_retries", new StreamContextSupplier() {
            @Override
            public StreamContext supplyStreamContext(long partSize) {
                return new StreamContext(new CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException>() {
                    @Override
                    public InputStreamContainer apply(Integer partNo, Long size, Long position) throws IOException {
                        InputStream inputStream = new OffsetRangeIndexInputStream(new ByteArrayIndexInput("desc", bytes), size, position);
                        openInputStreams.add(inputStream);
                        return new InputStreamContainer(inputStream, size, position);
                    }
                }, partSize, calculateLastPartSize(bytes.length, partSize), calculateNumberOfParts(bytes.length, partSize));
            }
        }, bytes.length, false, WritePriority.NORMAL, Assert::assertTrue, false, null), completionListener);

        assertTrue(countDownLatch.await(5000, TimeUnit.SECONDS));

        assertThat(countDown.isCountedDown(), is(true));

        openInputStreams.forEach(inputStream -> {
            try {
                inputStream.close();
            } catch (IOException e) {
                fail("Failure while closing open input streams");
            }
        });
    }

    private long calculateLastPartSize(long totalSize, long partSize) {
        return totalSize % partSize == 0 ? partSize : totalSize % partSize;
    }

    private int calculateNumberOfParts(long contentLength, long partSize) {
        return (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);
    }

    public void testWriteBlobWithReadTimeouts() {
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = createBlobContainer(1, readTimeout, true, null);

        // HTTP server does not send a response
        httpServer.createContext("/bucket/write_blob_timeout", exchange -> {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, bytes.length - 1)]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                }
            }
        });

        Exception exception = expectThrows(IOException.class, () -> {
            try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                blobContainer.writeBlob("write_blob_timeout", stream, bytes.length, false);
            }
        });
        assertThat(
            exception.getMessage().toLowerCase(Locale.ROOT),
            containsString("unable to upload object [write_blob_timeout] using a single upload")
        );

        assertThat(exception.getCause(), instanceOf(SdkClientException.class));
        assertThat(exception.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));

        assertThat(exception.getCause().getCause(), instanceOf(SocketTimeoutException.class));
        assertThat(exception.getCause().getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
    }

    public void testWriteLargeBlob() throws Exception {
        final boolean useTimeout = rarely();
        final TimeValue readTimeout = useTimeout ? TimeValue.timeValueMillis(randomIntBetween(100, 500)) : null;
        final ByteSizeValue bufferSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        final BlobContainer blobContainer = createBlobContainer(null, readTimeout, true, bufferSize);

        final int parts = randomIntBetween(1, 5);
        final long lastPartSize = randomLongBetween(10, 512);
        final long blobSize = (parts * bufferSize.getBytes()) + lastPartSize;

        final int nbErrors = 2; // we want all requests to fail at least once
        final CountDown countDownInitiate = new CountDown(nbErrors);
        final AtomicInteger countDownUploads = new AtomicInteger(nbErrors * (parts + 1));
        final CountDown countDownComplete = new CountDown(nbErrors);

        httpServer.createContext("/bucket/write_large_blob", exchange -> {
            final long contentLength = Long.parseLong(exchange.getRequestHeaders().getFirst("Content-Length"));

            if ("POST".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery().equals("uploads")) {
                // initiate multipart upload request
                if (countDownInitiate.countDown()) {
                    byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "<InitiateMultipartUploadResult>\n"
                        + "  <Bucket>bucket</Bucket>\n"
                        + "  <Key>write_large_blob</Key>\n"
                        + "  <UploadId>TEST</UploadId>\n"
                        + "</InitiateMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
            } else if ("PUT".equals(exchange.getRequestMethod())
                && exchange.getRequestURI().getQuery().contains("uploadId=TEST")
                && exchange.getRequestURI().getQuery().contains("partNumber=")) {
                    // upload part request
                    SdkDigestInputStream digestInputStream = new SdkDigestInputStream(exchange.getRequestBody(), MessageDigests.md5());
                    BytesReference bytes = Streams.readFully(digestInputStream);
                    assertThat((long) bytes.length(), anyOf(equalTo(lastPartSize), equalTo(bufferSize.getBytes())));
                    assertThat(contentLength, anyOf(equalTo(lastPartSize), equalTo(bufferSize.getBytes())));

                    if (countDownUploads.decrementAndGet() % 2 == 0) {
                        exchange.getResponseHeaders().add("ETag", Base16.encodeAsString(digestInputStream.getMessageDigest().digest()));
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                        exchange.close();
                        return;
                    }

                } else if ("POST".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery().equals("uploadId=TEST")) {
                    // complete multipart upload request
                    if (countDownComplete.countDown()) {
                        Streams.readFully(exchange.getRequestBody());
                        byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                            + "<CompleteMultipartUploadResult>\n"
                            + "  <Bucket>bucket</Bucket>\n"
                            + "  <Key>write_large_blob</Key>\n"
                            + "</CompleteMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                        exchange.getResponseBody().write(response);
                        exchange.close();
                        return;
                    }
                }

            // sends an error back or let the request time out
            if (useTimeout == false) {
                if (randomBoolean() && contentLength > 0) {
                    Streams.readFully(exchange.getRequestBody(), new byte[randomIntBetween(1, Math.toIntExact(contentLength - 1))]);
                } else {
                    Streams.readFully(exchange.getRequestBody());
                    exchange.sendResponseHeaders(
                        randomFrom(
                            HttpStatus.SC_INTERNAL_SERVER_ERROR,
                            HttpStatus.SC_BAD_GATEWAY,
                            HttpStatus.SC_SERVICE_UNAVAILABLE,
                            HttpStatus.SC_GATEWAY_TIMEOUT
                        ),
                        -1
                    );
                }
                exchange.close();
            }
        });

        blobContainer.writeBlob("write_large_blob", new ZeroInputStream(blobSize), blobSize, false);

        assertThat(countDownInitiate.isCountedDown(), is(true));
        assertThat(countDownUploads.get(), equalTo(0));
        assertThat(countDownComplete.isCountedDown(), is(true));
    }

    /**
     * Asserts that an InputStream is fully consumed, or aborted, when it is closed
     */
    private static class AssertingInputStream extends FilterInputStream {

        private final String blobName;
        private final boolean range;
        private final long position;
        private final long length;

        AssertingInputStream(InputStream in, String blobName) {
            super(in);
            this.blobName = blobName;
            this.position = 0L;
            this.length = Long.MAX_VALUE;
            this.range = false;
        }

        AssertingInputStream(InputStream in, String blobName, long position, long length) {
            super(in);
            this.blobName = blobName;
            this.position = position;
            this.length = length;
            this.range = true;
        }

        @Override
        public String toString() {
            String description = "[blobName='" + blobName + "', range=" + range;
            if (range) {
                description += ", position=" + position;
                description += ", length=" + length;
            }
            description += ']';
            return description;
        }

        @Override
        public void close() throws IOException {
            super.close();
            if (in instanceof S3RetryingInputStream) {
                final S3RetryingInputStream s3Stream = (S3RetryingInputStream) in;
                assertTrue(
                    "Stream "
                        + toString()
                        + " should have reached EOF or should have been aborted but got [eof="
                        + s3Stream.isEof()
                        + ", aborted="
                        + s3Stream.isAborted()
                        + ']',
                    s3Stream.isEof() || s3Stream.isAborted()
                );
            } else {
                assertThat(in, instanceOf(ByteArrayInputStream.class));
                assertThat(((ByteArrayInputStream) in).available(), equalTo(0));
            }
        }
    }
}
