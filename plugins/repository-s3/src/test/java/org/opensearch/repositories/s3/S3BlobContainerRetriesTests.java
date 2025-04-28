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

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.io.SdkDigestInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.internal.Base16;

import org.apache.http.HttpStatus;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.StreamContext;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.StreamContextSupplier;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.hash.MessageDigests;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.io.Streams;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.blobstore.AbstractBlobContainerRetriesTestCase;
import org.opensearch.repositories.blobstore.ZeroInputStream;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.repositories.s3.async.TransferSemaphoresHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.repositories.s3.S3ClientSettings.DISABLE_CHUNKED_ENCODING;
import static org.opensearch.repositories.s3.S3ClientSettings.ENDPOINT_SETTING;
import static org.opensearch.repositories.s3.S3ClientSettings.MAX_RETRIES_SETTING;
import static org.opensearch.repositories.s3.S3ClientSettings.READ_TIMEOUT_SETTING;
import static org.opensearch.repositories.s3.S3ClientSettings.REGION;
import static org.opensearch.repositories.s3.S3Repository.BULK_DELETE_SIZE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    private ExecutorService remoteTransferRetry;
    private ExecutorService transferQueueConsumerService;
    private ScheduledExecutorService scheduler;
    private AsyncTransferEventLoopGroup transferNIOGroup;
    private SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ;
    private SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ;

    @Before
    public void setUp() throws Exception {
        previousOpenSearchPathConf = SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        service = new S3Service(configPath());
        asyncService = new S3AsyncService(configPath());

        futureCompletionService = Executors.newSingleThreadExecutor();
        streamReaderService = Executors.newSingleThreadExecutor();
        transferNIOGroup = new AsyncTransferEventLoopGroup(1);
        remoteTransferRetry = Executors.newFixedThreadPool(20);
        transferQueueConsumerService = Executors.newFixedThreadPool(2);
        scheduler = new ScheduledThreadPoolExecutor(1);
        GenericStatsMetricPublisher genericStatsMetricPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
        normalPrioritySizeBasedBlockingQ = new SizeBasedBlockingQ(
            new ByteSizeValue(Runtime.getRuntime().availableProcessors() * 5L, ByteSizeUnit.GB),
            transferQueueConsumerService,
            2,
            genericStatsMetricPublisher,
            SizeBasedBlockingQ.QueueEventType.NORMAL
        );
        lowPrioritySizeBasedBlockingQ = new SizeBasedBlockingQ(
            new ByteSizeValue(Runtime.getRuntime().availableProcessors() * 5L, ByteSizeUnit.GB),
            transferQueueConsumerService,
            2,
            genericStatsMetricPublisher,
            SizeBasedBlockingQ.QueueEventType.LOW
        );
        normalPrioritySizeBasedBlockingQ.start();
        lowPrioritySizeBasedBlockingQ.start();
        // needed by S3AsyncService
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", configPath().toString()));
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(service, asyncService);

        streamReaderService.shutdown();
        futureCompletionService.shutdown();
        remoteTransferRetry.shutdown();
        transferQueueConsumerService.shutdown();
        scheduler.shutdown();
        normalPrioritySizeBasedBlockingQ.close();
        lowPrioritySizeBasedBlockingQ.close();
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
    protected AsyncMultiStreamBlobContainer createBlobContainer(
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
        GenericStatsMetricPublisher genericStatsMetricPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
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
                BULK_DELETE_SIZE.get(Settings.EMPTY),
                repositoryMetadata,
                new AsyncTransferManager(
                    S3Repository.PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING.getDefault(Settings.EMPTY).getBytes(),
                    asyncExecutorContainer.getStreamReader(),
                    asyncExecutorContainer.getStreamReader(),
                    asyncExecutorContainer.getStreamReader(),
                    new TransferSemaphoresHolder(
                        3,
                        Math.max(Runtime.getRuntime().availableProcessors() * 5, 10),
                        5,
                        TimeUnit.MINUTES,
                        genericStatsMetricPublisher
                    )
                ),
                asyncExecutorContainer,
                asyncExecutorContainer,
                asyncExecutorContainer,
                normalPrioritySizeBasedBlockingQ,
                lowPrioritySizeBasedBlockingQ,
                genericStatsMetricPublisher
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

    public void testWriteBlobWithMetadataIfVerifiedAndETagRetries() throws Exception {
        final Map<String, String> metadata = randomBoolean()
            ? Map.of(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10))
            : null;

        final String expectedETag = "\"" + randomAlphaOfLength(32) + "\"";
        final AtomicReference<String> returnedETag = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        final ActionListener<String> eTagListener = ActionListener.wrap(eTag -> {
            returnedETag.set(eTag);
            latch.countDown();
        }, e -> fail("Should not have failed: " + e));

        writeBlobWithMetadataIfVerifiedAndETagRetriesHelper(metadata, expectedETag, eTagListener);

        assertTrue("Listener was not called within timeout", latch.await(2, TimeUnit.SECONDS));
        assertNotNull("ETag should not be null", returnedETag.get());
        assertEquals("Returned ETag should match expected", expectedETag, returnedETag.get());
    }

    private void writeBlobWithMetadataIfVerifiedAndETagRetriesHelper(
        Map<String, String> metadata,
        String expectedETag,
        ActionListener<String> eTagListener
    ) throws Exception {
        final int maxRetries = randomInt(5);
        final CountDown countDown = new CountDown(maxRetries + 1);

        final String blobName = "write_blob_etag_retries";
        final String contextPath = "/bucket/" + blobName;

        final byte[] bytes = randomBlobContent();
        httpServer.createContext(contextPath, exchange -> {
            if ("PUT".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery() == null) {
                List<String> ifMatchHeaders = exchange.getRequestHeaders().get("If-Match");
                assertNotNull("If-Match header should be present", ifMatchHeaders);
                assertEquals("If-Match header value", expectedETag, ifMatchHeaders.getFirst());

                if (metadata != null) {
                    for (Map.Entry<String, String> entry : metadata.entrySet()) {
                        List<String> values = exchange.getRequestHeaders().get("x-amz-meta-" + entry.getKey());
                        assertNotNull("Metadata header missing: " + entry.getKey(), values);
                        assertEquals("Metadata value incorrect", entry.getValue(), values.getFirst());
                    }
                } else {
                    for (String header : exchange.getRequestHeaders().keySet()) {
                        assertFalse(
                            "No metadata headers should be present when metadata is null",
                            header.toLowerCase(Locale.ROOT).startsWith("x-amz-meta-")
                        );
                    }
                }

                if (countDown.countDown()) {
                    final BytesReference body = Streams.readFully(exchange.getRequestBody());
                    if (Objects.deepEquals(bytes, BytesReference.toBytes(body))) {
                        exchange.getResponseHeaders().set("ETag", expectedETag);
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

        try {
            final BlobContainer blobContainer = createBlobContainer(maxRetries, null, true, null);
            try (InputStream stream = new ByteArrayInputStream(bytes)) {
                if (metadata != null) {
                    blobContainer.writeBlobWithMetadataIfVerified(
                        blobName,
                        stream,
                        bytes.length,
                        false,
                        metadata,
                        expectedETag,
                        eTagListener
                    );
                } else {
                    blobContainer.writeBlobIfVerified(blobName, stream, bytes.length, false, expectedETag, eTagListener);
                }
            }
            assertThat(countDown.isCountedDown(), is(true));
        } finally {
            httpServer.removeContext(contextPath);
        }
    }

    public void testWriteBlobWithMetadataIfVerifiedAndETagReadTimeouts() {
        final Map<String, String> metadata = randomBoolean() ? Map.of(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null;

        final String eTag = "\"" + randomAlphaOfLength(32) + "\"";
        final byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 128));
        final TimeValue readTimeout = TimeValue.timeValueMillis(randomIntBetween(100, 500));
        final BlobContainer blobContainer = createBlobContainer(1, readTimeout, true, null);

        final CountDownLatch listenerLatch = new CountDownLatch(1);
        final AtomicReference<Exception> listenerException = new AtomicReference<>();
        final ActionListener<String> eTagListener = ActionListener.wrap(s -> fail("Should not succeed with timeout"), e -> {
            listenerException.set(e);
            listenerLatch.countDown();
        });

        final String blobName = "write_blob_etag_timeout";
        final String contextPath = "/bucket/" + blobName;

        httpServer.createContext(contextPath, exchange -> {
            try {
                List<String> ifMatchHeaders = exchange.getRequestHeaders().get("If-Match");
                assertNotNull("If-Match header should be present", ifMatchHeaders);
                assertEquals("If-Match header value", eTag, ifMatchHeaders.getFirst());

                if (metadata != null) {
                    for (Map.Entry<String, String> entry : metadata.entrySet()) {
                        List<String> values = exchange.getRequestHeaders().get("x-amz-meta-" + entry.getKey());
                        assertNotNull("Metadata header missing: " + entry.getKey(), values);
                        assertEquals("Metadata value incorrect", entry.getValue(), values.getFirst());
                    }
                } else {
                    for (String header : exchange.getRequestHeaders().keySet()) {
                        assertFalse(
                            "No metadata headers should be present when metadata is null",
                            header.toLowerCase(Locale.ROOT).startsWith("x-amz-meta-")
                        );
                    }
                }

                Streams.readFully(exchange.getRequestBody());

                try {
                    Thread.sleep(readTimeout.millis() + 100);
                } catch (InterruptedException e) {}

                exchange.close();
            } catch (Exception ex) {
                exchange.close();
            }
        });

        try {
            Exception directException = expectThrows(IOException.class, () -> {
                try (InputStream stream = new InputStreamIndexInput(new ByteArrayIndexInput("desc", bytes), bytes.length)) {
                    if (metadata != null) {
                        blobContainer.writeBlobWithMetadataIfVerified(blobName, stream, bytes.length, false, metadata, eTag, eTagListener);
                    } else {
                        blobContainer.writeBlobIfVerified(blobName, stream, bytes.length, false, eTag, eTagListener);
                    }
                }
            });

            assertThat(directException.getMessage().toLowerCase(Locale.ROOT), containsString("s3 upload failed for [" + blobName + "]"));
            assertThat(directException.getCause(), instanceOf(SdkClientException.class));
            assertThat(directException.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
            assertThat(directException.getCause().getCause(), instanceOf(SocketTimeoutException.class));
            assertThat(directException.getCause().getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));

            assertTrue("Listener was not called within timeout", listenerLatch.await(3, TimeUnit.SECONDS));

            Exception asyncException = listenerException.get();
            assertNotNull("Exception should not be null", asyncException);
            assertTrue("Exception should be IOException", asyncException instanceof IOException);

            assertThat(asyncException.getMessage().toLowerCase(Locale.ROOT), containsString("s3 upload failed for [" + blobName + "]"));
            assertThat(asyncException.getCause(), instanceOf(SdkClientException.class));
            assertThat(asyncException.getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
            assertThat(asyncException.getCause().getCause(), instanceOf(SocketTimeoutException.class));
            assertThat(asyncException.getCause().getCause().getMessage().toLowerCase(Locale.ROOT), containsString("read timed out"));
        } catch (InterruptedException e) {
            fail("Test was interrupted: " + e);
        } finally {
            httpServer.removeContext(contextPath);
        }
    }

    public void testWriteLargeBlobWithMetadataAndETag() throws Exception {
        final Map<String, String> metadata = randomBoolean() ? Map.of(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null;

        final String expectedETag = "\"" + randomAlphaOfLength(32) + "\"";
        final AtomicReference<String> returnedETag = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final ActionListener<String> eTagListener = ActionListener.wrap(eTag -> {
            returnedETag.set(eTag);
            latch.countDown();
        }, e -> { latch.countDown(); });

        writeLargeBlobWithMetadataAndETagHelper(metadata, expectedETag, eTagListener);

        assertTrue("Listener was not called within timeout", latch.await(5, TimeUnit.SECONDS));
        if (returnedETag.get() != null) {
            assertEquals("Final ETag should match expected", expectedETag, returnedETag.get());
        }
    }

    private void writeLargeBlobWithMetadataAndETagHelper(
        Map<String, String> metadata,
        String verificationTag,
        ActionListener<String> verificationTagListener
    ) throws Exception {
        final boolean useTimeout = rarely();
        final TimeValue readTimeout = useTimeout ? TimeValue.timeValueMillis(randomIntBetween(100, 500)) : null;
        final ByteSizeValue bufferSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        final BlobContainer blobContainer = createBlobContainer(null, readTimeout, true, bufferSize);

        final int parts = randomIntBetween(1, 5);
        final long lastPartSize = randomLongBetween(10, 512);
        final long blobSize = (parts * bufferSize.getBytes()) + lastPartSize;

        final int nbErrors = 2;
        final CountDown countDownInitiate = new CountDown(nbErrors);
        final AtomicInteger countDownUploads = new AtomicInteger(nbErrors * (parts + 1));
        final CountDown countDownComplete = new CountDown(nbErrors);

        final String contextPath = "/bucket/write_large_blob_metadata_etag";

        httpServer.createContext(contextPath, exchange -> {
            try {
                long contentLength = 0;
                String contentLengthHeader = exchange.getRequestHeaders().getFirst("Content-Length");
                if (contentLengthHeader != null) {
                    try {
                        contentLength = Long.parseLong(contentLengthHeader);
                    } catch (NumberFormatException e) {}
                }

                if ("POST".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery().equals("uploads")) {
                    List<String> ifMatchHeaders = exchange.getRequestHeaders().get("If-Match");
                    if (ifMatchHeaders == null || ifMatchHeaders.isEmpty()) {} else if (!verificationTag.equals(
                        ifMatchHeaders.getFirst()
                    )) {
                    }

                    if (metadata != null) {
                        for (Map.Entry<String, String> entry : metadata.entrySet()) {
                            List<String> values = exchange.getRequestHeaders().get("x-amz-meta-" + entry.getKey());
                            if (values == null || values.isEmpty()) {
                            }
                        }
                    }

                    if (countDownInitiate.countDown()) {
                        byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                            + "<InitiateMultipartUploadResult>\n"
                            + "  <Bucket>bucket</Bucket>\n"
                            + "  <Key>write_large_blob_metadata_etag</Key>\n"
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

                        if (exchange.getRequestHeaders().get("If-Match") != null) {
                        }

                        SdkDigestInputStream digestInputStream = new SdkDigestInputStream(exchange.getRequestBody(), MessageDigests.md5());
                        BytesReference bytes = Streams.readFully(digestInputStream);

                        if ((long) bytes.length() != lastPartSize && (long) bytes.length() != bufferSize.getBytes()) {
                        }

                        if (countDownUploads.decrementAndGet() % 2 == 0) {
                            exchange.getResponseHeaders().add("ETag", Base16.encodeAsString(digestInputStream.getMessageDigest().digest()));
                            exchange.sendResponseHeaders(HttpStatus.SC_OK, -1);
                            exchange.close();
                            return;
                        }
                    } else if ("POST".equals(exchange.getRequestMethod()) && exchange.getRequestURI().getQuery().equals("uploadId=TEST")) {

                        List<String> ifMatchHeaders = exchange.getRequestHeaders().get("If-Match");
                        if (ifMatchHeaders == null || ifMatchHeaders.isEmpty()) {} else if (!verificationTag.equals(
                            ifMatchHeaders.getFirst()
                        )) {
                        }

                        if (countDownComplete.countDown()) {
                            Streams.readFully(exchange.getRequestBody());
                            byte[] response = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                + "<CompleteMultipartUploadResult>\n"
                                + "  <Bucket>bucket</Bucket>\n"
                                + "  <Key>write_large_blob_metadata_etag</Key>\n"
                                + "  <ETag>"
                                + verificationTag
                                + "</ETag>\n"
                                + "</CompleteMultipartUploadResult>").getBytes(StandardCharsets.UTF_8);
                            exchange.getResponseHeaders().add("Content-Type", "application/xml");
                            exchange.sendResponseHeaders(HttpStatus.SC_OK, response.length);
                            exchange.getResponseBody().write(response);
                            exchange.close();
                            return;
                        }
                    }

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
                } else {}
            } catch (Exception e) {
                if (!useTimeout) {
                    try {
                        exchange.close();
                    } catch (Exception ignored) {}
                }
            }
        });

        try {
            if (metadata != null) {
                blobContainer.writeBlobWithMetadataIfVerified(
                    "write_large_blob_metadata_etag",
                    new ZeroInputStream(blobSize),
                    blobSize,
                    false,
                    metadata,
                    verificationTag,
                    verificationTagListener
                );
            } else {
                blobContainer.writeBlobIfVerified(
                    "write_large_blob_metadata_etag",
                    new ZeroInputStream(blobSize),
                    blobSize,
                    false,
                    verificationTag,
                    verificationTagListener
                );
            }

            assertThat(countDownInitiate.isCountedDown(), is(true));
            assertThat(countDownUploads.get(), equalTo(0));
            assertThat(countDownComplete.isCountedDown(), is(true));

        } catch (IOException e) {
            if (useTimeout
                || e.getMessage().contains("Unable to execute HTTP request")
                || e.getMessage().contains("failed to respond")
                || e.getMessage().contains("service unavailable")
                || e.getMessage().contains("timed out")
                || e.getMessage().contains("timeout")
                || (e.getCause() instanceof java.net.SocketTimeoutException)) {} else {
                throw e;
            }
        } finally {
            httpServer.removeContext(contextPath);
        }
    }

    public void testWriteBlobWithMetadataIfVerifiedAndETagMismatch() throws Exception {
        final Map<String, String> metadata = randomBoolean() ? Map.of(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null;
        final String eTag = "\"" + randomAlphaOfLength(32) + "\"";
        final AtomicReference<Exception> listenerException = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final ActionListener<String> eTagListener = ActionListener.wrap(s -> fail("Should not succeed with ETag mismatch"), e -> {
            listenerException.set(e);
            latch.countDown();
        });

        S3Exception preconditionFailedException = (S3Exception) S3Exception.builder()
            .statusCode(412)
            .message("The condition specified using HTTP conditional header(s) evaluated to false")
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("PreconditionFailed")
                    .errorMessage("The condition specified in the request evaluated to false")
                    .serviceName("S3")
                    .build()
            )
            .build();

        S3Client mockS3Client = mock(S3Client.class);
        when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(preconditionFailedException);

        AmazonS3Reference mockRef = mock(AmazonS3Reference.class);
        when(mockRef.get()).thenReturn(mockS3Client);

        S3BlobStore mockBlobStore = mock(S3BlobStore.class);
        when(mockBlobStore.bucket()).thenReturn("bucket");
        when(mockBlobStore.clientReference()).thenReturn(mockRef);
        when(mockBlobStore.bufferSizeInBytes()).thenReturn(104857600L);
        when(mockBlobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        BlobPath blobPath = new BlobPath();
        S3BlobContainer blobContainer = new S3BlobContainer(blobPath, mockBlobStore);

        final String blobName = "write_blob_metadata_etag_mismatch";

        final AtomicBoolean streamClosed = new AtomicBoolean(false);
        InputStream trackingStream = new ByteArrayInputStream(new byte[1024]) {
            @Override
            public void close() throws IOException {
                streamClosed.set(true);
                super.close();
            }
        };

        try {
            try (InputStream ignored = trackingStream) {
                if (metadata != null) {
                    blobContainer.writeBlobWithMetadataIfVerified(blobName, trackingStream, 1024, false, metadata, eTag, eTagListener);
                } else {
                    blobContainer.writeBlobIfVerified(blobName, trackingStream, 1024, false, eTag, eTagListener);
                }
                fail("Expected exception was not thrown");
            } catch (IOException expected) {
                assertTrue(
                    "Direct exception should indicate ETag mismatch",
                    expected.getMessage().contains("Unable to upload object") && expected.getMessage().contains("due to ETag mismatch")
                );
                assertSame("Exception cause should be our mock S3Exception", preconditionFailedException, expected.getCause());
            }

            assertTrue("Listener was not called within timeout", latch.await(30, TimeUnit.SECONDS));
            assertNotNull("Exception should not be null", listenerException.get());
            assertTrue("Should receive OpenSearchException", listenerException.get() instanceof OpenSearchException);

            verify(mockRef).close();
            assertTrue("Input stream should be closed", streamClosed.get());
        } catch (Exception e) {
            throw e;
        }
    }

    public void writeBlobWithRetriesHelper(Map<String, String> metadata) throws Exception {
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
            if (metadata != null) {
                blobContainer.writeBlobWithMetadata("write_blob_max_retries", stream, bytes.length, false, metadata);
            } else {
                blobContainer.writeBlob("write_blob_max_retries", stream, bytes.length, false);
            }
        }
        assertThat(countDown.isCountedDown(), is(true));
    }

    public void testWriteBlobWithMetadataWithRetries() throws Exception {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        writeBlobWithRetriesHelper(metadata);
    }

    public void testWriteBlobWithRetries() throws Exception {
        writeBlobWithRetriesHelper(null);
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

        final AsyncMultiStreamBlobContainer blobContainer = createBlobContainer(maxRetries, null, true, null);
        List<InputStream> openInputStreams = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        ActionListener<Void> completionListener = ActionListener.wrap(resp -> { countDownLatch.countDown(); }, ex -> {
            exceptionRef.set(ex);
            countDownLatch.countDown();
        });

        StreamContextSupplier streamContextSupplier = partSize -> new StreamContext((partNo, size, position) -> {
            InputStream inputStream = new OffsetRangeIndexInputStream(new ByteArrayIndexInput("desc", bytes), size, position);
            openInputStreams.add(inputStream);
            return new InputStreamContainer(inputStream, size, position);
        }, partSize, calculateLastPartSize(bytes.length, partSize), calculateNumberOfParts(bytes.length, partSize));

        WriteContext writeContext = new WriteContext.Builder().fileName("write_blob_by_streams_max_retries")
            .streamContextSupplier(streamContextSupplier)
            .fileSize(bytes.length)
            .failIfAlreadyExists(false)
            .writePriority(WritePriority.NORMAL)
            .uploadFinalizer(Assert::assertTrue)
            .doRemoteDataIntegrityCheck(false)
            .build();

        blobContainer.asyncBlobUpload(writeContext, completionListener);
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

    public void writeBlobWithReadTimeoutsHelper(Map<String, String> metadata) {
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
                if (metadata != null) {
                    blobContainer.writeBlobWithMetadata("write_blob_timeout", stream, bytes.length, false, metadata);
                } else {
                    blobContainer.writeBlob("write_blob_timeout", stream, bytes.length, false);
                }
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

    public void testWriteBlobWithMetadataWithReadTimeouts() throws Exception {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        writeBlobWithReadTimeoutsHelper(metadata);
    }

    public void testWriteBlobWithReadTimeouts() throws Exception {
        writeBlobWithReadTimeoutsHelper(null);
    }

    public void WriteLargeBlobHelper(Map<String, String> metadata) throws Exception {
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

        if (metadata != null) {
            blobContainer.writeBlobWithMetadata("write_large_blob", new ZeroInputStream(blobSize), blobSize, false, metadata);
        } else {
            blobContainer.writeBlob("write_large_blob", new ZeroInputStream(blobSize), blobSize, false);
        }

        assertThat(countDownInitiate.isCountedDown(), is(true));
        assertThat(countDownUploads.get(), equalTo(0));
        assertThat(countDownComplete.isCountedDown(), is(true));
    }

    public void testWriteLargeBlobWithMetadata() throws Exception {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        WriteLargeBlobHelper(metadata);
    }

    public void testWriteLargeBlob() throws Exception {
        WriteLargeBlobHelper(null);
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
