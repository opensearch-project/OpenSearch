/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobVersionConflictException;
import org.opensearch.common.blobstore.VersionedBlob;
import org.opensearch.common.io.Streams;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.scheduler.Schedulers;
import reactor.netty.http.HttpResources;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.opensearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;

@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerConditionalWriteTests extends OpenSearchTestCase {

    private HttpServer httpServer;
    private ThreadPool threadPool;
    private AzureStorageService service;

    @Before
    public void setUp() throws Exception {
        threadPool = new TestThreadPool(getTestClass().getName(), AzureRepositoryPlugin.executorBuilder());
        httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/", exchange -> {
            sendAzureError(exchange, 404, "BlobNotFound");
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
        HttpResources.disposeLoopsAndConnections();
        Schedulers.shutdownNow();
    }

    public void testConditionalWriteSupported() {
        assertTrue(createBlobContainer(3).isConditionalWriteSupported());
    }

    public void testReadBlobWithVersionReturnsContentAndETag() throws Exception {
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        final String eTag = randomAlphaOfLength(16);
        httpServer.createContext("/container/versioned", exchange -> {
            assertEquals("GET", exchange.getRequestMethod());
            assertEquals("bytes=0-" + AzureBlobStore.MAX_CONDITIONAL_WRITE_SIZE, exchange.getRequestHeaders().getFirst("x-ms-range"));
            sendDownload(exchange, content, quoted(eTag));
        });

        final VersionedBlob blob = createBlobContainer(3).readBlobWithVersion("versioned");
        assertArrayEquals(content, blob.content());
        assertEquals(eTag, blob.versionToken());
    }

    public void testReadBlobWithVersionOnMissingBlobThrowsNoSuchFile() {
        httpServer.createContext("/container/missing", exchange -> sendAzureError(exchange, 404, "BlobNotFound"));
        expectThrows(NoSuchFileException.class, () -> createBlobContainer(3).readBlobWithVersion("missing"));
    }

    public void testReadBlobWithVersionDoesNotTreatMissingContainerAsMissingBlob() {
        httpServer.createContext("/container/missing-container", exchange -> sendAzureError(exchange, 404, "ContainerNotFound"));
        final IOException e = expectThrows(IOException.class, () -> createBlobContainer(3).readBlobWithVersion("missing-container"));
        assertFalse(e instanceof NoSuchFileException);
    }

    public void testReadBlobWithVersionRejectsOversizedBlob() {
        final byte[] content = randomByteArrayOfLength(Math.toIntExact(AzureBlobStore.MAX_CONDITIONAL_WRITE_SIZE + 1));
        httpServer.createContext("/container/oversized", exchange -> sendDownload(exchange, content, quoted("etag")));
        final IOException e = expectThrows(IOException.class, () -> createBlobContainer(3).readBlobWithVersion("oversized"));
        assertTrue(e.getMessage(), e.getMessage().contains("too large"));
    }

    public void testReadBlobWithVersionRejectsMissingETag() {
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        httpServer.createContext("/container/no-read-etag", exchange -> sendDownload(exchange, content, null));
        final IOException e = expectThrows(IOException.class, () -> createBlobContainer(3).readBlobWithVersion("no-read-etag"));
        assertTrue(e.getMessage(), e.getMessage().contains("did not return an ETag"));
    }

    public void testConditionalCreateAndVersionedReadOfEmptyBlob() throws Exception {
        final String eTag = randomAlphaOfLength(16);
        final AtomicInteger putRequests = new AtomicInteger();
        final AtomicInteger getRequests = new AtomicInteger();
        final AtomicInteger headRequests = new AtomicInteger();
        httpServer.createContext("/container/empty", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                putRequests.incrementAndGet();
                assertEquals("*", exchange.getRequestHeaders().getFirst("If-None-Match"));
                assertArrayEquals(new byte[0], BytesReference.toBytes(Streams.readFully(exchange.getRequestBody())));
                sendUploadSuccess(exchange, quoted(eTag));
            } else if ("GET".equals(exchange.getRequestMethod())) {
                getRequests.incrementAndGet();
                assertEquals("bytes=0-" + AzureBlobStore.MAX_CONDITIONAL_WRITE_SIZE, exchange.getRequestHeaders().getFirst("x-ms-range"));
                sendAzureError(exchange, 416, "InvalidRange");
            } else if ("HEAD".equals(exchange.getRequestMethod())) {
                headRequests.incrementAndGet();
                sendProperties(exchange, 0, quoted(eTag));
            } else {
                fail("unexpected method " + exchange.getRequestMethod());
            }
        });

        final BlobContainer container = createBlobContainer(3);
        assertEquals(eTag, container.writeBlobConditionally("empty", new ByteArrayInputStream(new byte[0]), 0, null));

        final VersionedBlob blob = container.readBlobWithVersion("empty");
        assertArrayEquals(new byte[0], blob.content());
        assertEquals(eTag, blob.versionToken());
        assertEquals(1, putRequests.get());
        assertEquals(1, getRequests.get());
        assertEquals(1, headRequests.get());
    }

    public void testInvalidRangeIsNotTreatedAsEmptyWhenPropertiesAreNonEmpty() {
        httpServer.createContext("/container/not-empty", exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                sendAzureError(exchange, 416, "InvalidRange");
            } else if ("HEAD".equals(exchange.getRequestMethod())) {
                sendProperties(exchange, 1, quoted("etag"));
            } else {
                fail("unexpected method " + exchange.getRequestMethod());
            }
        });

        final IOException e = expectThrows(IOException.class, () -> createBlobContainer(3).readBlobWithVersion("not-empty"));
        assertTrue(e.getMessage(), e.getMessage().contains("is not empty"));
    }

    public void testCreateIfAbsentUsesIfNoneMatchAndReturnsETag() throws Exception {
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        final String eTag = randomAlphaOfLength(16);
        httpServer.createContext("/container/create", exchange -> {
            assertEquals("PUT", exchange.getRequestMethod());
            assertEquals("*", exchange.getRequestHeaders().getFirst("If-None-Match"));
            assertNull(exchange.getRequestHeaders().getFirst("If-Match"));
            assertArrayEquals(content, BytesReference.toBytes(Streams.readFully(exchange.getRequestBody())));
            sendUploadSuccess(exchange, quoted(eTag));
        });

        assertEquals(
            eTag,
            createBlobContainer(3).writeBlobConditionally("create", new ByteArrayInputStream(content), content.length, null)
        );
    }

    public void testUpdateIfMatchUsesETagAndReturnsNewETag() throws Exception {
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        final String expectedETag = randomAlphaOfLength(16);
        final String newETag = randomAlphaOfLength(16);
        httpServer.createContext("/container/update", exchange -> {
            assertEquals("PUT", exchange.getRequestMethod());
            assertEquals(expectedETag, exchange.getRequestHeaders().getFirst("If-Match"));
            assertNull(exchange.getRequestHeaders().getFirst("If-None-Match"));
            assertArrayEquals(content, BytesReference.toBytes(Streams.readFully(exchange.getRequestBody())));
            sendUploadSuccess(exchange, quoted(newETag));
        });

        assertEquals(
            newETag,
            createBlobContainer(3).writeBlobConditionally("update", new ByteArrayInputStream(content), content.length, expectedETag)
        );
    }

    public void testConditionalWriteRejectsMissingETag() {
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        httpServer.createContext("/container/no-write-etag", exchange -> {
            Streams.readFully(exchange.getRequestBody());
            sendUploadSuccess(exchange, null);
        });
        final IOException e = expectThrows(
            IOException.class,
            () -> createBlobContainer(3).writeBlobConditionally("no-write-etag", new ByteArrayInputStream(content), content.length, null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("did not return an ETag"));
    }

    public void testDefinitiveConditionFailuresAreConflicts() {
        final BlobContainer container = createBlobContainer(3);
        assertConflict(container, "create-conflict", null, 409, "BlobAlreadyExists");
        assertConflict(container, "create-precondition", null, 412, "ConditionNotMet");
        assertConflict(container, "update-conflict", quoted("stale"), 412, "ConditionNotMet");
        assertConflict(container, "update-missing", quoted("deleted"), 404, "BlobNotFound");
    }

    public void testOtherServiceErrorsRemainRetryableIOExceptions() {
        final BlobContainer container = createBlobContainer(3);
        assertRetryable(container, "create-not-found", null, 404, "BlobNotFound");
        assertRetryable(container, "update-container-missing", quoted("etag"), 404, "ContainerNotFound");
        assertRetryable(container, "update-conflict", quoted("etag"), 409, "LeaseAlreadyPresent");
        assertRetryable(container, "unlabelled-precondition", quoted("etag"), 412, null);
        assertRetryable(container, "server-error", quoted("etag"), 500, "InternalError");
    }

    public void testLostResponseIsNotRetriedIntoConflictAndCanBeReconciled() throws Exception {
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 512));
        final String expectedETag = randomAlphaOfLength(16);
        final String landedETag = randomAlphaOfLength(16);
        final AtomicReference<byte[]> stored = new AtomicReference<>();
        final AtomicInteger putAttempts = new AtomicInteger();

        httpServer.createContext("/container/ambiguous", exchange -> {
            if ("PUT".equals(exchange.getRequestMethod())) {
                final int attempt = putAttempts.incrementAndGet();
                if (attempt == 1) {
                    stored.set(BytesReference.toBytes(Streams.readFully(exchange.getRequestBody())));
                    exchange.close();
                } else {
                    sendAzureError(exchange, 412, "ConditionNotMet");
                }
            } else if ("GET".equals(exchange.getRequestMethod())) {
                sendDownload(exchange, stored.get(), quoted(landedETag));
            } else {
                fail("unexpected method " + exchange.getRequestMethod());
            }
        });

        final BlobContainer container = createBlobContainer(4);
        final IOException failure = expectThrows(
            IOException.class,
            () -> container.writeBlobConditionally("ambiguous", new ByteArrayInputStream(content), content.length, expectedETag)
        );
        assertFalse(failure instanceof BlobVersionConflictException);
        assertEquals("the conditional client must make exactly one attempt", 1, putAttempts.get());

        final VersionedBlob reconciled = container.readBlobWithVersion("ambiguous");
        assertArrayEquals(content, reconciled.content());
        assertEquals(landedETag, reconciled.versionToken());
    }

    public void testConditionalWriteRejectsOversizedPayload() {
        final long size = AzureBlobStore.MAX_CONDITIONAL_WRITE_SIZE + 1;
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createBlobContainer(3).writeBlobConditionally("large", new ByteArrayInputStream(new byte[0]), size, null)
        );
        assertEquals("Conditional write request size [" + size + "] can't be larger than buffer size", e.getMessage());
    }

    private void assertConflict(BlobContainer container, String blobName, String expectedETag, int status, String errorCode) {
        registerError(blobName, status, errorCode);
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 32));
        expectThrows(
            BlobVersionConflictException.class,
            () -> container.writeBlobConditionally(blobName, new ByteArrayInputStream(content), content.length, expectedETag)
        );
    }

    private void assertRetryable(BlobContainer container, String blobName, String expectedETag, int status, String errorCode) {
        registerError(blobName, status, errorCode);
        final byte[] content = randomByteArrayOfLength(randomIntBetween(1, 32));
        final IOException e = expectThrows(
            IOException.class,
            () -> container.writeBlobConditionally(blobName, new ByteArrayInputStream(content), content.length, expectedETag)
        );
        assertFalse(e instanceof BlobVersionConflictException);
    }

    private void registerError(String blobName, int status, String errorCode) {
        httpServer.createContext("/container/" + blobName, exchange -> sendAzureError(exchange, status, errorCode));
    }

    private BlobContainer createBlobContainer(int maxRetries) {
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
        clientSettings.put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueSeconds(5));

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "account");
        secureSettings.setString(
            KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
            Base64.getEncoder().encodeToString(randomAlphaOfLength(10).getBytes(UTF_8))
        );
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
            ParallelTransferOptions getBlobRequestOptionsForWriteBlob(String clientName) {
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

    private static void sendUploadSuccess(HttpExchange exchange, String eTag) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        if (eTag != null) {
            headers.add("ETag", eTag);
        }
        headers.add("x-ms-request-server-encrypted", "false");
        exchange.sendResponseHeaders(201, -1);
        exchange.close();
    }

    private static void sendDownload(HttpExchange exchange, byte[] content, String eTag) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        if (eTag != null) {
            headers.add("ETag", eTag);
        }
        headers.add("Content-Type", "application/octet-stream");
        headers.add("Content-Length", Integer.toString(content.length));
        headers.add("Content-Range", "bytes 0-" + (content.length - 1) + "/" + content.length);
        headers.add("x-ms-blob-type", "BlockBlob");
        headers.add("x-ms-request-server-encrypted", "false");
        exchange.sendResponseHeaders(206, content.length);
        exchange.getResponseBody().write(content);
        exchange.close();
    }

    private static void sendProperties(HttpExchange exchange, long contentLength, String eTag) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Length", Long.toString(contentLength));
        headers.add("ETag", eTag);
        headers.add("x-ms-blob-type", "BlockBlob");
        headers.add("x-ms-request-server-encrypted", "false");
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private static void sendAzureError(HttpExchange exchange, int status, String errorCode) throws IOException {
        final Headers headers = exchange.getResponseHeaders();
        headers.add("Content-Type", "application/xml");
        if (errorCode != null) {
            headers.add("x-ms-error-code", errorCode);
        }
        final byte[] response = errorCode == null
            ? new byte[0]
            : ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>"
                + errorCode
                + "</Code><Message>"
                + errorCode
                + "</Message></Error>").getBytes(UTF_8);
        exchange.sendResponseHeaders(status, response.length == 0 ? -1 : response.length);
        if (response.length > 0) {
            exchange.getResponseBody().write(response);
        }
        exchange.close();
    }

    private static String quoted(String value) {
        return "\"" + value + "\"";
    }
}
