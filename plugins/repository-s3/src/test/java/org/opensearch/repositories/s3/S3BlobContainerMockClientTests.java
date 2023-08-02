/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import org.apache.lucene.store.IndexInput;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.StreamContextSupplier;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.AsyncTransferEventLoopGroup;
import org.opensearch.test.OpenSearchTestCase;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class S3BlobContainerMockClientTests extends OpenSearchTestCase implements ConfigPathSupport {

    private MockS3AsyncService asyncService;
    private ExecutorService futureCompletionService;
    private ExecutorService streamReaderService;
    private AsyncTransferEventLoopGroup transferNIOGroup;
    private S3BlobContainer blobContainer;

    static class MockS3AsyncService extends S3AsyncService {

        private final S3AsyncClient asyncClient = mock(S3AsyncClient.class);
        private final int maxDelayInFutureCompletionMillis;

        private boolean failPutObjectRequest;
        private boolean failCreateMultipartUploadRequest;
        private boolean failUploadPartRequest;
        private boolean failCompleteMultipartUploadRequest;

        private String multipartUploadId;

        public MockS3AsyncService(Path configPath, int maxDelayInFutureCompletionMillis) {
            super(configPath);
            this.maxDelayInFutureCompletionMillis = maxDelayInFutureCompletionMillis;
        }

        public void initializeMocks(
            boolean failPutObjectRequest,
            boolean failCreateMultipartUploadRequest,
            boolean failUploadPartRequest,
            boolean failCompleteMultipartUploadRequest
        ) {
            setupFailureBooleans(
                failPutObjectRequest,
                failCreateMultipartUploadRequest,
                failUploadPartRequest,
                failCompleteMultipartUploadRequest
            );
            doAnswer(this::doOnPutObject).when(asyncClient).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
            doAnswer(this::doOnDeleteObject).when(asyncClient).deleteObject(any(DeleteObjectRequest.class));
            doAnswer(this::doOnCreateMultipartUpload).when(asyncClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            doAnswer(this::doOnPartUpload).when(asyncClient).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
            doAnswer(this::doOnCompleteMultipartUpload).when(asyncClient)
                .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            doAnswer(this::doOnAbortMultipartUpload).when(asyncClient).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }

        private void setupFailureBooleans(
            boolean failPutObjectRequest,
            boolean failCreateMultipartUploadRequest,
            boolean failUploadPartRequest,
            boolean failCompleteMultipartUploadRequest
        ) {
            this.failPutObjectRequest = failPutObjectRequest;
            this.failCreateMultipartUploadRequest = failCreateMultipartUploadRequest;
            this.failUploadPartRequest = failUploadPartRequest;
            this.failCompleteMultipartUploadRequest = failCompleteMultipartUploadRequest;
        }

        private CompletableFuture<PutObjectResponse> doOnPutObject(InvocationOnMock invocationOnMock) {
            CompletableFuture<PutObjectResponse> completableFuture = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    Thread.sleep(randomInt(maxDelayInFutureCompletionMillis));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (failPutObjectRequest) {
                    completableFuture.completeExceptionally(new IOException());
                } else {
                    completableFuture.complete(PutObjectResponse.builder().build());
                }
            }).start();

            return completableFuture;
        }

        private CompletableFuture<DeleteObjectResponse> doOnDeleteObject(InvocationOnMock invocationOnMock) {
            CompletableFuture<DeleteObjectResponse> completableFuture = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    Thread.sleep(randomInt(maxDelayInFutureCompletionMillis));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (failPutObjectRequest) {
                    completableFuture.completeExceptionally(new IOException());
                } else {
                    completableFuture.complete(DeleteObjectResponse.builder().build());
                }
            }).start();

            return completableFuture;
        }

        private CompletableFuture<CreateMultipartUploadResponse> doOnCreateMultipartUpload(InvocationOnMock invocationOnMock) {
            multipartUploadId = randomAlphaOfLength(5);
            CompletableFuture<CreateMultipartUploadResponse> completableFuture = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    Thread.sleep(randomInt(maxDelayInFutureCompletionMillis));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (failCreateMultipartUploadRequest) {
                    completableFuture.completeExceptionally(new IOException());
                } else {
                    completableFuture.complete(CreateMultipartUploadResponse.builder().uploadId(multipartUploadId).build());
                }
            }).start();

            return completableFuture;
        }

        private CompletableFuture<UploadPartResponse> doOnPartUpload(InvocationOnMock invocationOnMock) {
            UploadPartRequest uploadPartRequest = invocationOnMock.getArgument(0);
            assertEquals(multipartUploadId, uploadPartRequest.uploadId());
            CompletableFuture<UploadPartResponse> completableFuture = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    Thread.sleep(randomInt(maxDelayInFutureCompletionMillis));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (failUploadPartRequest) {
                    completableFuture.completeExceptionally(new IOException());
                } else {
                    completableFuture.complete(UploadPartResponse.builder().eTag("eTag").build());
                }
            }).start();

            return completableFuture;
        }

        private CompletableFuture<CompleteMultipartUploadResponse> doOnCompleteMultipartUpload(InvocationOnMock invocationOnMock) {
            CompleteMultipartUploadRequest completeMultipartUploadRequest = invocationOnMock.getArgument(0);
            assertEquals(multipartUploadId, completeMultipartUploadRequest.uploadId());
            CompletableFuture<CompleteMultipartUploadResponse> completableFuture = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    Thread.sleep(randomInt(maxDelayInFutureCompletionMillis));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (failCompleteMultipartUploadRequest) {
                    completableFuture.completeExceptionally(new IOException());
                } else {
                    completableFuture.complete(CompleteMultipartUploadResponse.builder().build());
                }
            }).start();

            return completableFuture;
        }

        private CompletableFuture<AbortMultipartUploadResponse> doOnAbortMultipartUpload(InvocationOnMock invocationOnMock) {
            AbortMultipartUploadRequest abortMultipartUploadRequest = invocationOnMock.getArgument(0);
            assertEquals(multipartUploadId, abortMultipartUploadRequest.uploadId());
            CompletableFuture<AbortMultipartUploadResponse> completableFuture = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    Thread.sleep(randomInt(maxDelayInFutureCompletionMillis));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                completableFuture.complete(AbortMultipartUploadResponse.builder().build());

            }).start();

            return completableFuture;
        }

        public void verifyMultipartUploadCallCount(int numberOfParts, boolean finalizeUploadFailure) {
            verify(asyncClient, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(asyncClient, times(!failCreateMultipartUploadRequest ? numberOfParts : 0)).uploadPart(
                any(UploadPartRequest.class),
                any(AsyncRequestBody.class)
            );
            verify(asyncClient, times(!failCreateMultipartUploadRequest && !failUploadPartRequest && !finalizeUploadFailure ? 1 : 0))
                .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(
                asyncClient,
                times(
                    (!failCreateMultipartUploadRequest && (failUploadPartRequest || failCompleteMultipartUploadRequest))
                        || finalizeUploadFailure ? 1 : 0
                )
            ).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }

        public void verifySingleChunkUploadCallCount(boolean finalizeUploadFailure) {
            verify(asyncClient, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
            verify(asyncClient, times(finalizeUploadFailure ? 1 : 0)).deleteObject(any(DeleteObjectRequest.class));
        }

        @Override
        public AmazonAsyncS3Reference client(
            RepositoryMetadata repositoryMetadata,
            AsyncExecutorContainer priorityExecutorBuilder,
            AsyncExecutorContainer normalExecutorBuilder
        ) {
            return new AmazonAsyncS3Reference(AmazonAsyncS3WithCredentials.create(asyncClient, asyncClient, null));
        }
    }

    /**
     * An IndexInput implementation that serves only zeroes
     */
    static class ZeroIndexInput extends IndexInput {

        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicLong reads = new AtomicLong(0);
        private final long length;

        /**
         * @param resourceDescription resourceDescription should be a non-null, opaque string describing this resource; it's returned
         *                            from {@link #toString}.
         */
        public ZeroIndexInput(String resourceDescription, final long length) {
            super(resourceDescription);
            this.length = length;
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
        }

        @Override
        public long getFilePointer() {
            return reads.get();
        }

        @Override
        public void seek(long pos) throws IOException {
            reads.set(pos);
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new ZeroIndexInput(sliceDescription, length);
        }

        @Override
        public byte readByte() throws IOException {
            ensureOpen();
            return (byte) ((reads.incrementAndGet() <= length) ? 0 : -1);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            ensureOpen();
            final long available = available();
            final int toCopy = Math.min(len, (int) available);
            Arrays.fill(b, offset, offset + toCopy, (byte) 0);
            reads.addAndGet(toCopy);
        }

        private long available() {
            return Math.max(length - reads.get(), 0);
        }

        private void ensureOpen() throws IOException {
            if (closed.get()) {
                throw new IOException("Stream closed");
            }
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        asyncService = new MockS3AsyncService(configPath(), 1000);
        futureCompletionService = Executors.newSingleThreadExecutor();
        streamReaderService = Executors.newSingleThreadExecutor();
        transferNIOGroup = new AsyncTransferEventLoopGroup(1);
        blobContainer = createBlobContainer();
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        IOUtils.close(asyncService);
        super.tearDown();
    }

    private S3BlobContainer createBlobContainer() {
        return new S3BlobContainer(BlobPath.cleanPath(), createBlobStore());
    }

    private S3BlobStore createBlobStore() {
        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);

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

        return new S3BlobStore(
            null,
            asyncService,
            true,
            "bucket",
            S3Repository.SERVER_SIDE_ENCRYPTION_SETTING.getDefault(Settings.EMPTY),
            S3Repository.BUFFER_SIZE_SETTING.getDefault(Settings.EMPTY),
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
        );
    }

    public void testWriteBlobByStreamsNoFailure() throws IOException, ExecutionException, InterruptedException {
        asyncService.initializeMocks(false, false, false, false);
        testWriteBlobByStreamsLargeBlob(false, false);
    }

    public void testWriteBlobByStreamsFinalizeUploadFailure() throws IOException, ExecutionException, InterruptedException {
        asyncService.initializeMocks(false, false, false, false);
        testWriteBlobByStreamsLargeBlob(false, true);
    }

    public void testWriteBlobByStreamsCreateMultipartRequestFailure() throws IOException, ExecutionException, InterruptedException {
        asyncService.initializeMocks(false, true, false, false);
        testWriteBlobByStreamsLargeBlob(true, false);
    }

    public void testWriteBlobByStreamsUploadPartRequestFailure() throws IOException, ExecutionException, InterruptedException {
        asyncService.initializeMocks(false, false, true, false);
        testWriteBlobByStreamsLargeBlob(true, false);
    }

    public void testWriteBlobByStreamsCompleteMultipartRequestFailure() throws IOException, ExecutionException, InterruptedException {
        asyncService.initializeMocks(false, false, false, true);
        testWriteBlobByStreamsLargeBlob(true, false);
    }

    public void testWriteBlobByStreamsSingleChunkUploadNoFailure() throws IOException, ExecutionException, InterruptedException {
        asyncService.initializeMocks(false, false, false, false);
        testWriteBlobByStreams(false, false);
    }

    public void testWriteBlobByStreamsSingleChunkUploadPutObjectFailure() throws IOException, ExecutionException, InterruptedException {
        asyncService.initializeMocks(true, false, false, false);
        testWriteBlobByStreams(true, false);
    }

    public void testWriteBlobByStreamsSingleChunkUploadFinalizeUploadFailure() throws IOException, ExecutionException,
        InterruptedException {
        asyncService.initializeMocks(false, false, false, false);
        testWriteBlobByStreams(false, true);
    }

    private void testWriteBlobByStreams(boolean expectException, boolean throwExceptionOnFinalizeUpload) throws IOException,
        ExecutionException, InterruptedException {
        final byte[] bytes = randomByteArrayOfLength(100);
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
        }, bytes.length, false, WritePriority.NORMAL, uploadSuccess -> {
            assertTrue(uploadSuccess);
            if (throwExceptionOnFinalizeUpload) {
                throw new RuntimeException();
            }
        }, false, null), completionListener);

        assertTrue(countDownLatch.await(5000, TimeUnit.SECONDS));
        // wait for completableFuture to finish
        if (expectException || throwExceptionOnFinalizeUpload) {
            assertNotNull(exceptionRef.get());
        }

        asyncService.verifySingleChunkUploadCallCount(throwExceptionOnFinalizeUpload);

        openInputStreams.forEach(inputStream -> {
            try {
                inputStream.close();
            } catch (IOException e) {
                fail("Failure while closing open input streams");
            }
        });
    }

    private void testWriteBlobByStreamsLargeBlob(boolean expectException, boolean throwExceptionOnFinalizeUpload) throws IOException,
        ExecutionException, InterruptedException {
        final ByteSizeValue partSize = S3Repository.PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING.getDefault(Settings.EMPTY);

        int numberOfParts = randomIntBetween(2, 5);
        final long lastPartSize = randomLongBetween(10, 512);
        final long blobSize = ((numberOfParts - 1) * partSize.getBytes()) + lastPartSize;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        ActionListener<Void> completionListener = ActionListener.wrap(resp -> { countDownLatch.countDown(); }, ex -> {
            exceptionRef.set(ex);
            countDownLatch.countDown();
        });
        List<InputStream> openInputStreams = new ArrayList<>();
        blobContainer.asyncBlobUpload(new WriteContext("write_large_blob", new StreamContextSupplier() {
            @Override
            public StreamContext supplyStreamContext(long partSize) {
                return new StreamContext(new CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException>() {
                    @Override
                    public InputStreamContainer apply(Integer partNo, Long size, Long position) throws IOException {
                        InputStream inputStream = new OffsetRangeIndexInputStream(new ZeroIndexInput("desc", blobSize), size, position);
                        openInputStreams.add(inputStream);
                        return new InputStreamContainer(inputStream, size, position);
                    }
                }, partSize, calculateLastPartSize(blobSize, partSize), calculateNumberOfParts(blobSize, partSize));
            }
        }, blobSize, false, WritePriority.HIGH, uploadSuccess -> {
            assertTrue(uploadSuccess);
            if (throwExceptionOnFinalizeUpload) {
                throw new RuntimeException();
            }
        }, false, null), completionListener);

        assertTrue(countDownLatch.await(5000, TimeUnit.SECONDS));
        if (expectException || throwExceptionOnFinalizeUpload) {
            assertNotNull(exceptionRef.get());
        }

        asyncService.verifyMultipartUploadCallCount(numberOfParts, throwExceptionOnFinalizeUpload);

        openInputStreams.forEach(inputStream -> {
            try {
                inputStream.close();
            } catch (IOException ex) {
                logger.error("Error closing input stream");
            }
        });
    }

    private long calculateLastPartSize(long totalSize, long partSize) {
        return totalSize % partSize == 0 ? partSize : totalSize % partSize;
    }

    private int calculateNumberOfParts(long contentLength, long partSize) {
        return (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);
    }
}
