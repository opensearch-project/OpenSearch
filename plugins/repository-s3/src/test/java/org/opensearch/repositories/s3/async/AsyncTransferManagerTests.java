/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.HttpStatusCode;
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
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.repositories.blobstore.ZeroInputStream;
import org.opensearch.repositories.s3.GenericStatsMetricPublisher;
import org.opensearch.repositories.s3.StatsMetricPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncTransferManagerTests extends OpenSearchTestCase {

    private AsyncTransferManager asyncTransferManager;
    private S3AsyncClient s3AsyncClient;

    @Override
    @Before
    public void setUp() throws Exception {
        s3AsyncClient = mock(S3AsyncClient.class);
        GenericStatsMetricPublisher genericStatsMetricPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
        asyncTransferManager = new AsyncTransferManager(
            ByteSizeUnit.MB.toBytes(5),
            Executors.newSingleThreadExecutor(),
            Executors.newSingleThreadExecutor(),
            Executors.newSingleThreadExecutor(),
            new TransferSemaphoresHolder(
                3,
                Math.max(Runtime.getRuntime().availableProcessors() * 5, 10),
                5,
                TimeUnit.MINUTES,
                genericStatsMetricPublisher
            )
        );
        super.setUp();
    }

    public void testOneChunkUpload() {
        CompletableFuture<PutObjectResponse> putObjectResponseCompletableFuture = new CompletableFuture<>();
        putObjectResponseCompletableFuture.complete(PutObjectResponse.builder().build());
        when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(
            putObjectResponseCompletableFuture
        );

        AtomicReference<InputStream> streamRef = new AtomicReference<>();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");
        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(1), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, false, null, true, metadata, ServerSideEncryption.AWS_KMS.toString(), randomAlphaOfLength(10), true, null, null),
            new StreamContext((partIdx, partSize, position) -> {
                streamRef.set(new ZeroInputStream(partSize));
                return new InputStreamContainer(streamRef.get(), partSize, position);
            }, ByteSizeUnit.MB.toBytes(1), ByteSizeUnit.MB.toBytes(1), 1),
            new StatsMetricPublisher()
        );

        try {
            resultFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            fail("did not expect resultFuture to fail");
        }

        verify(s3AsyncClient, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

        boolean closeError = false;
        try {
            streamRef.get().available();
        } catch (IOException e) {
            closeError = e.getMessage().equals("Stream closed");
        }
        assertTrue("InputStream was still open after upload", closeError);
    }

    public void testOneChunkUploadCorruption() {
        CompletableFuture<PutObjectResponse> putObjectResponseCompletableFuture = new CompletableFuture<>();
        putObjectResponseCompletableFuture.completeExceptionally(
            S3Exception.builder()
                .statusCode(HttpStatusCode.BAD_REQUEST)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("BadDigest").build())
                .build()
        );
        when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(
            putObjectResponseCompletableFuture
        );

        CompletableFuture<DeleteObjectResponse> deleteObjectResponseCompletableFuture = new CompletableFuture<>();
        deleteObjectResponseCompletableFuture.complete(DeleteObjectResponse.builder().build());
        when(s3AsyncClient.deleteObject(any(DeleteObjectRequest.class))).thenReturn(deleteObjectResponseCompletableFuture);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(1), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, false, null, true, metadata, ServerSideEncryption.AWS_KMS.toString(), randomAlphaOfLength(10), true, null, null),
            new StreamContext(
                (partIdx, partSize, position) -> new InputStreamContainer(new ZeroInputStream(partSize), partSize, position),
                ByteSizeUnit.MB.toBytes(1),
                ByteSizeUnit.MB.toBytes(1),
                1
            ),
            new StatsMetricPublisher()
        );

        try {
            resultFuture.get();
            fail("did not expect resultFuture to pass");
        } catch (ExecutionException | InterruptedException e) {
            Throwable throwable = ExceptionsHelper.unwrap(e, CorruptFileException.class);
            assertNotNull(throwable);
            assertTrue(throwable instanceof CorruptFileException);
        }

        verify(s3AsyncClient, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        verify(s3AsyncClient, times(1)).deleteObject(any(DeleteObjectRequest.class));
    }

    public void testMultipartUpload() {
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadRequestCompletableFuture = new CompletableFuture<>();
        createMultipartUploadRequestCompletableFuture.complete(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());
        when(s3AsyncClient.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
            createMultipartUploadRequestCompletableFuture
        );

        CompletableFuture<UploadPartResponse> uploadPartResponseCompletableFuture = new CompletableFuture<>();
        uploadPartResponseCompletableFuture.complete(UploadPartResponse.builder().checksumCRC32("pzjqHA==").build());
        when(s3AsyncClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenReturn(
            uploadPartResponseCompletableFuture
        );

        CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUploadResponseCompletableFuture = new CompletableFuture<>();
        completeMultipartUploadResponseCompletableFuture.complete(CompleteMultipartUploadResponse.builder().build());
        when(s3AsyncClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(
            completeMultipartUploadResponseCompletableFuture
        );

        CompletableFuture<AbortMultipartUploadResponse> abortMultipartUploadResponseCompletableFuture = new CompletableFuture<>();
        abortMultipartUploadResponseCompletableFuture.complete(AbortMultipartUploadResponse.builder().build());
        when(s3AsyncClient.abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(
            abortMultipartUploadResponseCompletableFuture
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        List<InputStream> streams = new ArrayList<>();
        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(5), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, true, 3376132981L, true, metadata, ServerSideEncryption.AWS_KMS.toString(), randomAlphaOfLength(10), true, null, null),
            new StreamContext((partIdx, partSize, position) -> {
                InputStream stream = new ZeroInputStream(partSize);
                streams.add(stream);
                return new InputStreamContainer(stream, partSize, position);
            }, ByteSizeUnit.MB.toBytes(1), ByteSizeUnit.MB.toBytes(1), 5),
            new StatsMetricPublisher()
        );

        try {
            resultFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            fail("did not expect resultFuture to fail");
        }

        streams.forEach(stream -> {
            boolean closeError = false;
            try {
                stream.available();
            } catch (IOException e) {
                closeError = e.getMessage().equals("Stream closed");
            }
            assertTrue("InputStream was still open after upload", closeError);
        });

        verify(s3AsyncClient, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3AsyncClient, times(5)).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
        verify(s3AsyncClient, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3AsyncClient, times(0)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    public void testMultipartUploadCorruption() {
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadRequestCompletableFuture = new CompletableFuture<>();
        createMultipartUploadRequestCompletableFuture.complete(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());
        when(s3AsyncClient.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
            createMultipartUploadRequestCompletableFuture
        );

        CompletableFuture<UploadPartResponse> uploadPartResponseCompletableFuture = new CompletableFuture<>();
        uploadPartResponseCompletableFuture.complete(UploadPartResponse.builder().checksumCRC32("pzjqHA==").build());
        when(s3AsyncClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenReturn(
            uploadPartResponseCompletableFuture
        );

        CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUploadResponseCompletableFuture = new CompletableFuture<>();
        completeMultipartUploadResponseCompletableFuture.complete(CompleteMultipartUploadResponse.builder().build());
        when(s3AsyncClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(
            completeMultipartUploadResponseCompletableFuture
        );

        CompletableFuture<AbortMultipartUploadResponse> abortMultipartUploadResponseCompletableFuture = new CompletableFuture<>();
        abortMultipartUploadResponseCompletableFuture.complete(AbortMultipartUploadResponse.builder().build());
        when(s3AsyncClient.abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(
            abortMultipartUploadResponseCompletableFuture
        );

        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(5), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, true, 0L, true, metadata, ServerSideEncryption.AWS_KMS.toString(), randomAlphaOfLength(10), true, null, null),
            new StreamContext(
                (partIdx, partSize, position) -> new InputStreamContainer(new ZeroInputStream(partSize), partSize, position),
                ByteSizeUnit.MB.toBytes(1),
                ByteSizeUnit.MB.toBytes(1),
                5
            ),
            new StatsMetricPublisher()
        );

        try {
            resultFuture.get();
            fail("did not expect resultFuture to pass");
        } catch (ExecutionException | InterruptedException e) {
            Throwable throwable = ExceptionsHelper.unwrap(e, CorruptFileException.class);
            assertNotNull(throwable);
            assertTrue(throwable instanceof CorruptFileException);
        }

        verify(s3AsyncClient, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3AsyncClient, times(5)).uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class));
        verify(s3AsyncClient, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3AsyncClient, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    /**
     * Regression: when provideStream() throws for one part (e.g., AlreadyClosedException from a
     * concurrently-merged Lucene segment), the upload must fail cleanly rather than silently
     * leaving a null slot in inputStreamContainers and later NPE-ing in mergeAndVerifyChecksum.
     *
     * Root cause: the catch block in AsyncPartsHandler.uploadParts() swallowed the exception and
     * added no future, so allOfExceptionForwarded() completed successfully on the shorter futures
     * list, then mergeAndVerifyChecksum() dereferenced the null slot — producing a confusing
     * "Failed to send multipart upload requests" SdkClientException wrapping an NPE instead of
     * the original IOException.
     *
     * Fix: add a pre-failed CompletableFuture for any part whose provideStream() throws, so
     * allOfExceptionForwarded() sees the failure, the upload is aborted, and the original
     * exception propagates to the caller.
     */
    public void testMultipartUploadProvideStreamExceptionOnMiddlePart() throws Exception {
        setUpMultipartMocks();

        // Part index 2 (middle) throws — simulates AlreadyClosedException from mmap'd segment
        CompletableFuture<Void> result = asyncTransferManager.uploadObject(
            s3AsyncClient,
            buildUploadRequest(true, 3376132981L),
            new StreamContext((partIdx, partSize, position) -> {
                if (partIdx == 2) {
                    throw new IOException("Already closed: MemorySegmentIndexInput(path=\"_merged_430d.parquet\")");
                }
                return new InputStreamContainer(new ZeroInputStream(partSize), partSize, position);
            }, ByteSizeUnit.MB.toBytes(1), ByteSizeUnit.MB.toBytes(1), 5),
            new StatsMetricPublisher()
        );

        ExecutionException ex = expectThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));

        // Must NOT be NPE (was the pre-fix symptom)
        assertFalse(
            "Must not surface as NullPointerException",
            ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(ex, t -> t instanceof NullPointerException).isPresent()
        );
        // Original IOException must appear in the cause chain
        assertTrue(
            "Original IOException from provideStream must be in the cause chain",
            ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(
                ex,
                t -> t instanceof IOException && t.getMessage().contains("Already closed")
            ).isPresent()
        );

        verify(s3AsyncClient, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3AsyncClient, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3AsyncClient, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    /**
     * Regression: same fix, first part fails. Ensures the mismatch between futures list size (0)
     * and numberOfParts (5) is handled — previously allOfExceptionForwarded on an empty list
     * completed immediately, then mergeAndVerifyChecksum NPE'd on slot 0.
     */
    public void testMultipartUploadProvideStreamExceptionOnFirstPart() throws Exception {
        setUpMultipartMocks();

        CompletableFuture<Void> result = asyncTransferManager.uploadObject(
            s3AsyncClient,
            buildUploadRequest(true, 3376132981L),
            new StreamContext((partIdx, partSize, position) -> {
                if (partIdx == 0) {
                    throw new IOException("Already closed: MemorySegmentIndexInput(path=\"_merged_430d.parquet\")");
                }
                return new InputStreamContainer(new ZeroInputStream(partSize), partSize, position);
            }, ByteSizeUnit.MB.toBytes(1), ByteSizeUnit.MB.toBytes(1), 5),
            new StatsMetricPublisher()
        );

        ExecutionException ex = expectThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
        assertFalse(
            "Must not surface as NullPointerException",
            ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(ex, t -> t instanceof NullPointerException).isPresent()
        );
        assertTrue(
            "Original IOException must be in the cause chain",
            ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(
                ex,
                t -> t instanceof IOException && t.getMessage().contains("Already closed")
            ).isPresent()
        );
        verify(s3AsyncClient, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3AsyncClient, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3AsyncClient, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    /**
     * Regression: all parts fail — futures list is empty. allOfExceptionForwarded on [] used to
     * complete immediately with success, then NPE on every slot. With the fix every part adds a
     * failed future so the first failure propagates correctly.
     */
    public void testMultipartUploadProvideStreamExceptionOnAllParts() throws Exception {
        setUpMultipartMocks();

        CompletableFuture<Void> result = asyncTransferManager.uploadObject(
            s3AsyncClient,
            buildUploadRequest(true, 3376132981L),
            new StreamContext((partIdx, partSize, position) -> {
                throw new IOException("Already closed: MemorySegmentIndexInput(path=\"_merged_430d.parquet\")");
            }, ByteSizeUnit.MB.toBytes(1), ByteSizeUnit.MB.toBytes(1), 5),
            new StatsMetricPublisher()
        );

        ExecutionException ex = expectThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
        assertFalse(
            "Must not surface as NullPointerException",
            ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(ex, t -> t instanceof NullPointerException).isPresent()
        );
        assertTrue(
            "Original IOException must be in the cause chain",
            ExceptionsHelper.<IOException>unwrapCausesAndSuppressed(
                ex,
                t -> t instanceof IOException && t.getMessage().contains("Already closed")
            ).isPresent()
        );
        verify(s3AsyncClient, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(s3AsyncClient, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        verify(s3AsyncClient, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private void setUpMultipartMocks() {
        CompletableFuture<CreateMultipartUploadResponse> createFuture = new CompletableFuture<>();
        createFuture.complete(CreateMultipartUploadResponse.builder().uploadId("uploadId").build());
        when(s3AsyncClient.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(createFuture);

        CompletableFuture<UploadPartResponse> partFuture = new CompletableFuture<>();
        partFuture.complete(UploadPartResponse.builder().checksumCRC32("pzjqHA==").build());
        when(s3AsyncClient.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class))).thenReturn(partFuture);

        CompletableFuture<AbortMultipartUploadResponse> abortFuture = new CompletableFuture<>();
        abortFuture.complete(AbortMultipartUploadResponse.builder().build());
        when(s3AsyncClient.abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(abortFuture);
    }

    private UploadRequest buildUploadRequest(boolean integrityCheck, long checksum) {
        return new UploadRequest(
            "bucket",
            "key",
            ByteSizeUnit.MB.toBytes(5),
            WritePriority.HIGH,
            uploadSuccess -> {},
            integrityCheck,
            checksum,
            true,
            new HashMap<>(),
            ServerSideEncryption.AWS_KMS.toString(),
            randomAlphaOfLength(10),
            true,
            null,
            null
        );
    }
}
