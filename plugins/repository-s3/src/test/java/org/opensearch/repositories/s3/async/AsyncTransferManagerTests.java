/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.junit.Before;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.repositories.blobstore.ZeroInputStream;
import org.opensearch.test.OpenSearchTestCase;
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
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

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
        asyncTransferManager = new AsyncTransferManager(
            ByteSizeUnit.MB.toBytes(5),
            Executors.newSingleThreadExecutor(),
            Executors.newSingleThreadExecutor()
        );
        super.setUp();
    }

    public void testOneChunkUpload() {
        CompletableFuture<PutObjectResponse> putObjectResponseCompletableFuture = new CompletableFuture<>();
        putObjectResponseCompletableFuture.complete(PutObjectResponse.builder().build());
        when(s3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class))).thenReturn(
            putObjectResponseCompletableFuture
        );

        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(1), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, false, null),
            new StreamContext(
                (partIdx, partSize, position) -> new InputStreamContainer(new ZeroInputStream(partSize), partSize, position),
                ByteSizeUnit.MB.toBytes(1),
                ByteSizeUnit.MB.toBytes(1),
                1
            )
        );

        try {
            resultFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            fail("did not expect resultFuture to fail");
        }

        verify(s3AsyncClient, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
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

        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(1), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, false, null),
            new StreamContext(
                (partIdx, partSize, position) -> new InputStreamContainer(new ZeroInputStream(partSize), partSize, position),
                ByteSizeUnit.MB.toBytes(1),
                ByteSizeUnit.MB.toBytes(1),
                1
            )
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

        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(5), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, true, 3376132981L),
            new StreamContext(
                (partIdx, partSize, position) -> new InputStreamContainer(new ZeroInputStream(partSize), partSize, position),
                ByteSizeUnit.MB.toBytes(1),
                ByteSizeUnit.MB.toBytes(1),
                5
            )
        );

        try {
            resultFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            fail("did not expect resultFuture to fail");
        }

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

        CompletableFuture<Void> resultFuture = asyncTransferManager.uploadObject(
            s3AsyncClient,
            new UploadRequest("bucket", "key", ByteSizeUnit.MB.toBytes(5), WritePriority.HIGH, uploadSuccess -> {
                // do nothing
            }, true, 0L),
            new StreamContext(
                (partIdx, partSize, position) -> new InputStreamContainer(new ZeroInputStream(partSize), partSize, position),
                ByteSizeUnit.MB.toBytes(1),
                ByteSizeUnit.MB.toBytes(1),
                5
            )
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
}
