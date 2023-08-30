/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Checksum;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesParts;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.opensearch.ExceptionsHelper;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.repositories.blobstore.ZeroInputStream;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.mockito.ArgumentMatchers;

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

    public void testGetBlobMetadata() throws Exception {
        final String checksum = randomAlphaOfLengthBetween(1, 10);
        final long objectSize = 100L;
        final int objectPartCount = 10;
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String bucketName = randomAlphaOfLengthBetween(1, 10);

        CompletableFuture<GetObjectAttributesResponse> getObjectAttributesResponseCompletableFuture = new CompletableFuture<>();
        getObjectAttributesResponseCompletableFuture.complete(
            GetObjectAttributesResponse.builder()
                .checksum(Checksum.builder().checksumCRC32(checksum).build())
                .objectSize(objectSize)
                .objectParts(GetObjectAttributesParts.builder().totalPartsCount(objectPartCount).build())
                .build()
        );
        when(s3AsyncClient.getObjectAttributes(any(GetObjectAttributesRequest.class))).thenReturn(
            getObjectAttributesResponseCompletableFuture
        );

        CompletableFuture<GetObjectAttributesResponse> responseFuture = asyncTransferManager.getBlobMetadata(
            s3AsyncClient,
            bucketName,
            blobName
        );
        GetObjectAttributesResponse objectAttributesResponse = responseFuture.get();

        assertEquals(checksum, objectAttributesResponse.checksum().checksumCRC32());
        assertEquals(Long.valueOf(objectSize), objectAttributesResponse.objectSize());
        assertEquals(Integer.valueOf(objectPartCount), objectAttributesResponse.objectParts().totalPartsCount());
    }

    public void testGetBlobPartInputStream() throws Exception {
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final long contentLength = 10L;
        final String contentRange = "bytes 0-10/100";
        final InputStream inputStream = ResponseInputStream.nullInputStream();

        GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength(contentLength).contentRange(contentRange).build();

        CompletableFuture<ResponseInputStream<GetObjectResponse>> getObjectPartResponse = new CompletableFuture<>();
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(getObjectResponse, inputStream);
        getObjectPartResponse.complete(responseInputStream);

        when(
            s3AsyncClient.getObject(
                any(GetObjectRequest.class),
                ArgumentMatchers.<AsyncResponseTransformer<GetObjectResponse, ResponseInputStream<GetObjectResponse>>>any()
            )
        ).thenReturn(getObjectPartResponse);

        InputStreamContainer inputStreamContainer = asyncTransferManager.getBlobPartInputStreamContainer(
            s3AsyncClient,
            bucketName,
            blobName,
            0
        ).get();

        assertEquals(0, inputStreamContainer.getOffset());
        assertEquals(contentLength, inputStreamContainer.getContentLength());
        assertEquals(inputStream.available(), inputStreamContainer.getInputStream().available());
    }

    public void testTransformResponseToInputStreamContainer() throws Exception {
        final String contentRange = "bytes 0-10/100";
        final long contentLength = 10L;
        final InputStream inputStream = ResponseInputStream.nullInputStream();

        GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength(contentLength).build();

        ResponseInputStream<GetObjectResponse> responseInputStreamNoRange = new ResponseInputStream<>(getObjectResponse, inputStream);
        assertThrows(SdkException.class, () -> asyncTransferManager.transformResponseToInputStreamContainer(responseInputStreamNoRange));

        getObjectResponse = GetObjectResponse.builder().contentRange(contentRange).build();
        ResponseInputStream<GetObjectResponse> responseInputStreamNoContentLength = new ResponseInputStream<>(
            getObjectResponse,
            inputStream
        );
        assertThrows(
            SdkException.class,
            () -> asyncTransferManager.transformResponseToInputStreamContainer(responseInputStreamNoContentLength)
        );

        getObjectResponse = GetObjectResponse.builder().contentRange(contentRange).contentLength(contentLength).build();
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(getObjectResponse, inputStream);
        InputStreamContainer inputStreamContainer = asyncTransferManager.transformResponseToInputStreamContainer(responseInputStream);
        assertEquals(contentLength, inputStreamContainer.getContentLength());
        assertEquals(0, inputStreamContainer.getOffset());
        assertEquals(inputStream.available(), inputStreamContainer.getInputStream().available());
    }
}
