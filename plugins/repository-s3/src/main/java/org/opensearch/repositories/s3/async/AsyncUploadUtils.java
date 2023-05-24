/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import com.jcraft.jzlib.JZlib;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.util.ByteUtils;
import org.opensearch.repositories.s3.io.InputStreamCRC32Container;
import org.opensearch.repositories.s3.SocketAccess;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * A helper class that automatically uses multipart upload based on the size of the source object
 */
public final class AsyncUploadUtils {
    private static final Logger log = LogManager.getLogger(AsyncUploadUtils.class);
    private final ExecutorService executorService;
    private final ExecutorService priorityExecutorService;
    private final long minimumPartSize;

    /**
     * The max number of parts on S3 side is 10,000
     */
    private static final long MAX_UPLOAD_PARTS = 10_000;

    /**
     * Construct a new object of AsyncUploadUtils
     *
     * @param minimumPartSize         The minimum part size for parallel multipart uploads
     * @param executorService         The stream reader {@link ExecutorService} for normal priority uploads
     * @param priorityExecutorService the stream read {@link ExecutorService} for high priority uploads
     */
    public AsyncUploadUtils(long minimumPartSize, ExecutorService executorService, ExecutorService priorityExecutorService) {
        this.executorService = executorService;
        this.priorityExecutorService = priorityExecutorService;
        this.minimumPartSize = minimumPartSize;
    }

    /**
     * Upload an object to S3 using the async client
     *
     * @param s3AsyncClient The {@link S3AsyncClient} to use for uploads
     * @param uploadRequest The {@link UploadRequest} object encapsulating all relevant details for upload
     * @param streamContext The {@link StreamContext} to supply streams during upload
     * @return A {@link CompletableFuture} to listen for upload completion
     */
    public CompletableFuture<Void> uploadObject(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest, StreamContext streamContext) {

        CompletableFuture<Void> returnFuture = new CompletableFuture<>();
        try {
            if (streamContext.getNumberOfParts() == 1) {
                log.debug(() -> "Starting the upload as a single upload part request");
                uploadInOneChunk(s3AsyncClient, uploadRequest, streamContext.provideStream(0), returnFuture);
            } else {
                log.debug(() -> "Starting the upload as multipart upload request");
                uploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture);
            }
        } catch (Throwable throwable) {
            returnFuture.completeExceptionally(throwable);
        }

        return returnFuture;
    }

    private void uploadInParts(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        CompletableFuture<Void> returnFuture
    ) {

        CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey());
        if (uploadRequest.doRemoteDataIntegrityCheck()) {
            createMultipartUploadRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
        }
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadFuture = SocketAccess.doPrivileged(
            () -> s3AsyncClient.createMultipartUpload(createMultipartUploadRequestBuilder.build())
        );

        // Ensure cancellations are forwarded to the createMultipartUploadFuture future
        CompletableFutureUtils.forwardExceptionTo(returnFuture, createMultipartUploadFuture);

        createMultipartUploadFuture.whenComplete((createMultipartUploadResponse, throwable) -> {
            if (throwable != null) {
                handleException(returnFuture, () -> "Failed to initiate multipart upload", throwable);
            } else {
                log.debug(() -> "Initiated new multipart upload, uploadId: " + createMultipartUploadResponse.uploadId());
                doUploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture, createMultipartUploadResponse.uploadId());
            }
        });
    }

    private void doUploadInParts(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        CompletableFuture<Void> returnFuture,
        String uploadId
    ) {

        // The list of completed parts must be sorted
        AtomicReferenceArray<CompletedPart> completedParts = new AtomicReferenceArray<>(streamContext.getNumberOfParts());
        AtomicReferenceArray<InputStreamCRC32Container> inputStreamContainers = new AtomicReferenceArray<>(
            streamContext.getNumberOfParts()
        );

        List<CompletableFuture<CompletedPart>> futures;
        try {
            futures = sendUploadPartRequests(s3AsyncClient, uploadRequest, streamContext, uploadId, completedParts, inputStreamContainers);
        } catch (Exception ex) {
            try {
                cleanUpParts(s3AsyncClient, uploadRequest, uploadId);
            } finally {
                returnFuture.completeExceptionally(ex);
            }
            return;
        }

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).thenApply(resp -> {
            uploadRequest.getUploadFinalizer().accept(true);
            return resp;
        })
            .thenApply(ignore -> {
                if (uploadRequest.doRemoteDataIntegrityCheck()) {
                    mergeAndVerifyChecksum(inputStreamContainers, uploadRequest.getKey(), uploadRequest.getExpectedChecksum());
                }
                return null;
            })
            .thenCompose(ignore -> completeMultipartUpload(s3AsyncClient, uploadRequest, uploadId, completedParts))
            .handle(handleExceptionOrResponse(s3AsyncClient, uploadRequest, returnFuture, uploadId))
            .exceptionally(throwable -> {
                handleException(returnFuture, () -> "Unexpected exception occurred", throwable);
                return null;
            });
    }

    private void mergeAndVerifyChecksum(
        AtomicReferenceArray<InputStreamCRC32Container> inputStreamContainers,
        String fileName,
        long expectedChecksum
    ) {
        long resultantChecksum = fromBase64String(inputStreamContainers.get(0).getChecksum());
        for (int index = 1; index < inputStreamContainers.length(); index++) {
            long curChecksum = fromBase64String(inputStreamContainers.get(index).getChecksum());
            resultantChecksum = JZlib.crc32_combine(resultantChecksum, curChecksum, inputStreamContainers.get(index).getContentLength());
        }

        if (resultantChecksum != expectedChecksum) {
            throw new RuntimeException(new CorruptFileException("File level checksums didn't match combined part checksums", fileName));
        }
    }

    private BiFunction<CompleteMultipartUploadResponse, Throwable, Void> handleExceptionOrResponse(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        CompletableFuture<Void> returnFuture,
        String uploadId
    ) {

        return (response, throwable) -> {
            if (throwable != null) {
                cleanUpParts(s3AsyncClient, uploadRequest, uploadId);
                handleException(returnFuture, () -> "Failed to send multipart upload requests.", throwable);
            } else {
                returnFuture.complete(null);
            }

            return null;
        };
    }

    private CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        String uploadId,
        AtomicReferenceArray<CompletedPart> completedParts
    ) {

        log.debug(() -> new ParameterizedMessage("Sending completeMultipartUploadRequest, uploadId: {}", uploadId));
        CompletedPart[] parts = IntStream.range(0, completedParts.length()).mapToObj(completedParts::get).toArray(CompletedPart[]::new);
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
            .build();

        return SocketAccess.doPrivileged(() -> s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest));
    }

    private static String base64StringFromLong(Long val) {
        return Base64.getEncoder().encodeToString(Arrays.copyOfRange(ByteUtils.toByteArrayBE(val), 4, 8));
    }

    private static long fromBase64String(String base64String) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64String);
        if (decodedBytes.length != 4) {
            throw new IllegalArgumentException("Invalid Base64 encoded CRC32 checksum");
        }
        long result = 0;
        for (int i = 0; i < 4; i++) {
            result <<= 8;
            result |= (decodedBytes[i] & 0xFF);
        }
        return result;
    }

    private void cleanUpParts(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest, String uploadId) {

        AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .uploadId(uploadId)
            .build();
        SocketAccess.doPrivileged(() -> s3AsyncClient.abortMultipartUpload(abortMultipartUploadRequest).exceptionally(throwable -> {
            log.warn(
                () -> new ParameterizedMessage(
                    "Failed to abort previous multipart upload "
                        + "(id: {})"
                        + ". You may need to call "
                        + "S3AsyncClient#abortMultiPartUpload to "
                        + "free all storage consumed by"
                        + " all parts. ",
                    uploadId
                ),
                throwable
            );
            return null;
        }));
    }

    private static void handleException(CompletableFuture<Void> returnFuture, Supplier<String> message, Throwable throwable) {
        Throwable cause = throwable instanceof CompletionException ? throwable.getCause() : throwable;

        if (cause instanceof Error) {
            returnFuture.completeExceptionally(cause);
        } else {
            SdkClientException exception = SdkClientException.create(message.get(), cause);
            returnFuture.completeExceptionally(exception);
        }
    }

    private List<CompletableFuture<CompletedPart>> sendUploadPartRequests(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        String uploadId,
        AtomicReferenceArray<CompletedPart> completedParts,
        AtomicReferenceArray<InputStreamCRC32Container> inputStreamContainers
    ) throws IOException {
        List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();
        for (int partIdx = 0; partIdx < streamContext.getNumberOfParts(); partIdx++) {
            InputStreamContainer inputStreamContainer = streamContext.provideStream(partIdx);
            inputStreamContainers.set(
                partIdx,
                new InputStreamCRC32Container(inputStreamContainer.getInputStream(), inputStreamContainer.getContentLength())
            );
            UploadPartRequest.Builder uploadPartRequestBuilder = UploadPartRequest.builder()
                .bucket(uploadRequest.getBucket())
                .partNumber(partIdx + 1)
                .key(uploadRequest.getKey())
                .uploadId(uploadId)
                .contentLength(inputStreamContainer.getContentLength());
            if (uploadRequest.doRemoteDataIntegrityCheck()) {
                uploadPartRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
            }
            sendIndividualUploadPart(
                s3AsyncClient,
                completedParts,
                inputStreamContainers,
                futures,
                uploadPartRequestBuilder.build(),
                inputStreamContainer,
                uploadRequest
            );
        }

        return futures;
    }

    private void sendIndividualUploadPart(
        S3AsyncClient s3AsyncClient,
        AtomicReferenceArray<CompletedPart> completedParts,
        AtomicReferenceArray<InputStreamCRC32Container> inputStreamContainers,
        List<CompletableFuture<CompletedPart>> futures,
        UploadPartRequest uploadPartRequest,
        InputStreamContainer inputStreamContainer,
        UploadRequest uploadRequest
    ) {
        Integer partNumber = uploadPartRequest.partNumber();

        ExecutorService streamReadExecutor = uploadRequest.getWritePriority() == WritePriority.HIGH
            ? priorityExecutorService
            : executorService;
        CompletableFuture<UploadPartResponse> uploadPartResponseFuture = SocketAccess.doPrivileged(
            () -> s3AsyncClient.uploadPart(
                uploadPartRequest,
                AsyncRequestBody.fromInputStream(
                    inputStreamContainer.getInputStream(),
                    inputStreamContainer.getContentLength(),
                    streamReadExecutor
                )
            )
        );

        CompletableFuture<CompletedPart> convertFuture = uploadPartResponseFuture.thenApply(
            uploadPartResponse -> convertUploadPartResponse(
                completedParts,
                inputStreamContainers,
                uploadPartResponse,
                partNumber,
                uploadRequest.doRemoteDataIntegrityCheck()
            )
        );
        futures.add(convertFuture);

        CompletableFutureUtils.forwardExceptionTo(convertFuture, uploadPartResponseFuture);
    }

    private CompletedPart convertUploadPartResponse(
        AtomicReferenceArray<CompletedPart> completedParts,
        AtomicReferenceArray<InputStreamCRC32Container> inputStreamContainers,
        UploadPartResponse partResponse,
        int partNumber,
        boolean isRemoteDataIntegrityCheckEnabled
    ) {
        CompletedPart.Builder completedPartBuilder = CompletedPart.builder().eTag(partResponse.eTag()).partNumber(partNumber);
        if (isRemoteDataIntegrityCheckEnabled) {
            completedPartBuilder.checksumCRC32(partResponse.checksumCRC32());
            InputStreamCRC32Container inputStreamCRC32Container = inputStreamContainers.get(partNumber - 1);
            inputStreamCRC32Container.setChecksum(partResponse.checksumCRC32());
            inputStreamContainers.set(partNumber - 1, inputStreamCRC32Container);
        }
        CompletedPart completedPart = completedPartBuilder.build();
        completedParts.set(partNumber - 1, completedPart);
        return completedPart;
    }

    /**
     * Calculates the optimal part size of each part request if the upload operation is carried out as multipart upload.
     */
    public long calculateOptimalPartSize(long contentLengthOfSource) {
        if (contentLengthOfSource < ByteSizeUnit.MB.toBytes(5)) {
            return contentLengthOfSource;
        }
        double optimalPartSize = contentLengthOfSource / (double) MAX_UPLOAD_PARTS;
        optimalPartSize = Math.ceil(optimalPartSize);
        return (long) Math.max(optimalPartSize, minimumPartSize);
    }

    private void uploadInOneChunk(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        InputStreamContainer inputStreamContainer,
        CompletableFuture<Void> returnFuture
    ) {
        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .contentLength(uploadRequest.getContentLength());
        if (uploadRequest.doRemoteDataIntegrityCheck()) {
            putObjectRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
            putObjectRequestBuilder.checksumCRC32(base64StringFromLong(uploadRequest.getExpectedChecksum()));
        }
        ExecutorService streamReadExecutor = uploadRequest.getWritePriority() == WritePriority.HIGH
            ? priorityExecutorService
            : executorService;
        CompletableFuture<Void> putObjectFuture = SocketAccess.doPrivileged(
            () -> s3AsyncClient.putObject(
                putObjectRequestBuilder.build(),
                AsyncRequestBody.fromInputStream(
                    inputStreamContainer.getInputStream(),
                    inputStreamContainer.getContentLength(),
                    streamReadExecutor
                )
            ).handle((resp, throwable) -> {
                if (throwable != null) {
                    Throwable unwrappedThrowable = ExceptionsHelper.unwrap(throwable, S3Exception.class);
                    if (unwrappedThrowable != null) {
                        S3Exception s3Exception = (S3Exception) unwrappedThrowable;
                        if (s3Exception.statusCode() == HttpStatusCode.BAD_REQUEST
                            && "BadDigest".equals(s3Exception.awsErrorDetails().errorCode())) {
                            throw new RuntimeException(new CorruptFileException(s3Exception, uploadRequest.getKey()));
                        }
                    }
                    returnFuture.completeExceptionally(throwable);
                } else {
                    uploadRequest.getUploadFinalizer().accept(true);
                    returnFuture.complete(null);
                }

                return null;
            }).handle((resp, throwable) -> {
                if (throwable != null) {
                    deleteUploadedObject(s3AsyncClient, uploadRequest);
                    returnFuture.completeExceptionally(throwable);
                }

                return null;
            })
        );

        CompletableFutureUtils.forwardExceptionTo(returnFuture, putObjectFuture);
        CompletableFutureUtils.forwardResultTo(putObjectFuture, returnFuture);
    }

    private void deleteUploadedObject(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .build();

        SocketAccess.doPrivileged(() -> s3AsyncClient.deleteObject(deleteObjectRequest)).exceptionally(throwable -> {
            log.warn("Failed to delete uploaded object", throwable);
            return null;
        });
    }
}
