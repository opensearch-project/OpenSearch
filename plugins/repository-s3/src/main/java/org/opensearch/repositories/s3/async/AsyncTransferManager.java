/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.exception.CorruptFileException;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.util.ByteUtils;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.s3.SocketAccess;
import org.opensearch.repositories.s3.StatsMetricPublisher;
import org.opensearch.repositories.s3.io.CheckedContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.jcraft.jzlib.JZlib;

/**
 * A helper class that automatically uses multipart upload based on the size of the source object
 */
public final class AsyncTransferManager {
    private static final Logger log = LogManager.getLogger(AsyncTransferManager.class);
    private final ExecutorService executorService;
    private final ExecutorService priorityExecutorService;
    private final ExecutorService urgentExecutorService;
    private final long minimumPartSize;
    private final long maxRetryablePartSize;

    @SuppressWarnings("rawtypes")
    private final TransferSemaphoresHolder transferSemaphoresHolder;

    /**
     * The max number of parts on S3 side is 10,000
     */
    private static final long MAX_UPLOAD_PARTS = 10_000;

    /**
     * Construct a new object of AsyncTransferManager
     *
     * @param minimumPartSize         The minimum part size for parallel multipart uploads
     */
    @SuppressWarnings("rawtypes")
    public AsyncTransferManager(
        long minimumPartSize,
        ExecutorService executorService,
        ExecutorService priorityExecutorService,
        ExecutorService urgentExecutorService,
        TransferSemaphoresHolder transferSemaphoresHolder
    ) {
        this.executorService = executorService;
        this.priorityExecutorService = priorityExecutorService;
        this.minimumPartSize = minimumPartSize;
        // 10% buffer to allow additional metadata size in content such as encryption.
        this.maxRetryablePartSize = (long) (minimumPartSize + 0.1 * minimumPartSize);
        this.urgentExecutorService = urgentExecutorService;
        this.transferSemaphoresHolder = transferSemaphoresHolder;
    }

    /**
     * Upload an object to S3 using the async client
     *
     * @param s3AsyncClient S3 client to use for upload
     * @param uploadRequest The {@link UploadRequest} object encapsulating all relevant details for upload
     * @param streamContext The {@link StreamContext} to supply streams during upload
     * @return A {@link CompletableFuture} to listen for upload completion
     */
    public CompletableFuture<Void> uploadObject(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        StatsMetricPublisher statsMetricPublisher
    ) {

        CompletableFuture<Void> returnFuture = new CompletableFuture<>();
        try {
            if (streamContext.getNumberOfParts() == 1) {
                log.debug(() -> "Starting the upload as a single upload part request");
                TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
                Semaphore semaphore = AsyncPartsHandler.maybeAcquireSemaphore(
                    transferSemaphoresHolder,
                    requestContext,
                    uploadRequest.getWritePriority(),
                    uploadRequest.getKey()
                );
                try {
                    uploadInOneChunk(s3AsyncClient, uploadRequest, streamContext, returnFuture, statsMetricPublisher, semaphore);
                } catch (Exception ex) {
                    if (semaphore != null) {
                        semaphore.release();
                    }
                    throw ex;
                }
            } else {
                log.debug(() -> "Starting the upload as multipart upload request");
                uploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture, statsMetricPublisher);
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
        CompletableFuture<Void> returnFuture,
        StatsMetricPublisher statsMetricPublisher
    ) {

        CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .overrideConfiguration(o -> o.addMetricPublisher(statsMetricPublisher.multipartUploadMetricCollector));

        if (CollectionUtils.isNotEmpty(uploadRequest.getMetadata())) {
            createMultipartUploadRequestBuilder.metadata(uploadRequest.getMetadata());
        }
        if (uploadRequest.doRemoteDataIntegrityCheck()) {
            createMultipartUploadRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
        }
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadFuture = SocketAccess.doPrivileged(
            () -> s3AsyncClient.createMultipartUpload(createMultipartUploadRequestBuilder.build())
        );

        // Ensure cancellations are forwarded to the createMultipartUploadFuture future
        CompletableFutureUtils.forwardExceptionTo(returnFuture, createMultipartUploadFuture);

        String uploadId;
        try {
            // Block main thread here so that upload of parts doesn't get executed in future completion thread.
            // We should never execute latent operation like acquisition of permit in future completion pool.
            CreateMultipartUploadResponse createMultipartUploadResponse = createMultipartUploadFuture.get();
            uploadId = createMultipartUploadResponse.uploadId();
            log.debug(() -> "Initiated new multipart upload, uploadId: " + createMultipartUploadResponse.uploadId());
        } catch (Exception ex) {
            handleException(returnFuture, () -> "Failed to initiate multipart upload", ex);
            return;
        }

        doUploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture, uploadId, statsMetricPublisher);
    }

    private void doUploadInParts(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        CompletableFuture<Void> returnFuture,
        String uploadId,
        StatsMetricPublisher statsMetricPublisher
    ) {

        // The list of completed parts must be sorted
        AtomicReferenceArray<CompletedPart> completedParts = new AtomicReferenceArray<>(streamContext.getNumberOfParts());
        AtomicReferenceArray<CheckedContainer> inputStreamContainers = new AtomicReferenceArray<>(streamContext.getNumberOfParts());

        List<CompletableFuture<CompletedPart>> futures;
        try {
            futures = AsyncPartsHandler.uploadParts(
                s3AsyncClient,
                executorService,
                priorityExecutorService,
                urgentExecutorService,
                uploadRequest,
                streamContext,
                uploadId,
                completedParts,
                inputStreamContainers,
                statsMetricPublisher,
                uploadRequest.isUploadRetryEnabled(),
                transferSemaphoresHolder,
                maxRetryablePartSize
            );
        } catch (Exception ex) {
            try {
                AsyncPartsHandler.cleanUpParts(s3AsyncClient, uploadRequest, uploadId);
            } finally {
                returnFuture.completeExceptionally(ex);
            }
            return;
        }

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).thenApply(resp -> {
            try {
                uploadRequest.getUploadFinalizer().accept(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return resp;
        }).thenApply(ignore -> {
            if (uploadRequest.doRemoteDataIntegrityCheck()) {
                mergeAndVerifyChecksum(inputStreamContainers, uploadRequest.getKey(), uploadRequest.getExpectedChecksum());
            }
            return null;
        })
            .thenCompose(ignore -> completeMultipartUpload(s3AsyncClient, uploadRequest, uploadId, completedParts, statsMetricPublisher))
            .handle(handleExceptionOrResponse(s3AsyncClient, uploadRequest, returnFuture, uploadId))
            .exceptionally(throwable -> {
                handleException(returnFuture, () -> "Unexpected exception occurred", throwable);
                return null;
            });
    }

    private void mergeAndVerifyChecksum(
        AtomicReferenceArray<CheckedContainer> inputStreamContainers,
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
                AsyncPartsHandler.cleanUpParts(s3AsyncClient, uploadRequest, uploadId);
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
        AtomicReferenceArray<CompletedPart> completedParts,
        StatsMetricPublisher statsMetricPublisher
    ) {

        log.debug(() -> new ParameterizedMessage("Sending completeMultipartUploadRequest, uploadId: {}", uploadId));
        CompletedPart[] parts = IntStream.range(0, completedParts.length()).mapToObj(completedParts::get).toArray(CompletedPart[]::new);
        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .uploadId(uploadId)
            .overrideConfiguration(o -> o.addMetricPublisher(statsMetricPublisher.multipartUploadMetricCollector))
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

    private static void handleException(CompletableFuture<Void> returnFuture, Supplier<String> message, Throwable throwable) {
        Throwable cause = throwable instanceof CompletionException ? throwable.getCause() : throwable;

        if (cause instanceof Error) {
            returnFuture.completeExceptionally(cause);
        } else {
            SdkClientException exception = SdkClientException.create(message.get(), cause);
            returnFuture.completeExceptionally(exception);
        }
    }

    /**
     * Calculates the optimal part size of each part request if the upload operation is carried out as multipart upload.
     */
    public long calculateOptimalPartSize(long contentLengthOfSource, WritePriority writePriority, boolean uploadRetryEnabled) {
        if (contentLengthOfSource < ByteSizeUnit.MB.toBytes(5)) {
            return contentLengthOfSource;
        }
        if (uploadRetryEnabled && (writePriority == WritePriority.HIGH || writePriority == WritePriority.URGENT)) {
            return new ByteSizeValue(5, ByteSizeUnit.MB).getBytes();
        }
        double optimalPartSize = contentLengthOfSource / (double) MAX_UPLOAD_PARTS;
        optimalPartSize = Math.ceil(optimalPartSize);
        return (long) Math.max(optimalPartSize, minimumPartSize);
    }

    @SuppressWarnings("unchecked")
    private void uploadInOneChunk(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        CompletableFuture<Void> returnFuture,
        StatsMetricPublisher statsMetricPublisher,
        Semaphore semaphore
    ) {
        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .contentLength(uploadRequest.getContentLength())
            .overrideConfiguration(o -> o.addMetricPublisher(statsMetricPublisher.putObjectMetricPublisher));

        if (CollectionUtils.isNotEmpty(uploadRequest.getMetadata())) {
            putObjectRequestBuilder.metadata(uploadRequest.getMetadata());
        }
        if (uploadRequest.doRemoteDataIntegrityCheck()) {
            putObjectRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
            putObjectRequestBuilder.checksumCRC32(base64StringFromLong(uploadRequest.getExpectedChecksum()));
        }
        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        ExecutorService streamReadExecutor;
        if (uploadRequest.getWritePriority() == WritePriority.URGENT) {
            streamReadExecutor = urgentExecutorService;
        } else if (uploadRequest.getWritePriority() == WritePriority.HIGH) {
            streamReadExecutor = priorityExecutorService;
        } else {
            streamReadExecutor = executorService;
        }

        CompletableFuture<Void> putObjectFuture = SocketAccess.doPrivileged(() -> {
            InputStream inputStream = null;
            CompletableFuture<PutObjectResponse> putObjectRespFuture;
            try {
                InputStreamContainer inputStreamContainer = streamContext.provideStream(0);
                inputStream = AsyncPartsHandler.maybeRetryInputStream(
                    inputStreamContainer.getInputStream(),
                    uploadRequest.getWritePriority(),
                    uploadRequest.isUploadRetryEnabled(),
                    uploadRequest.getContentLength(),
                    maxRetryablePartSize
                );
                AsyncRequestBody asyncRequestBody = AsyncRequestBody.fromInputStream(
                    inputStream,
                    inputStreamContainer.getContentLength(),
                    streamReadExecutor
                );
                putObjectRespFuture = s3AsyncClient.putObject(putObjectRequest, asyncRequestBody);
            } catch (Exception e) {
                releaseResourcesSafely(semaphore, inputStream, uploadRequest.getKey());
                return CompletableFuture.failedFuture(e);
            }

            InputStream finalInputStream = inputStream;
            return putObjectRespFuture.handle((resp, throwable) -> {
                releaseResourcesSafely(semaphore, finalInputStream, uploadRequest.getKey());

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
                    try {
                        uploadRequest.getUploadFinalizer().accept(true);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    returnFuture.complete(null);
                }

                return null;
            }).handle((resp, throwable) -> {
                if (throwable != null) {
                    deleteUploadedObject(s3AsyncClient, uploadRequest);
                    returnFuture.completeExceptionally(throwable);
                }

                return null;
            });
        });

        CompletableFutureUtils.forwardExceptionTo(returnFuture, putObjectFuture);
        CompletableFutureUtils.forwardResultTo(putObjectFuture, returnFuture);
    }

    private void releaseResourcesSafely(Semaphore semaphore, InputStream inputStream, String file) {
        if (semaphore != null) {
            semaphore.release();
        }

        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error(() -> new ParameterizedMessage("Failed to close stream while uploading single file {}.", file), e);
            }
        }
    }

    private void deleteUploadedObject(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest) {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .build();

        SocketAccess.doPrivileged(() -> s3AsyncClient.deleteObject(deleteObjectRequest)).exceptionally(throwable -> {
            log.error(() -> new ParameterizedMessage("Failed to delete uploaded object of key {}", uploadRequest.getKey()), throwable);
            return null;
        });
    }
}
