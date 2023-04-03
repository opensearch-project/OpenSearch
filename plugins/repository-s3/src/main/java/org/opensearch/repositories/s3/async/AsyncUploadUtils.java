package org.opensearch.repositories.s3.async;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Stream;
import org.opensearch.common.blobstore.stream.StreamContext;
import org.opensearch.common.blobstore.stream.write.UploadResponse;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.repositories.s3.SocketAccess;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import java.util.ArrayList;
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
    private final boolean multipartUploadEnabled;

    /**
     * The max number of parts on S3 side is 10,000
     */
    private static final long MAX_UPLOAD_PARTS = 10_000;


    public AsyncUploadUtils(boolean multipartUploadEnabled, long minimumPartSize, ExecutorService executorService, ExecutorService  priorityExecutorService) {
        this.executorService = executorService;
        this.priorityExecutorService = priorityExecutorService;
        this.minimumPartSize = minimumPartSize;
        this.multipartUploadEnabled = multipartUploadEnabled;
    }

    public boolean isMultipartUploadEnabled() {
        return multipartUploadEnabled;
    }

    public  CompletableFuture<UploadResponse> uploadObject(S3AsyncClient s3AsyncClient,
                                                           UploadRequest uploadRequest,
                                                           StreamContext streamContext) {

        CompletableFuture<UploadResponse> returnFuture = new CompletableFuture<>();
        try {
            if (streamContext.getNumberOfParts() == 1) {
                log.debug(() -> "Starting the upload as a single upload part request");
                uploadInOneChunk(s3AsyncClient, uploadRequest, streamContext.getStreamProvider().provideStream(0),
                    returnFuture);
            } else {
                log.debug(() -> "Starting the upload as multipart upload request");
                uploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture);
            }
        } catch (Throwable throwable) {
            returnFuture.completeExceptionally(throwable);
        }

        return returnFuture;
    }

    private void uploadInParts(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest, StreamContext streamContext,
                               CompletableFuture<UploadResponse> returnFuture) {

        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .build();
        CompletableFuture<CreateMultipartUploadResponse> createMultipartUploadFuture =
            SocketAccess.doPrivileged(()-> s3AsyncClient.createMultipartUpload(request));

        // Ensure cancellations are forwarded to the createMultipartUploadFuture future
        CompletableFutureUtils.forwardExceptionTo(returnFuture, createMultipartUploadFuture);

        createMultipartUploadFuture.whenComplete((createMultipartUploadResponse, throwable) -> {
            if (throwable != null) {
                handleException(returnFuture, () -> "Failed to initiate multipart upload", throwable);
            } else {
                log.debug(() -> "Initiated new multipart upload, uploadId: " + createMultipartUploadResponse.uploadId());
                doUploadInParts(s3AsyncClient, uploadRequest, streamContext, returnFuture,
                    createMultipartUploadResponse.uploadId());
            }
        });
    }

    private void doUploadInParts(S3AsyncClient s3AsyncClient,
                                 UploadRequest uploadRequest,
                                 StreamContext streamContext,
                                 CompletableFuture<UploadResponse> returnFuture,
                                 String uploadId) {

        // The list of completed parts must be sorted
        AtomicReferenceArray<CompletedPart> completedParts = new AtomicReferenceArray<>(streamContext.getNumberOfParts());

        List<CompletableFuture<CompletedPart>> futures = sendUploadPartRequests(s3AsyncClient, uploadRequest,
            streamContext, uploadId, completedParts);

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(new CompletableFuture[0]))
            .thenApply(resp -> {
                uploadRequest.getUploadFinalizer().accept(true);
                return resp;
            })
            .thenCompose(ignore -> completeMultipartUpload(s3AsyncClient, uploadRequest, uploadId, completedParts))
            .handle(handleExceptionOrResponse(s3AsyncClient, uploadRequest, returnFuture, uploadId))
            .exceptionally(throwable -> {
                handleException(returnFuture, () -> "Unexpected exception occurred", throwable);
                return null;
            });
    }

    private BiFunction<CompleteMultipartUploadResponse, Throwable, Void> handleExceptionOrResponse(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        CompletableFuture<UploadResponse> returnFuture,
        String uploadId) {

        return (response, throwable) -> {
            if (throwable != null) {
                cleanUpParts(s3AsyncClient, uploadRequest, uploadId);
                handleException(returnFuture, () -> "Failed to send multipart upload requests.",
                    throwable);
            } else {
                returnFuture.complete(new UploadResponse(true));
            }

            return null;
        };
    }

    private CompletableFuture<CompleteMultipartUploadResponse> completeMultipartUpload(
        S3AsyncClient s3AsyncClient, UploadRequest uploadRequest, String uploadId,
        AtomicReferenceArray<CompletedPart> completedParts) {

        log.debug(() -> String.format("Sending completeMultipartUploadRequest, uploadId: %s",
            uploadId));
        CompletedPart[] parts =
            IntStream.range(0, completedParts.length())
                .mapToObj(completedParts::get)
                .toArray(CompletedPart[]::new);
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
            CompleteMultipartUploadRequest.builder()
                .bucket(uploadRequest.getBucket())
                .key(uploadRequest.getKey())
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                    .parts(parts)
                    .build())
                .build();

        return SocketAccess.doPrivileged(()-> s3AsyncClient.completeMultipartUpload(completeMultipartUploadRequest));
    }

    private void cleanUpParts(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest, String uploadId) {

        AbortMultipartUploadRequest abortMultipartUploadRequest = AbortMultipartUploadRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .uploadId(uploadId)
            .build();
        SocketAccess.doPrivileged(()->s3AsyncClient.abortMultipartUpload(abortMultipartUploadRequest)
            .exceptionally(throwable -> {
                log.warn(() -> String.format("Failed to abort previous multipart upload "
                        + "(id: %s)"
                        + ". You may need to call "
                        + "S3AsyncClient#abortMultiPartUpload to "
                        + "free all storage consumed by"
                        + " all parts. ",
                    uploadId), throwable);
                return null;
            }));
    }

    private static void handleException(CompletableFuture<UploadResponse> returnFuture,
                                        Supplier<String> message,
                                        Throwable throwable) {
        Throwable cause = throwable instanceof CompletionException ? throwable.getCause() : throwable;

        if (cause instanceof Error) {
            returnFuture.completeExceptionally(cause);
        } else {
            SdkClientException exception = SdkClientException.create(message.get(), cause);
            returnFuture.completeExceptionally(exception);
        }
    }

    private List<CompletableFuture<CompletedPart>> sendUploadPartRequests(S3AsyncClient s3AsyncClient,
                                                                          UploadRequest uploadRequest,
                                                                          StreamContext streamContext,
                                                                          String uploadId,
                                                                          AtomicReferenceArray<CompletedPart> completedParts) {
        List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();

        for (int partIdx = 0;partIdx < streamContext.getNumberOfParts(); partIdx++) {
            Stream stream = streamContext.getStreamProvider().provideStream(partIdx);
            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(uploadRequest.getBucket())
                .partNumber(partIdx+1)
                .key(uploadRequest.getKey())
                .uploadId(uploadId)
                .contentLength(stream.getContentLength())
                .build();
            sendIndividualUploadPart(s3AsyncClient, completedParts, futures, uploadPartRequest, stream, uploadRequest);
        }


        return futures;
    }

    private void sendIndividualUploadPart(S3AsyncClient s3AsyncClient,
                                          AtomicReferenceArray<CompletedPart> completedParts,
                                          List<CompletableFuture<CompletedPart>> futures,
                                          UploadPartRequest uploadPartRequest,
                                          Stream stream,  UploadRequest uploadRequest) {
        Integer partNumber = uploadPartRequest.partNumber();

        ExecutorService streamReadExecutor = uploadRequest.getWritePriority() == WritePriority.HIGH ?
            priorityExecutorService : executorService;
        CompletableFuture<UploadPartResponse> uploadPartResponseFuture = SocketAccess.doPrivileged(()->
            s3AsyncClient.uploadPart(uploadPartRequest, AsyncRequestBody.fromInputStream(stream.getInputStream(),
                stream.getContentLength(), streamReadExecutor)));

        CompletableFuture<CompletedPart> convertFuture =
            uploadPartResponseFuture
                .thenApply(uploadPartResponse ->
                    convertUploadPartResponse(completedParts, uploadPartResponse, partNumber));
        futures.add(convertFuture);

        CompletableFutureUtils.forwardExceptionTo(convertFuture, uploadPartResponseFuture);
    }

    private CompletedPart convertUploadPartResponse(AtomicReferenceArray<CompletedPart> completedParts,
                                                    UploadPartResponse partResponse, int partNumber) {
        CompletedPart completedPart = CompletedPart.builder()
            .eTag(partResponse.eTag())
            .partNumber(partNumber)
            .build();
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

    private void uploadInOneChunk(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest, Stream stream,
                                  CompletableFuture<UploadResponse> returnFuture) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(uploadRequest.getBucket())
            .key(uploadRequest.getKey())
            .contentLength(uploadRequest.getContentLength())
            .build();
        ExecutorService streamReadExecutor = uploadRequest.getWritePriority() == WritePriority.HIGH ?
            priorityExecutorService : executorService;
        CompletableFuture<UploadResponse> putObjectFuture = SocketAccess.doPrivileged(() ->
            s3AsyncClient.putObject(putObjectRequest, AsyncRequestBody.fromInputStream(stream.getInputStream(),
                    stream.getContentLength(), streamReadExecutor))
                .handle((resp, throwable) -> {
                    if (throwable != null) {
                        returnFuture.completeExceptionally(throwable);
                    } else {
                        uploadRequest.getUploadFinalizer().accept(true);
                        returnFuture.complete(new UploadResponse(true));
                    }

                    return null;
                }));


        CompletableFutureUtils.forwardExceptionTo(returnFuture, putObjectFuture);
        CompletableFutureUtils.forwardResultTo(putObjectFuture, returnFuture);
    }
}

