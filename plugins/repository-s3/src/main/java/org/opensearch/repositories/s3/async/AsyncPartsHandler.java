/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.repositories.s3.SocketAccess;
import org.opensearch.repositories.s3.StatsMetricPublisher;
import org.opensearch.repositories.s3.io.CheckedContainer;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Responsible for handling parts of the original multipart request
 */
public class AsyncPartsHandler {

    private static Logger log = LogManager.getLogger(AsyncPartsHandler.class);

    /**
     * Uploads parts of the upload multipart request*
     * @param s3AsyncClient S3 client to use for upload
     * @param executorService Thread pool for regular upload
     * @param priorityExecutorService Thread pool for priority uploads
     * @param urgentExecutorService Thread pool for urgent uploads
     * @param uploadRequest request for upload
     * @param streamContext Stream context used in supplying individual file parts
     * @param uploadId Upload Id against which multi-part is being performed
     * @param completedParts Reference of completed parts
     * @param inputStreamContainers Checksum containers
     * @param statsMetricPublisher sdk metric publisher
     * @return list of completable futures
     * @throws IOException thrown in case of an IO error
     */
    public static List<CompletableFuture<CompletedPart>> uploadParts(
        S3AsyncClient s3AsyncClient,
        ExecutorService executorService,
        ExecutorService priorityExecutorService,
        ExecutorService urgentExecutorService,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        String uploadId,
        AtomicReferenceArray<CompletedPart> completedParts,
        AtomicReferenceArray<CheckedContainer> inputStreamContainers,
        StatsMetricPublisher statsMetricPublisher,
        boolean uploadRetryEnabled
    ) throws IOException {
        List<CompletableFuture<CompletedPart>> futures = new ArrayList<>();
        for (int partIdx = 0; partIdx < streamContext.getNumberOfParts(); partIdx++) {
            InputStreamContainer inputStreamContainer = streamContext.provideStream(partIdx);
            inputStreamContainers.set(partIdx, new CheckedContainer(inputStreamContainer.getContentLength()));
            UploadPartRequest.Builder uploadPartRequestBuilder = UploadPartRequest.builder()
                .bucket(uploadRequest.getBucket())
                .partNumber(partIdx + 1)
                .key(uploadRequest.getKey())
                .uploadId(uploadId)
                .overrideConfiguration(o -> o.addMetricPublisher(statsMetricPublisher.multipartUploadMetricCollector))
                .contentLength(inputStreamContainer.getContentLength());
            if (uploadRequest.doRemoteDataIntegrityCheck()) {
                uploadPartRequestBuilder.checksumAlgorithm(ChecksumAlgorithm.CRC32);
            }
            uploadPart(
                s3AsyncClient,
                executorService,
                priorityExecutorService,
                urgentExecutorService,
                completedParts,
                inputStreamContainers,
                futures,
                uploadPartRequestBuilder.build(),
                inputStreamContainer,
                uploadRequest,
                uploadRetryEnabled
            );
        }

        return futures;
    }

    /**
     * Cleans up parts of the original multipart request*
     * @param s3AsyncClient s3 client to use
     * @param uploadRequest upload request
     * @param uploadId upload id against which multi-part was carried out.
     */
    public static void cleanUpParts(S3AsyncClient s3AsyncClient, UploadRequest uploadRequest, String uploadId) {

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

    public static InputStream maybeRetryInputStream(
        InputStream inputStream,
        WritePriority writePriority,
        boolean uploadRetryEnabled,
        long contentLength
    ) {
        if (uploadRetryEnabled == true && (writePriority == WritePriority.HIGH || writePriority == WritePriority.URGENT)) {
            return new BufferedInputStream(inputStream, (int) (contentLength + 1));
        }
        return inputStream;
    }

    private static void uploadPart(
        S3AsyncClient s3AsyncClient,
        ExecutorService executorService,
        ExecutorService priorityExecutorService,
        ExecutorService urgentExecutorService,
        AtomicReferenceArray<CompletedPart> completedParts,
        AtomicReferenceArray<CheckedContainer> inputStreamContainers,
        List<CompletableFuture<CompletedPart>> futures,
        UploadPartRequest uploadPartRequest,
        InputStreamContainer inputStreamContainer,
        UploadRequest uploadRequest,
        boolean uploadRetryEnabled
    ) {
        Integer partNumber = uploadPartRequest.partNumber();

        ExecutorService streamReadExecutor;
        if (uploadRequest.getWritePriority() == WritePriority.URGENT) {
            streamReadExecutor = urgentExecutorService;
        } else if (uploadRequest.getWritePriority() == WritePriority.HIGH) {
            streamReadExecutor = priorityExecutorService;
        } else {
            streamReadExecutor = executorService;
        }

        InputStream inputStream = maybeRetryInputStream(
            inputStreamContainer.getInputStream(),
            uploadRequest.getWritePriority(),
            uploadRetryEnabled,
            uploadPartRequest.contentLength()
        );
        CompletableFuture<UploadPartResponse> uploadPartResponseFuture = SocketAccess.doPrivileged(
            () -> s3AsyncClient.uploadPart(
                uploadPartRequest,
                AsyncRequestBody.fromInputStream(inputStream, inputStreamContainer.getContentLength(), streamReadExecutor)
            )
        );

        CompletableFuture<CompletedPart> convertFuture = uploadPartResponseFuture.whenComplete((resp, throwable) -> {
            try {
                inputStream.close();
            } catch (IOException ex) {
                log.error(
                    () -> new ParameterizedMessage(
                        "Failed to close stream while uploading a part of idx {} and file {}.",
                        uploadPartRequest.partNumber(),
                        uploadPartRequest.key()
                    ),
                    ex
                );
            }
        })
            .thenApply(
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

    private static CompletedPart convertUploadPartResponse(
        AtomicReferenceArray<CompletedPart> completedParts,
        AtomicReferenceArray<CheckedContainer> inputStreamContainers,
        UploadPartResponse partResponse,
        int partNumber,
        boolean isRemoteDataIntegrityCheckEnabled
    ) {
        CompletedPart.Builder completedPartBuilder = CompletedPart.builder().eTag(partResponse.eTag()).partNumber(partNumber);
        if (isRemoteDataIntegrityCheckEnabled) {
            completedPartBuilder.checksumCRC32(partResponse.checksumCRC32());
            CheckedContainer inputStreamCRC32Container = inputStreamContainers.get(partNumber - 1);
            inputStreamCRC32Container.setChecksum(partResponse.checksumCRC32());
            inputStreamContainers.set(partNumber - 1, inputStreamCRC32Container);
        }
        CompletedPart completedPart = completedPartBuilder.build();
        completedParts.set(partNumber - 1, completedPart);
        return completedPart;
    }
}
