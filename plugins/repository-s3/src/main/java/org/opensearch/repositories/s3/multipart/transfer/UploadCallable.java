/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.multipart.transfer;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListenerChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Encryption;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.EncryptedInitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.EncryptedPutObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.transfer.PersistableUpload;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.internal.S3ProgressPublisher;
import com.amazonaws.services.s3.transfer.internal.UploadPartCallable;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opensearch.common.Stream;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.repositories.s3.ExecutorContainer;
import org.opensearch.repositories.s3.SocketAccess;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;

import static com.amazonaws.event.SDKProgressPublisher.publishProgress;

public class UploadCallable implements Callable<UploadResult> {
    private static final Log log = LogFactory.getLog(UploadCallable.class);
    private final AmazonS3 s3;
    private final ExecutorContainer executorContainer;
    private final String threadPoolName;
    private final PutObjectRequest origReq;
    private final long totalContentLength;
    private final Stream[] inputStreams;
    private final UploadImpl upload;
    private final TransferManagerConfiguration configuration;
    private final List<Future<PartETag>> futures = new ArrayList<Future<PartETag>>();
    private final ProgressListenerChain listener;
    private final TransferProgress transferProgress;
    /**
     * ETags retrieved from Amazon S3 for a multi-part upload id. These parts
     * will be skipped while resuming a paused upload.
     */
    private final List<PartETag> eTagsToSkip = new ArrayList<PartETag>();
    private String uploadId;
    private PersistableUpload persistableUpload;

    public UploadCallable(TransferManager transferManager,
                          ExecutorContainer executorContainer, UploadImpl upload,
                          PutObjectRequest origReq,
                          ProgressListenerChain progressListenerChain, String uploadId,
                          TransferProgress transferProgress,
                          Stream[] streams,
                          long totalContentLength,
                          String threadPoolName) {
        this.s3 = transferManager.getAmazonS3Client();
        this.configuration = transferManager.getConfiguration();
        this.executorContainer = executorContainer;
        this.origReq = origReq;
        this.listener = progressListenerChain;
        this.upload = upload;
        this.totalContentLength = totalContentLength;
        this.inputStreams = streams;
        this.transferProgress = transferProgress;
        this.uploadId = uploadId;
        this.threadPoolName = threadPoolName;
    }

    List<Future<PartETag>> getFutures() {
        return futures;
    }

    /**
     * Returns the ETags retrieved from Amazon S3 for a multi-part upload id.
     * These parts will be skipped while resuming a paused upload.
     */
    List<PartETag> getETags() {
        return eTagsToSkip;
    }

    String getUploadId() {
        return uploadId;
    }

    /**
     * Returns true if this UploadCallable is processing a multipart upload.
     *
     * @return True if this UploadCallable is processing a multipart upload.
     */
    public boolean isMultipartUpload() {
        return TransferManagerUtils.shouldUseMultipartUpload(totalContentLength, configuration, inputStreams[0].getContentLength());
    }

    public UploadResult call() {
        upload.setState(TransferState.InProgress);
        if (isMultipartUpload()) {
            publishProgress(listener, ProgressEventType.TRANSFER_STARTED_EVENT);
            return uploadInParts();
        } else {
            return uploadInOneChunk();
        }
    }

    /**
     * Uploads the given request in a single chunk and returns the result.
     */
    private UploadResult uploadInOneChunk() {
        assert inputStreams.length == 1 : "Single chunk upload request has multiple InputStream objects";

        origReq.setInputStream(inputStreams[0].getInputStream());
        PutObjectResult putObjectResult = SocketAccess.doPrivileged(() -> s3.putObject(origReq));

        UploadResult uploadResult = new UploadResult();
        uploadResult.setBucketName(origReq.getBucketName());
        uploadResult.setKey(origReq.getKey());
        uploadResult.setETag(putObjectResult.getETag());
        uploadResult.setVersionId(putObjectResult.getVersionId());
        return uploadResult;
    }

    /**
     * Captures the state of the upload.
     */
    @SuppressForbidden(reason = "Future#cancel()")
    private void captureUploadStateIfPossible() {
        if (origReq.getSSECustomerKey() == null && !TransferManagerUtils.isMultiStreamUpload(inputStreams)) {
            persistableUpload = new PersistableUpload(origReq.getBucketName(),
                origReq.getKey(), origReq.getFile()
                .getAbsolutePath(), uploadId,
                configuration.getMinimumUploadPartSize(),
                configuration.getMultipartUploadThreshold());
            notifyPersistableTransferAvailability();
        }
    }

    public PersistableUpload getPersistableUpload() {
        return persistableUpload;
    }

    /**
     * Notifies to the callbacks that state is available
     */
    private void notifyPersistableTransferAvailability() {
        S3ProgressPublisher.publishTransferPersistable(
            listener, persistableUpload);
    }

    /**
     * Uploads the request in multiple chunks, submitting each upload chunk task
     * to the thread pool and recording its corresponding Future object, as well
     * as the multipart upload id.
     */
    private UploadResult uploadInParts() {
        boolean isUsingEncryption = s3 instanceof AmazonS3Encryption;

        try {
            if (uploadId == null) {
                uploadId = initiateMultipartUpload(origReq, isUsingEncryption);
            }

            UploadPartRequestFactory requestFactory = new UploadPartRequestFactory(origReq, uploadId,
                inputStreams, totalContentLength);

            if (TransferManagerUtils.isUploadParallelizable(origReq, inputStreams)) {
                captureUploadStateIfPossible();
                uploadPartsInParallel(requestFactory, uploadId);
                return null;
            } else {
                return uploadPartsInSeries(requestFactory);
            }
        } catch (Exception e) {
            publishProgress(listener, ProgressEventType.TRANSFER_FAILED_EVENT);
            performAbortMultipartUpload();
            throw e;
        } finally {
            if (origReq.getInputStream() != null) {
                try {
                    origReq.getInputStream().close();
                } catch (Exception e) {
                    log.warn("Unable to cleanly close input stream: " + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Performs an
     * {@link AmazonS3#abortMultipartUpload(AbortMultipartUploadRequest)}
     * operation for the given multi-part upload.
     */
    void performAbortMultipartUpload() {
        if (uploadId == null) {
            return;
        }
        try {
            AbortMultipartUploadRequest abortRequest = addCredentialsToRequest(new AbortMultipartUploadRequest(origReq.getBucketName(), origReq.getKey(),
                uploadId)
                .withRequesterPays(origReq.isRequesterPays()));
            s3.abortMultipartUpload(abortRequest);
        } catch (Exception e2) {
            log.info(
                "Unable to abort multipart upload, you may need to manually remove uploaded parts: "
                    + e2.getMessage(), e2);
        }
    }

    /**
     * Uploads all parts in the request in serial in this thread, then completes
     * the upload and returns the result.
     */
    private UploadResult uploadPartsInSeries(UploadPartRequestFactory requestFactory) {

        final List<PartETag> partETags = new ArrayList<PartETag>();

        while (requestFactory.hasMoreRequests()) {
            if (executorContainer.getExecutorShutdownSignal().apply(threadPoolName)) {
                throw new CancellationException("TransferManager has been shutdown");
            }
            UploadPartRequest uploadPartRequest = requestFactory.getNextUploadPartRequest();
            // Mark the stream in case we need to reset it
            InputStream inputStream = uploadPartRequest.getInputStream();
            if (inputStream != null && inputStream.markSupported()) {
                if (uploadPartRequest.getPartSize() >= Integer.MAX_VALUE) {
                    inputStream.mark(Integer.MAX_VALUE);
                } else {
                    inputStream.mark((int) uploadPartRequest.getPartSize());
                }
            }
            partETags.add(s3.uploadPart(uploadPartRequest).getPartETag());
        }

        CompleteMultipartUploadRequest req =
            addCredentialsToRequest(new CompleteMultipartUploadRequest(
                origReq.getBucketName(), origReq.getKey(), uploadId,
                partETags)
                .withRequesterPays(origReq.isRequesterPays())
                .withGeneralProgressListener(origReq.getGeneralProgressListener())
                .withRequestMetricCollector(origReq.getRequestMetricCollector()));
        CompleteMultipartUploadResult res = SocketAccess.doPrivileged(() -> s3.completeMultipartUpload(req));

        UploadResult uploadResult = new UploadResult();
        uploadResult.setBucketName(res.getBucketName());
        uploadResult.setKey(res.getKey());
        uploadResult.setETag(res.getETag());
        uploadResult.setVersionId(res.getVersionId());
        return uploadResult;
    }

    /**
     * Submits a callable for each part to upload to our thread pool and records its corresponding Future.
     */
    private void uploadPartsInParallel(UploadPartRequestFactory requestFactory,
                                       String uploadId) {

        Map<Integer, PartSummary> partNumbers = identifyExistingPartsForResume(uploadId);

        while (requestFactory.hasMoreRequests()) {
            if (executorContainer.getExecutorShutdownSignal().apply(threadPoolName)) {
                throw new CancellationException("TransferManager has been shutdown");
            }
            UploadPartRequest request = requestFactory.getNextUploadPartRequest();
            if (partNumbers.containsKey(request.getPartNumber())) {
                PartSummary summary = partNumbers.get(request.getPartNumber());
                eTagsToSkip.add(new PartETag(request.getPartNumber(), summary
                    .getETag()));
                transferProgress.updateProgress(summary.getSize());
                continue;
            }
            UploadPartCallable uploadPartCallable = new UploadPartCallable(s3, request, shouldCalculatePartMd5());
            futures.add((Future<PartETag>) executorContainer.getThreadExecutor().apply(
                new PrivilegedCallable<>(uploadPartCallable),
                threadPoolName
            ));
        }
    }

    private Map<Integer, PartSummary> identifyExistingPartsForResume(
        String uploadId) {
        Map<Integer, PartSummary> partNumbers = new HashMap<Integer, PartSummary>();
        if (uploadId == null) {
            return partNumbers;
        }
        int partNumber = 0;

        while (true) {
            PartListing parts = s3.listParts(addCredentialsToRequest(new ListPartsRequest(
                origReq.getBucketName(),
                origReq.getKey(), uploadId)
                .withPartNumberMarker(partNumber)
                .withRequesterPays(origReq.isRequesterPays())));
            for (PartSummary partSummary : parts.getParts()) {
                partNumbers.put(partSummary.getPartNumber(), partSummary);
            }
            if (!parts.isTruncated()) {
                return partNumbers;
            }
            partNumber = parts.getNextPartNumberMarker();
        }
    }

    /**
     * Initiates a multipart upload and returns the upload id
     */
    private String initiateMultipartUpload(PutObjectRequest origReq, boolean isUsingEncryption) {

        InitiateMultipartUploadRequest req = null;
        if (isUsingEncryption && origReq instanceof EncryptedPutObjectRequest) {
            req = addCredentialsToRequest(new EncryptedInitiateMultipartUploadRequest(
                origReq.getBucketName(), origReq.getKey()).withCannedACL(
                origReq.getCannedAcl()).withObjectMetadata(origReq.getMetadata()));
            ((EncryptedInitiateMultipartUploadRequest) req)
                .setMaterialsDescription(((EncryptedPutObjectRequest) origReq).getMaterialsDescription());
        } else {
            req = addCredentialsToRequest(new InitiateMultipartUploadRequest(origReq.getBucketName(), origReq.getKey())
                .withCannedACL(origReq.getCannedAcl())
                .withObjectMetadata(origReq.getMetadata()));
        }

        req.withTagging(origReq.getTagging());

        TransferManager.appendMultipartUserAgent(req);

        req.withAccessControlList(origReq.getAccessControlList())
            .withRequesterPays(origReq.isRequesterPays())
            .withStorageClass(origReq.getStorageClass())
            .withRedirectLocation(origReq.getRedirectLocation())
            .withSSECustomerKey(origReq.getSSECustomerKey())
            .withSSEAwsKeyManagementParams(origReq.getSSEAwsKeyManagementParams())
            .withGeneralProgressListener(origReq.getGeneralProgressListener())
            .withRequestMetricCollector(origReq.getRequestMetricCollector())
        ;

        req.withObjectLockMode(origReq.getObjectLockMode())
            .withObjectLockRetainUntilDate(origReq.getObjectLockRetainUntilDate())
            .withObjectLockLegalHoldStatus(origReq.getObjectLockLegalHoldStatus());

        String uploadId = s3.initiateMultipartUpload(req).getUploadId();
        log.debug("Initiated new multipart upload: " + uploadId);

        return uploadId;
    }

    private boolean shouldCalculatePartMd5() {
        return origReq.getObjectLockMode() != null
            || origReq.getObjectLockRetainUntilDate() != null
            || origReq.getObjectLockLegalHoldStatus() != null;
    }

    private <T extends AmazonWebServiceRequest> T addCredentialsToRequest(T req) {
        return req.withRequestCredentialsProvider(origReq.getRequestCredentialsProvider());
    }
}
