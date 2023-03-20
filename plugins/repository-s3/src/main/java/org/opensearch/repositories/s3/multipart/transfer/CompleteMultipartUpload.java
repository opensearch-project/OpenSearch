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

import com.amazonaws.SdkClientException;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListenerChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.repositories.s3.SocketAccess;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.amazonaws.event.SDKProgressPublisher.publishProgress;

/**
 * Initiates a complete multi-part upload request for a
 * TransferManager multi-part parallel upload.
 */
@SuppressForbidden(reason = "AWS S3 SDK")
public class CompleteMultipartUpload implements Callable<UploadResult> {

    /** The upload id associated with the multi-part upload. */
    private final String uploadId;

    /**
     * The reference to underlying Amazon S3 client to be used for initiating
     * requests to Amazon S3.
     */
    private final AmazonS3 s3;

    /** The reference to the request initiated by the user. */
    private final PutObjectRequest origReq;

    /** The futures of threads that upload individual parts. */
    private final List<Future<PartETag>> futures;

    /**
     * The eTags of the parts that had been successfully uploaded before
     * resuming a paused upload.
     */
    private final List<PartETag> eTagsBeforeResume;

    /** The monitor to which the upload progress has to be communicated. */
    private final UploadMonitor monitor;

    /** The listener where progress of the upload needs to be published. */
    private final ProgressListenerChain listener;

    public CompleteMultipartUpload(
        String uploadId,
        AmazonS3 s3,
        PutObjectRequest putObjectRequest,
        List<Future<PartETag>> futures,
        List<PartETag> eTagsBeforeResume,
        ProgressListenerChain progressListenerChain,
        UploadMonitor monitor
    ) {
        this.uploadId = uploadId;
        this.s3 = s3;
        this.origReq = putObjectRequest;
        this.futures = futures;
        this.eTagsBeforeResume = eTagsBeforeResume;
        this.listener = progressListenerChain;
        this.monitor = monitor;
    }

    @Override
    public UploadResult call() throws Exception {
        CompleteMultipartUploadResult res;

        try {
            CompleteMultipartUploadRequest req = new CompleteMultipartUploadRequest(
                origReq.getBucketName(),
                origReq.getKey(),
                uploadId,
                collectPartETags()
            ).withRequesterPays(origReq.isRequesterPays())
                .withGeneralProgressListener(origReq.getGeneralProgressListener())
                .withRequestMetricCollector(origReq.getRequestMetricCollector())
                .withRequestCredentialsProvider(origReq.getRequestCredentialsProvider());
            res = SocketAccess.doPrivilegedIOException(() -> s3.completeMultipartUpload(req));
        } catch (Exception e) {
            monitor.uploadFailure();
            publishProgress(listener, ProgressEventType.TRANSFER_FAILED_EVENT);
            for (Future<PartETag> future : futures) {
                future.cancel(false);
            }
            throw e;
        }

        UploadResult uploadResult = new UploadResult();
        uploadResult.setBucketName(origReq.getBucketName());
        uploadResult.setKey(origReq.getKey());
        uploadResult.setETag(res.getETag());
        uploadResult.setVersionId(res.getVersionId());

        monitor.uploadComplete();

        return uploadResult;
    }

    /**
     * Collects the Part ETags for initiating the complete multi-part upload
     * request. This is blocking as it waits until all the upload part threads
     * complete.
     */
    private List<PartETag> collectPartETags() {

        final List<PartETag> partETags = new ArrayList<PartETag>();
        partETags.addAll(eTagsBeforeResume);
        for (Future<PartETag> future : futures) {
            try {
                partETags.add(future.get());
            } catch (Exception e) {
                throw new SdkClientException(
                    "Unable to complete multi-part upload. Individual part upload failed : " + e.getCause().getMessage(),
                    e.getCause()
                );

            }
        }
        return partETags;
    }
}
