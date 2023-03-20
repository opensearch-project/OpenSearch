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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.opensearch.common.Stream;
import org.opensearch.common.SuppressForbidden;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.services.s3.Headers.SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY;

/**
 * Factory for creating all the individual UploadPartRequest objects for a
 * multipart upload.
 * <p>
 * This allows us to delay creating each UploadPartRequest until we're ready for
 * it, instead of immediately creating thousands of UploadPartRequest objects
 * for each large upload, when we won't need most of those request objects for a
 * while.
 */
@SuppressForbidden(reason = "AWS S3 SDK")
public class UploadPartRequestFactory {
    private final String bucketName;
    private final String key;
    private final String uploadId;
    private final File file;
    private final PutObjectRequest origReq;
    private volatile long offset = 0;
    private final SSECustomerKey sseCustomerKey;
    private final long multiStreamsContentLength;
    private final AtomicInteger multiStreamUploadIndex = new AtomicInteger();

    /**
     * Wrapped to provide necessary mark-and-reset support for the underlying
     * input stream. In particular, it provides support for unlimited
     * mark-and-reset if the underlying stream is a {@link FileInputStream}.
     */
    private final Stream[] inputStreams;

    // Note: Do not copy object metadata from PutObjectRequest to the UploadPartRequest
    // as headers "like x-amz-server-side-encryption" are valid in PutObject but not in UploadPart API
    public UploadPartRequestFactory(PutObjectRequest origReq, String uploadId, Stream[] inputStreams, long multiStreamsContentLength) {
        this.origReq = origReq;
        this.uploadId = uploadId;
        this.bucketName = origReq.getBucketName();
        this.key = origReq.getKey();
        this.file = TransferManagerUtils.getRequestFile(origReq);
        this.sseCustomerKey = origReq.getSSECustomerKey();
        this.multiStreamsContentLength = multiStreamsContentLength;
        this.inputStreams = inputStreams;
    }

    public synchronized boolean hasMoreRequests() {
        return multiStreamUploadIndex.get() < inputStreams.length;
    }

    public Stream getStreamContainer(int partNumber) {
        return inputStreams[partNumber - 1];
    }

    public synchronized UploadPartRequest getNextUploadPartRequest() {
        long partSize = inputStreams[multiStreamUploadIndex.get()].getContentLength();
        int partNumber = multiStreamUploadIndex.get() + 1;

        UploadPartRequest req = addCredentialsToRequest(
            new UploadPartRequest().withBucketName(bucketName)
                .withKey(key)
                .withUploadId(uploadId)
                .withInputStream(getStreamContainer(partNumber).getInputStream())
                .withPartNumber(partNumber)
                .withPartSize(partSize)
        );

        multiStreamUploadIndex.incrementAndGet();

        // Copying the SSE-C metadata if it's using SSE-C, which is required for each UploadPart Request.
        // See https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadUploadPart.html, https://github.com/aws/aws-sdk-java/issues/1840
        ObjectMetadata origReqMetadata = origReq.getMetadata();
        if (origReqMetadata != null
            && origReqMetadata.getRawMetadataValue(SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY) != null
            && origReqMetadata.getSSECustomerAlgorithm() != null
            && origReqMetadata.getSSECustomerKeyMd5() != null) {

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader(
                SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
                origReqMetadata.getRawMetadataValue(SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY)
            );
            metadata.setSSECustomerAlgorithm(origReqMetadata.getSSECustomerAlgorithm());
            metadata.setSSECustomerKeyMd5(origReqMetadata.getSSECustomerKeyMd5());

            req.withObjectMetadata(metadata);
        }

        req.withRequesterPays(origReq.isRequesterPays());
        TransferManager.appendMultipartUserAgent(req);

        if (sseCustomerKey != null) req.setSSECustomerKey(sseCustomerKey);

        offset += partSize;

        req.setLastPart(TransferManagerUtils.isLastPart(multiStreamUploadIndex.get(), inputStreams));

        req.withGeneralProgressListener(origReq.getGeneralProgressListener())
            .withRequestMetricCollector(origReq.getRequestMetricCollector());
        req.getRequestClientOptions().setReadLimit(origReq.getReadLimit());
        return req;
    }

    private <T extends AmazonWebServiceRequest> T addCredentialsToRequest(T req) {
        return req.withRequestCredentialsProvider(origReq.getRequestCredentialsProvider());
    }
}
