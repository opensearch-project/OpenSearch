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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListenerChain;
import com.amazonaws.services.s3.transfer.internal.TransferProgressUpdatingListener;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import org.opensearch.common.Stream;
import org.opensearch.common.blobstore.stream.StreamContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.repositories.s3.ExecutorContainer;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Responsible for breaking down large files into multiple parts where each part is assigned to a separate
 * InputStream and uploading all parts in parallel. A file is defined as large if it's size is greater than
 * {@link TransferManagerConfiguration#getMinimumUploadPartSize()}.
 * If a file doesn't meet this condition then {@link MultipartTransferManager} will upload it in a single chunk.
 */
public class MultipartTransferManager extends TransferManager implements Closeable {

    private final AmazonS3 s3;
    private final ExecutorContainer executorContainer;

    public MultipartTransferManager(TransferManagerBuilder builder, ExecutorContainer executorContainer) {
        super(builder);
        this.executorContainer = executorContainer;
        this.s3 = builder.getS3Client();
    }

    public Upload upload(final String bucketName, final String blobName, final TransferStateChangeListener stateListener,
                         final S3ProgressListener progressListener, StreamContext streamContext, ObjectTagging tagging,
                         final WritePriority writePriority)
        throws AmazonClientException, IOException {

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(streamContext.getTotalContentLength());
        objectMetadata.setContentType(Mimetypes.getInstance().getMimetype(blobName));
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, blobName, null, objectMetadata)
            .withCannedAcl(CannedAccessControlList.BucketOwnerFullControl);

        if (putObjectRequest.getMetadata() == null) {
            putObjectRequest.setMetadata(new ObjectMetadata());
        }

        putObjectRequest.setTagging(tagging);

        String description = "Uploading to " + putObjectRequest.getBucketName()
            + "/" + putObjectRequest.getKey();
        TransferProgress transferProgress = new TransferProgress();
        transferProgress.setTotalBytesToTransfer(streamContext.getTotalContentLength());

        S3ProgressListenerChain listenerChain = new S3ProgressListenerChain(
            new TransferProgressUpdatingListener(transferProgress),
            putObjectRequest.getGeneralProgressListener(), progressListener);

        putObjectRequest.setGeneralProgressListener(listenerChain);

        UploadImpl upload = new UploadImpl(description, transferProgress, listenerChain, stateListener);
        /*
         * Since we use the same thread pool for uploading individual parts and
         * complete multi part upload, there is a possibility that the tasks for
         * complete multi-part upload will be added to end of queue in case of
         * multiple parallel uploads submitted. This may result in a delay for
         * processing the complete multi part upload request.
         */
        Stream[] uploadStreams = streamContext.getStreamSuppliers().stream().map(Supplier::get).toArray(Stream[]::new);
        UploadCallable uploadCallable = new UploadCallable(this, executorContainer,
            upload, putObjectRequest, listenerChain, null, transferProgress, uploadStreams,
            streamContext.getTotalContentLength(), getUploadThreadPoolName(writePriority));
        UploadMonitor watcher = UploadMonitor.create(this, upload, executorContainer,
            uploadCallable, putObjectRequest, listenerChain, getUploadThreadPoolName(writePriority));
        upload.setMonitor(watcher);

        return upload;
    }

    private String getUploadThreadPoolName(WritePriority writePriority) {
        return writePriority == WritePriority.HIGH ? ThreadPool.Names.PRIORITY_REMOTE_UPLOAD :
            ThreadPool.Names.REMOTE_UPLOAD;
    }

    @Override
    public void close() throws IOException {
        this.shutdownNow(false);
    }
}
