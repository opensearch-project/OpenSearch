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
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.PauseResult;
import com.amazonaws.services.s3.transfer.PauseStatus;
import com.amazonaws.services.s3.transfer.PersistableUpload;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.TransferMonitor;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.repositories.s3.SocketAccess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazonaws.event.SDKProgressPublisher.publishProgress;

/**
 * Manages an upload by periodically checking to see if the upload is done, and
 * returning a result if so. Otherwise, schedules a copy of itself to be run in
 * the futureReference and returns null. When waiting on the result of this class via a
 * Future object, clients must call {@link UploadMonitor#isDone()} and
 * {@link UploadMonitor#getFuture()}
 */
@SuppressForbidden(reason = "AWS S3 SDK")
public class UploadMonitor implements Callable<UploadResult>, TransferMonitor {

    private final AmazonS3 s3;
    private final PutObjectRequest origReq;
    private final ProgressListenerChain listener;
    private final UploadCallable multipartUploadCallable;
    private final UploadImpl transfer;
    private final ExecutorService threadPool;

    /*
     * Futures of threads that upload the parts.
     */
    private final List<Future<PartETag>> futures = Collections.synchronizedList(new ArrayList<Future<PartETag>>());

    /*
     * State for clients wishing to poll for completion
     */
    private boolean isUploadDone = false;
    private AtomicReference<Future<UploadResult>> futureReference = new AtomicReference<Future<UploadResult>>(null);

    public Future<UploadResult> getFuture() {
        return futureReference.get();
    }

    private synchronized void cancelFuture() {
        futureReference.get().cancel(true);
    }

    public synchronized boolean isDone() {
        return isUploadDone;
    }

    private synchronized void markAllDone() {
        isUploadDone = true;
    }

    /**
     * Constructs a new upload watcher and then immediately submits it to
     * the thread pool.
     *
     * @param manager
     *            The {@link TransferManager} that owns this upload.
     * @param transfer
     *            The transfer being processed.
     * @param threadPool
     *            The {@link ExecutorService} to which we should submit new
     *            tasks.
     * @param multipartUploadCallable
     *            The callable responsible for processing the upload
     *            asynchronously
     * @param putObjectRequest
     *            The original putObject request
     * @param progressListenerChain
     *            A chain of listeners that wish to be notified of upload
     *            progress
     */
    public static UploadMonitor create(
        TransferManager manager,
        UploadImpl transfer,
        ExecutorService threadPool,
        UploadCallable multipartUploadCallable,
        PutObjectRequest putObjectRequest,
        ProgressListenerChain progressListenerChain
    ) {

        UploadMonitor uploadMonitor = new UploadMonitor(
            manager,
            transfer,
            threadPool,
            multipartUploadCallable,
            putObjectRequest,
            progressListenerChain
        );
        Future<UploadResult> thisFuture = threadPool.submit(uploadMonitor);
        // Use an atomic compareAndSet to prevent a possible race between the
        // setting of the UploadMonitor's futureReference, and setting the
        // CompleteMultipartUpload's futureReference within the call() method.
        // We only want to set the futureReference to UploadMonitor's futureReference if the
        // current value is null, otherwise the futureReference that's set is
        // CompleteMultipartUpload's which is ultimately what we want.
        uploadMonitor.futureReference.compareAndSet(null, thisFuture);
        return uploadMonitor;
    }

    private UploadMonitor(
        TransferManager manager,
        UploadImpl transfer,
        ExecutorService threadPool,
        UploadCallable multipartUploadCallable,
        PutObjectRequest putObjectRequest,
        ProgressListenerChain progressListenerChain
    ) {

        this.s3 = manager.getAmazonS3Client();
        this.multipartUploadCallable = multipartUploadCallable;
        this.origReq = putObjectRequest;
        this.listener = progressListenerChain;
        this.transfer = transfer;
        this.threadPool = threadPool;
    }

    @Override
    public UploadResult call() throws Exception {
        try {
            UploadResult result = SocketAccess.doPrivilegedIOException(multipartUploadCallable::call);

            /**
             * If the result is null, it is a mutli part parellel upload. So, an
             * new task is submitted for initiating a complete multi part upload
             * request.
             */
            if (result == null) {
                futures.addAll(multipartUploadCallable.getFutures());
                futureReference.set(
                    threadPool.submit(
                        new CompleteMultipartUpload(
                            multipartUploadCallable.getMultipartUploadId(),
                            s3,
                            origReq,
                            futures,
                            multipartUploadCallable.getETags(),
                            listener,
                            this
                        )
                    )
                );
            } else {
                uploadComplete();
            }
            return result;
        } catch (CancellationException e) {
            transfer.setState(TransferState.Canceled);
            publishProgress(listener, ProgressEventType.TRANSFER_CANCELED_EVENT);
            throw new SdkClientException("Upload canceled");
        } catch (Exception e) {
            transfer.setState(TransferState.Failed);
            throw e;
        }
    }

    void uploadComplete() {
        markAllDone();
        transfer.setState(TransferState.Completed);

        // AmazonS3Client takes care of all the events for single part uploads,
        // so we only need to send a completed event for multipart uploads.
        if (multipartUploadCallable.isMultipartUpload()) {
            publishProgress(listener, ProgressEventType.TRANSFER_COMPLETED_EVENT);
        }
    }

    /**
     * Marks the upload as a failure.
     */
    void uploadFailure() {
        transfer.setState(TransferState.Failed);
    }

    /**
     * Cancels the futures in the following cases - If the user has requested
     * for forcefully aborting the transfers. - If the upload is a multi part
     * parellel upload. - If the upload operation hasn't started. Cancels all
     * the in flight transfers of the upload if applicable. Returns the
     * multi-part upload Id in case of the parallel multi-part uploads. Returns
     * null otherwise.
     */
    PauseResult<PersistableUpload> pause(boolean forceCancel) {

        PersistableUpload persistableUpload = multipartUploadCallable.getPersistableUpload();
        if (persistableUpload == null) {
            PauseStatus pauseStatus = TransferManagerUtils.determinePauseStatus(transfer.getState(), forceCancel);
            if (forceCancel) {
                cancelFutures();
                multipartUploadCallable.performAbortMultipartUpload();
            }
            return new PauseResult<PersistableUpload>(pauseStatus);
        }
        cancelFutures();
        return new PauseResult<PersistableUpload>(PauseStatus.SUCCESS, persistableUpload);
    }

    /**
     * Cancels the inflight transfers if they are not completed.
     */
    private void cancelFutures() {
        cancelFuture();
        for (Future<PartETag> f : futures) {
            f.cancel(true);
        }
        multipartUploadCallable.getFutures().clear();
        futures.clear();
    }

    /**
     * Cancels all the futures associated with this upload operation. Also
     * cleans up the parts on Amazon S3 if the upload is performed as a
     * multi-part upload operation.
     */
    void performAbort() {
        cancelFutures();
        multipartUploadCallable.performAbortMultipartUpload();
        publishProgress(listener, ProgressEventType.TRANSFER_CANCELED_EVENT);
    }
}
