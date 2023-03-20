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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressListenerChain;
import com.amazonaws.services.s3.transfer.PauseResult;
import com.amazonaws.services.s3.transfer.PauseStatus;
import com.amazonaws.services.s3.transfer.PersistableUpload;
import com.amazonaws.services.s3.transfer.TransferProgress;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.exception.PauseException;
import com.amazonaws.services.s3.transfer.internal.TransferStateChangeListener;
import com.amazonaws.services.s3.transfer.model.UploadResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class UploadImpl extends AbstractTransfer implements Upload {

    public UploadImpl(
        String description,
        TransferProgress transferProgressInternalState,
        ProgressListenerChain progressListenerChain,
        TransferStateChangeListener listener
    ) {
        super(description, transferProgressInternalState, progressListenerChain, listener);
    }

    /**
     * Waits for this upload to complete and returns the result of this upload.
     * This is a blocking call. Be prepared to handle errors when calling this
     * method. Any errors that occurred during the asynchronous transfer will be
     * re-thrown through this method.
     *
     * @return The result of this transfer.
     *
     * @throws AmazonClientException
     *             If any errors were encountered in the client while making the
     *             request or handling the response.
     * @throws AmazonServiceException
     *             If any errors occurred in Amazon S3 while processing the
     *             request.
     * @throws InterruptedException
     *             If this thread is interrupted while waiting for the upload to
     *             complete.
     */
    public UploadResult waitForUploadResult() throws AmazonClientException, AmazonServiceException, InterruptedException {
        try {
            UploadResult result = null;
            while (!monitor.isDone() || result == null) {
                Future<?> f = monitor.getFuture();
                result = (UploadResult) f.get();
            }
            return result;
        } catch (ExecutionException e) {
            rethrowExecutionException(e);
            return null;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.s3.transfer.Upload#pause()
     */
    @Override
    public PersistableUpload pause() throws PauseException {
        PauseResult<PersistableUpload> pauseResult = pause(true);
        if (pauseResult.getPauseStatus() != PauseStatus.SUCCESS) {
            throw new PauseException(pauseResult.getPauseStatus());
        }
        return pauseResult.getInfoToResume();
    }

    /**
     * Tries to pause and return the information required to resume the upload
     * operation.
     */
    private PauseResult<PersistableUpload> pause(final boolean forceCancelTransfers) throws AmazonClientException {
        UploadMonitor uploadMonitor = (UploadMonitor) monitor;
        return uploadMonitor.pause(forceCancelTransfers);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.s3.transfer.Upload#tryPause(boolean)
     */
    @Override
    public PauseResult<PersistableUpload> tryPause(boolean forceCancelTransfers) {
        return pause(forceCancelTransfers);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.s3.transfer.Upload#abort()
     */
    @Override
    public void abort() {
        UploadMonitor uploadMonitor = (UploadMonitor) monitor;
        uploadMonitor.performAbort();
    }
}
