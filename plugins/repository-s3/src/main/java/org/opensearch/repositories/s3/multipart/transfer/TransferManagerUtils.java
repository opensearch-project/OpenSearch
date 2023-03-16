/*
 * Copyright 2010-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.multipart.transfer;



import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.PauseStatus;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opensearch.common.Stream;
import org.opensearch.common.SuppressForbidden;

import java.io.File;

import static com.amazonaws.services.s3.internal.Constants.MAXIMUM_UPLOAD_PARTS;
import static com.amazonaws.services.s3.internal.Constants.MB;

/**
 * Internal utilities for multipart uploads with TransferManager.
 */
@SuppressForbidden(reason = "java.io.File")
public class TransferManagerUtils {

    private static final Log log = LogFactory.getLog(TransferManagerUtils.class);

    /**
     * Returns true if the specified upload request can use parallel part
     * uploads for increased performance.
     *
     * @param putObjectRequest The request to check.
     * @param inputStreams     Optional input streams array used for multi-stream upload.
     * @return True if this request can use parallel part uploads for faster
     * uploads.
     */
    public static boolean isUploadParallelizable(final PutObjectRequest putObjectRequest,
                                                 Stream[] inputStreams) {
        // Each uploaded part in an encrypted upload depends on the encryption context
        // from the previous upload, so we cannot parallelize encrypted upload parts.

        // Otherwise, if there's a file, we can process the uploads concurrently.
        return (getRequestFile(putObjectRequest) != null || TransferManagerUtils.isMultiStreamUpload(inputStreams));
    }

    /**
     * Returns true if the the specified request should be processed as a
     * multipart upload (instead of a single part upload).
     *
     * @param contentLength Content Length of the upload.
     * @param configuration Configuration settings controlling how transfer manager
     *                      processes requests.
     * @return True if the the specified request should be processed as a
     * multipart upload.
     */
    public static boolean shouldUseMultipartUpload(long contentLength, TransferManagerConfiguration configuration,
                                                   long partSize) {
        return contentLength != partSize;
    }

    /**
     * Convenience method for getting the file specified in a request.
     */
    public static File getRequestFile(final PutObjectRequest putObjectRequest) {
        if (putObjectRequest.getFile() != null) return putObjectRequest.getFile();
        return null;
    }

    /**
     * Determines the pause status based on the current state of transfer.
     */
    public static PauseStatus determinePauseStatus(TransferState transferState,
                                                   boolean forceCancel) {

        if (forceCancel) {
            if (transferState == TransferState.Waiting) {
                return PauseStatus.CANCELLED_BEFORE_START;
            } else if (transferState == TransferState.InProgress) {
                return PauseStatus.CANCELLED;
            }
        }
        if (transferState == TransferState.Waiting) {
            return PauseStatus.NOT_STARTED;
        }
        return PauseStatus.NO_EFFECT;
    }

    public static boolean isMultiStreamUpload(Stream[] inputStreams) {
        return inputStreams != null && inputStreams.length > 0;
    }

    public static boolean isLastPart(int currentStreamIndex, Stream[] inputStreams) {
        return currentStreamIndex == inputStreams.length - 1;
    }

    /**
     * Computes and returns the optimal part size for the upload.
     */
    public static long getOptimalPartSize(long contentLength,
                                          TransferManagerConfiguration configuration,
                                          long minimumPartSizeSetting) {
        if (contentLength <= configuration.getMultipartUploadThreshold()) {
            return contentLength;
        }

        double optimalPartSizeDecimal = (double) contentLength / (double) MAXIMUM_UPLOAD_PARTS;

        // round up so we don't push the upload over the maximum number of parts
        optimalPartSizeDecimal = Math.ceil(optimalPartSizeDecimal);
        long minimumPartSize = Math.max(minimumPartSizeSetting, configuration.getMinimumUploadPartSize());
        long optimalPartSize = (long) Math.max(optimalPartSizeDecimal, minimumPartSize);

        // If optimalPartSize > contentLength, file should be uploaded in a single chunk
        return Math.min(optimalPartSize, contentLength);
    }
}
