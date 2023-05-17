/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.write;

/**
 * Response object for uploads using <code>BlobContainer#writeBlobByStreams</code>
 */
public class UploadResponse {

    private final boolean uploadSuccessful;

    /**
     * Construct a new UploadResponse object
     *
     * @param uploadSuccessful Whether the current upload was successful or not
     */
    public UploadResponse(boolean uploadSuccessful) {
        this.uploadSuccessful = uploadSuccessful;
    }

    /**
     * @return The upload success result
     */
    public boolean isUploadSuccessful() {
        return uploadSuccessful;
    }
}
