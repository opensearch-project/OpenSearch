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

    private final String checksum;

    /**
     * Construct a new UploadResponse object
     *
     * @param checksum Checksum of uploaded object
     */
    public UploadResponse(String checksum) {
        this.checksum = checksum;
    }

    /**
     * @return Checksum of UploadResponse object
     */
    public String getChecksum() {
        return checksum;
    }
}
