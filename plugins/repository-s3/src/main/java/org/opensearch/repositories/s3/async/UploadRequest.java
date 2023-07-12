/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.blobstore.stream.write.WritePriority;

import java.io.IOException;

/**
 * A model encapsulating all details for an upload to S3
 */
public class UploadRequest {
    private final String bucket;
    private final String key;
    private final long contentLength;
    private final WritePriority writePriority;
    private final CheckedConsumer<Boolean, IOException> uploadFinalizer;
    private final boolean doRemoteDataIntegrityCheck;
    private final Long expectedChecksum;

    /**
     * Construct a new UploadRequest object
     *
     * @param bucket                     The name of the S3 bucket
     * @param key                        Key of the file needed to be uploaded
     * @param contentLength              Total content length of the file for upload
     * @param writePriority              The priority of this upload
     * @param uploadFinalizer            An upload finalizer to call once all parts are uploaded
     * @param doRemoteDataIntegrityCheck A boolean to inform vendor plugins whether remote data integrity checks need to be done
     * @param expectedChecksum           Checksum of the file being uploaded for remote data integrity check
     */
    public UploadRequest(
        String bucket,
        String key,
        long contentLength,
        WritePriority writePriority,
        CheckedConsumer<Boolean, IOException> uploadFinalizer,
        boolean doRemoteDataIntegrityCheck,
        Long expectedChecksum
    ) {
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.writePriority = writePriority;
        this.uploadFinalizer = uploadFinalizer;
        this.doRemoteDataIntegrityCheck = doRemoteDataIntegrityCheck;
        this.expectedChecksum = expectedChecksum;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public long getContentLength() {
        return contentLength;
    }

    public WritePriority getWritePriority() {
        return writePriority;
    }

    public CheckedConsumer<Boolean, IOException> getUploadFinalizer() {
        return uploadFinalizer;
    }

    public boolean doRemoteDataIntegrityCheck() {
        return doRemoteDataIntegrityCheck;
    }

    public Long getExpectedChecksum() {
        return expectedChecksum;
    }
}
