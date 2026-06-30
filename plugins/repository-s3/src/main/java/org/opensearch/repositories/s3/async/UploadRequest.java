/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.stream.write.WritePriority;

import java.io.IOException;
import java.util.Map;

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
    private final Map<String, String> metadata;
    private final boolean uploadRetryEnabled;
    private volatile String serverSideEncryptionType;
    private volatile String serverSideEncryptionKmsKey;
    private volatile boolean serverSideEncryptionBucketKey;
    private volatile String serverSideEncryptionEncryptionContext;
    private final String expectedBucketOwner;

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
     * @param metadata                   Metadata of the file being uploaded
     */
    public UploadRequest(
        String bucket,
        String key,
        long contentLength,
        WritePriority writePriority,
        CheckedConsumer<Boolean, IOException> uploadFinalizer,
        boolean doRemoteDataIntegrityCheck,
        Long expectedChecksum,
        boolean uploadRetryEnabled,
        @Nullable Map<String, String> metadata,
        String serverSideEncryptionType,
        String serverSideEncryptionKmsKey,
        boolean serverSideEncryptionBucketKey,
        @Nullable String serverSideEncryptionEncryptionContext,
        @Nullable String expectedBucketOwner
    ) {
        this.bucket = bucket;
        this.key = key;
        this.contentLength = contentLength;
        this.writePriority = writePriority;
        this.uploadFinalizer = uploadFinalizer;
        this.doRemoteDataIntegrityCheck = doRemoteDataIntegrityCheck;
        this.expectedChecksum = expectedChecksum;
        this.uploadRetryEnabled = uploadRetryEnabled;
        this.metadata = metadata;
        this.serverSideEncryptionType = serverSideEncryptionType;
        this.serverSideEncryptionKmsKey = serverSideEncryptionKmsKey;
        this.serverSideEncryptionBucketKey = serverSideEncryptionBucketKey;
        this.serverSideEncryptionEncryptionContext = serverSideEncryptionEncryptionContext;
        this.expectedBucketOwner = expectedBucketOwner;
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

    public boolean isUploadRetryEnabled() {
        return uploadRetryEnabled;
    }

    /**
     * @return metadata of the blob to be uploaded
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    public String getServerSideEncryptionType() {
        return serverSideEncryptionType;
    }

    public String getServerSideEncryptionKmsKey() {
        return serverSideEncryptionKmsKey;
    }

    public boolean getServerSideEncryptionBucketKey() {
        return serverSideEncryptionBucketKey;
    }

    public String getServerSideEncryptionEncryptionContext() {
        return serverSideEncryptionEncryptionContext;
    }

    public String getExpectedBucketOwner() {
        return expectedBucketOwner;
    }
}
