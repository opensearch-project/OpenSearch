/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.write;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Nullable;
import org.opensearch.common.StreamContext;

import java.io.IOException;

/**
 * WriteContext is used to encapsulate all data needed by <code>BlobContainer#writeStreams</code>
 *
 * @opensearch.internal
 */
public class WriteContext {

    private final String fileName;
    private final StreamContextSupplier streamContextSupplier;
    private final long fileSize;
    private final boolean failIfAlreadyExists;
    private final WritePriority writePriority;
    private final CheckedConsumer<Boolean, IOException> uploadFinalizer;
    private final boolean doRemoteDataIntegrityCheck;
    private final Long expectedChecksum;

    /**
     * Construct a new WriteContext object
     *
     * @param fileName                   The name of the file being uploaded
     * @param streamContextSupplier      A supplier that will provide StreamContext to the plugin
     * @param fileSize                   The total size of the file being uploaded
     * @param failIfAlreadyExists        A boolean to fail the upload is the file exists
     * @param writePriority              The <code>WritePriority</code> of this upload
     * @param doRemoteDataIntegrityCheck A boolean to inform vendor plugins whether remote data integrity checks need to be done
     * @param expectedChecksum           This parameter expected only when the vendor plugin is expected to do server side data integrity verification
     */
    public WriteContext(
        String fileName,
        StreamContextSupplier streamContextSupplier,
        long fileSize,
        boolean failIfAlreadyExists,
        WritePriority writePriority,
        CheckedConsumer<Boolean, IOException> uploadFinalizer,
        boolean doRemoteDataIntegrityCheck,
        @Nullable Long expectedChecksum
    ) {
        this.fileName = fileName;
        this.streamContextSupplier = streamContextSupplier;
        this.fileSize = fileSize;
        this.failIfAlreadyExists = failIfAlreadyExists;
        this.writePriority = writePriority;
        this.uploadFinalizer = uploadFinalizer;
        this.doRemoteDataIntegrityCheck = doRemoteDataIntegrityCheck;
        this.expectedChecksum = expectedChecksum;
    }

    /**
     * @return The file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @return The boolean representing whether to fail the file upload if it exists
     */
    public boolean isFailIfAlreadyExists() {
        return failIfAlreadyExists;
    }

    /**
     * @param partSize The size of a single part to be uploaded
     * @return The stream context which will be used by the plugin to initialize streams from the file
     */
    public StreamContext getStreamProvider(long partSize) {
        return streamContextSupplier.supplyStreamContext(partSize);
    }

    /**
     * @return The total size of the file
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * @return The <code>WritePriority</code> of the upload
     */
    public WritePriority getWritePriority() {
        return writePriority;
    }

    /**
     * @return The <code>UploadFinalizer</code> for this upload
     */
    public CheckedConsumer<Boolean, IOException> getUploadFinalizer() {
        return uploadFinalizer;
    }

    /**
     * @return A boolean for whether remote data integrity check has to be done for this upload or not
     */
    public boolean doRemoteDataIntegrityCheck() {
        return doRemoteDataIntegrityCheck;
    }

    /**
     * @return The CRC32 checksum associated with this file
     */
    public Long getExpectedChecksum() {
        return expectedChecksum;
    }
}
