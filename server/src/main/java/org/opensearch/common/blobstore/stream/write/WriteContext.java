/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.write;

import org.opensearch.common.blobstore.stream.StreamContext;
import org.opensearch.common.blobstore.transfer.UploadFinalizer;

/**
 * WriteContext is used to encapsulate all data needed by <code>BlobContainer#writeStreams</code>
 */
public class WriteContext {

    private final String fileName;
    private final StreamContextSupplier streamContextSupplier;
    private final long fileSize;
    private final boolean failIfAlreadyExists;
    private final WritePriority writePriority;
    private final UploadFinalizer uploadFinalizer;

    /**
     * Construct a new WriteContext object
     *
     * @param fileName              The name of the file being uploaded
     * @param streamContextSupplier A supplier that will provide StreamContext to the plugin
     * @param fileSize              The total size of the file being uploaded
     * @param failIfAlreadyExists   A boolean to fail the upload is the file exists
     * @param writePriority         The <code>WritePriority</code> of this upload
     */
    public WriteContext(
        String fileName,
        StreamContextSupplier streamContextSupplier,
        long fileSize,
        boolean failIfAlreadyExists,
        WritePriority writePriority,
        UploadFinalizer uploadFinalizer
    ) {
        this.fileName = fileName;
        this.streamContextSupplier = streamContextSupplier;
        this.fileSize = fileSize;
        this.failIfAlreadyExists = failIfAlreadyExists;
        this.writePriority = writePriority;
        this.uploadFinalizer = uploadFinalizer;
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
    public StreamContext getStreamContext(long partSize) {
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
    public UploadFinalizer getUploadFinalizer() {
        return uploadFinalizer;
    }
}
