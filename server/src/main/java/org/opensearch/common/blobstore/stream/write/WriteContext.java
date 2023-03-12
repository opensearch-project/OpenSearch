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

import java.util.function.Consumer;

/**
 * ABCDE
 *
 * @opensearch.internal
 */
public class WriteContext {

    private final String fileName;
    private final StreamContextSupplier streamContextSupplier;
    private final long fileSize;
    private final boolean failIfAlreadyExists;
    private final WritePriority writePriority;
    private final UploadFinalizer uploadFinalizer;

    /**
     * ABCDE
     *
     * @param fileName
     * @param streamContextSupplier
     * @param fileSize
     * @param failIfAlreadyExists
     */
    public WriteContext(String fileName, StreamContextSupplier streamContextSupplier, long fileSize,
                        boolean failIfAlreadyExists, WritePriority writePriority, UploadFinalizer uploadFinalizer) {
        this.fileName = fileName;
        this.streamContextSupplier = streamContextSupplier;
        this.fileSize = fileSize;
        this.failIfAlreadyExists = failIfAlreadyExists;
        this.writePriority = writePriority;
        this.uploadFinalizer = uploadFinalizer;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public boolean isFailIfAlreadyExists() {
        return failIfAlreadyExists;
    }

    /**
     * ABCDE
     *
     * @param partSize
     * @return
     */
    public StreamContext getStreamContext(long partSize) {
        return streamContextSupplier.supplyStreamContext(partSize);
    }

    /**
     * ABCDE
     *
     * @return
     */
    public long getFileSize() {
        return fileSize;
    }

    public WritePriority getWritePriority() {
        return writePriority;
    }

    public UploadFinalizer getUploadFinalizer() {
        return uploadFinalizer;
    }
}
