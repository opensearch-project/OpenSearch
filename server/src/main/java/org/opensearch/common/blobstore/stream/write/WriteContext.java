/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.write;

import org.opensearch.common.blobstore.stream.StreamContext;

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
    private final Consumer<Boolean> finalizeUploadConsumer;

    /**
     * ABCDE
     *
     * @param fileName
     * @param streamContextSupplier
     * @param fileSize
     * @param failIfAlreadyExists
     */
    public WriteContext(String fileName, StreamContextSupplier streamContextSupplier, long fileSize,
                        boolean failIfAlreadyExists, WritePriority writePriority, Consumer<Boolean> finalizeUploadConsumer) {
        this.fileName = fileName;
        this.streamContextSupplier = streamContextSupplier;
        this.fileSize = fileSize;
        this.failIfAlreadyExists = failIfAlreadyExists;
        this.writePriority = writePriority;
        this.finalizeUploadConsumer = finalizeUploadConsumer;
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

    public Consumer<Boolean> getFinalizeUploadConsumer() {
        return finalizeUploadConsumer;
    }
}
