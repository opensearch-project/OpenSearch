/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import java.nio.file.Path;

public class FileInfo {
    private final Path path;
    private final long checksum;
    private final long contentLength;
    private final String name;
    private TransferState transferState;

    public FileInfo(String name, Path path, long checksum, long contentLength) {
        this.name = name;
        this.path = path;
        this.checksum = checksum;
        this.contentLength = contentLength;
        this.transferState = TransferState.INIT;
    }

    public Path getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public long getChecksum() {
        return checksum;
    }

    public long getContentLength() {
        return contentLength;
    }

    public synchronized void setTransferState(TransferState transferState) {

    }

    public enum TransferState {
        INIT,
        STARTED,
        SUCCESS,
        FAILED,
        DELETED
    }
}
