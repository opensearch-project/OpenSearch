/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileTransferTracker implements FileTransferListener {

    private final Map<String, FileInfo> fileTransferTracker;

    public FileTransferTracker() {
        this.fileTransferTracker = new ConcurrentHashMap<>();
    }

    @Override
    public void onSuccess(FileInfo fileInfo) {
        fileInfo.setTransferState(FileInfo.TransferState.SUCCESS);
        fileTransferTracker.put(fileInfo.getName(), fileInfo);
    }

    @Override
    public void onFailure(FileInfo fileInfo, Exception e) {
        fileInfo.setTransferState(FileInfo.TransferState.FAILED);
        fileTransferTracker.put(fileInfo.getName(), fileInfo);
    }
}
