/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * FileCompletionListener listens for completion of fetch on all the streams for a file, where
 * individual streams are handled using {@link FilePartWriter}. The {@link FilePartWriter}(s)
 * hold a reference to the file completion listener to be notified.
 */
@InternalApi
class FileCompletionListener implements ActionListener<Integer> {

    private final int numberOfParts;
    private final String fileName;
    private final AtomicInteger completedPartsCount;
    private final ActionListener<String> completionListener;

    public FileCompletionListener(int numberOfParts, String fileName, ActionListener<String> completionListener) {
        this.completedPartsCount = new AtomicInteger();
        this.numberOfParts = numberOfParts;
        this.fileName = fileName;
        this.completionListener = completionListener;
    }

    @Override
    public void onResponse(Integer unused) {
        if (completedPartsCount.incrementAndGet() == numberOfParts) {
            completionListener.onResponse(fileName);
        }
    }

    @Override
    public void onFailure(Exception e) {
        completionListener.onFailure(e);
    }
}
