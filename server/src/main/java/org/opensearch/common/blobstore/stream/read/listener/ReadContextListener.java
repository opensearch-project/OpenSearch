/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReadContextListener implements ActionListener<ReadContext> {

    private final String segmentName;
    private final Path segmentFileLocation;
    private final ThreadPool threadPool;
    private final ActionListener<String> segmentCompletionListener;
    private static final Logger logger = LogManager.getLogger(ReadContextListener.class);

    public ReadContextListener(
        String segmentName,
        Path segmentFileLocation,
        ThreadPool threadPool,
        ActionListener<String> segmentCompletionListener
    ) {
        this.segmentName = segmentName;
        this.segmentFileLocation = segmentFileLocation;
        this.threadPool = threadPool;
        this.segmentCompletionListener = segmentCompletionListener;
    }

    @Override
    public void onResponse(ReadContext readContext) {
        final int numParts = readContext.getNumberOfParts();
        final AtomicBoolean anyStreamFailed = new AtomicBoolean();
        FileCompletionListener fileCompletionListener = new FileCompletionListener(numParts, segmentName, segmentCompletionListener);

        for (int partNumber = 0; partNumber < numParts; partNumber++) {
            StreamCompletionListener streamCompletionListener = new StreamCompletionListener(
                partNumber,
                readContext.getPartStreams().get(partNumber),
                segmentFileLocation,
                anyStreamFailed,
                fileCompletionListener
            );
            new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.GENERIC, streamCompletionListener, false).onResponse(null);
        }
    }

    @Override
    public void onFailure(Exception e) {
        segmentCompletionListener.onFailure(e);
    }
}
