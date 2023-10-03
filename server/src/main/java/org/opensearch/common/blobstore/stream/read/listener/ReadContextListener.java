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
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

/**
 * ReadContextListener orchestrates the async file fetch from the {@link org.opensearch.common.blobstore.BlobContainer}
 * using a {@link ReadContext} callback. On response, it spawns off the download using multiple streams.
 */
@InternalApi
public class ReadContextListener implements ActionListener<ReadContext> {
    private static final Logger logger = LogManager.getLogger(ReadContextListener.class);

    private final String blobName;
    private final Path fileLocation;
    private final ActionListener<String> completionListener;
    private final ThreadPool threadPool;
    private final UnaryOperator<InputStream> rateLimiter;
    private final int maxConcurrentStreams;

    public ReadContextListener(
        String blobName,
        Path fileLocation,
        ActionListener<String> completionListener,
        ThreadPool threadPool,
        UnaryOperator<InputStream> rateLimiter,
        int maxConcurrentStreams
    ) {
        this.blobName = blobName;
        this.fileLocation = fileLocation;
        this.completionListener = completionListener;
        this.threadPool = threadPool;
        this.rateLimiter = rateLimiter;
        this.maxConcurrentStreams = maxConcurrentStreams;
    }

    @Override
    public void onResponse(ReadContext readContext) {
        logger.debug("Received {} parts for blob {}", readContext.getNumberOfParts(), blobName);
        final int numParts = readContext.getNumberOfParts();
        final AtomicBoolean anyPartStreamFailed = new AtomicBoolean(false);
        final GroupedActionListener<String> groupedListener = new GroupedActionListener<>(
            ActionListener.wrap(r -> completionListener.onResponse(blobName), completionListener::onFailure),
            numParts
        );
        final Queue<ReadContext.StreamPartCreator> queue = new ConcurrentLinkedQueue<>(readContext.getPartStreams());
        final StreamPartProcessor processor = new StreamPartProcessor(
            queue,
            anyPartStreamFailed,
            fileLocation,
            groupedListener,
            threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY),
            rateLimiter
        );
        for (int i = 0; i < Math.min(maxConcurrentStreams, queue.size()); i++) {
            processor.process(queue.poll());
        }
    }

    @Override
    public void onFailure(Exception e) {
        completionListener.onFailure(e);
    }

    private static class StreamPartProcessor {
        private static final RuntimeException CANCELED_PART_EXCEPTION = new RuntimeException(
            "Canceled part download due to previous failure"
        );
        private final Queue<ReadContext.StreamPartCreator> queue;
        private final AtomicBoolean anyPartStreamFailed;
        private final Path fileLocation;
        private final GroupedActionListener<String> completionListener;
        private final Executor executor;
        private final UnaryOperator<InputStream> rateLimiter;

        private StreamPartProcessor(
            Queue<ReadContext.StreamPartCreator> queue,
            AtomicBoolean anyPartStreamFailed,
            Path fileLocation,
            GroupedActionListener<String> completionListener,
            Executor executor,
            UnaryOperator<InputStream> rateLimiter
        ) {
            this.queue = queue;
            this.anyPartStreamFailed = anyPartStreamFailed;
            this.fileLocation = fileLocation;
            this.completionListener = completionListener;
            this.executor = executor;
            this.rateLimiter = rateLimiter;
        }

        private void process(ReadContext.StreamPartCreator supplier) {
            if (supplier == null) {
                return;
            }
            supplier.get().whenCompleteAsync((blobPartStreamContainer, throwable) -> {
                if (throwable != null) {
                    processFailure(throwable instanceof Exception ? (Exception) throwable : new RuntimeException(throwable));
                } else if (anyPartStreamFailed.get()) {
                    processFailure(CANCELED_PART_EXCEPTION);
                } else {
                    try {
                        FilePartWriter.write(fileLocation, blobPartStreamContainer, rateLimiter);
                        completionListener.onResponse(fileLocation.toString());

                        // Upon successfully completing a file part, pull another
                        // file part off the queue to trigger asynchronous processing
                        process(queue.poll());
                    } catch (Exception e) {
                        processFailure(e);
                    }
                }
            }, executor);
        }

        private void processFailure(Exception e) {
            if (anyPartStreamFailed.getAndSet(true) == false) {
                completionListener.onFailure(e);

                // Drain the queue of pending part downloads. These can be discarded
                // since they haven't started any work yet, but the listener must be
                // notified for each part.
                Object item = queue.poll();
                while (item != null) {
                    completionListener.onFailure(CANCELED_PART_EXCEPTION);
                    item = queue.poll();
                }
            } else {
                completionListener.onFailure(e);
            }
            try {
                Files.deleteIfExists(fileLocation);
            } catch (IOException ex) {
                // Die silently
                logger.info("Failed to delete file {} on stream failure: {}", fileLocation, ex);
            }
        }
    }
}
