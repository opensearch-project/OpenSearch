/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Default implementation of {@link StreamPoller}
 */
public class DefaultStreamPoller implements StreamPoller {
    private static final Logger logger = LogManager.getLogger(DefaultStreamPoller.class);

    // TODO: make this configurable
    public static final long MAX_POLL_SIZE = 1000;
    public static final int POLL_TIMEOUT = 1000;


    private volatile State state = State.NONE;

    // goal state
    private volatile boolean closed;
    private volatile boolean paused;


    private IngestionShardConsumer consumer;

    private ExecutorService consumerThread;

    private DocumentProcessor processor;

    // start of the batch, inclusive
    private IngestionShardPointer batchStartPointer;

    // end of the batch, exclusive
    private IngestionShardPointer batchEndPointer;

    // batch end pointer restored from the last checkpoint
    private IngestionShardPointer restoredBatchEndPointer;

    // todo: find the default value
    private IngestionShardPointer currentPointer;

    public DefaultStreamPoller(IngestionShardPointer startPointer, IngestionShardConsumer consumer, DocumentProcessor processor) {
        this.consumer = consumer;
        this.processor = processor;
        batchStartPointer = startPointer;
        batchEndPointer = startPointer;
        this.consumerThread =
            Executors.newSingleThreadExecutor(
                r ->
                    new Thread(
                        r,
                        String.format("stream-poller-%d-%d", consumer.getShardId(), System.currentTimeMillis())));
    }

    @Override
    public void start() {
        if (closed) {
            throw new RuntimeException("poller is closed!");
        }
        consumerThread.submit(this::startPoll).isDone();
    }

    private void startPoll() {
        if (closed) {
            throw new RuntimeException("poller is closed!");
        }
        logger.info("Starting poller for shard {}", consumer.getShardId());

        while (true) {
            try {
                if (closed) {
                    state = State.CLOSED;
                    break;
                }

                if (paused) {
                    state = State.PAUSED;
                    try {
                        Thread.sleep(1000);
                    } catch (Throwable e) {
                        logger.error(
                            "Error in pausing the poller of shard {}",
                            consumer.getShardId(),
                            e);
                    }
                    continue;
                }

                state = State.POLLING;

                List<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> results
                    = consumer.readNext(batchStartPointer, MAX_POLL_SIZE, POLL_TIMEOUT);

                if(results.isEmpty()) {
                    // no new records
                    batchEndPointer = batchStartPointer;
                    continue;
                } else {
                    batchEndPointer = consumer.nextPointer();
                }
                state = State.PROCESSING;
                // process the records
                // TODO: consider a separate thread to decoupling the polling and processing
                for (IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message> result : results) {
                    processor.accept(result.getMessage());
                    currentPointer = result.getPointer();
                }

                // move pointer to read next
                batchStartPointer = batchEndPointer;
            }  catch (Throwable e) {
                // TODO better error handling
                logger.error("Error in polling the shard {}", consumer.getShardId(), e);
            }
        }
    }

    @Override
    public void pause() {
        if (closed) {
            throw new RuntimeException("consumer is closed!");
        }
        paused = true;
    }

    @Override
    public void resume() {
        if (closed) {
            throw new RuntimeException("consumer is closed!");
        }
        paused = false;
    }

    @Override
    public void close() {
        closed = true;
        while (state != State.CLOSED) {
            try {
                Thread.sleep(1000);
            } catch (Throwable e) {
                logger.error("Error in closing the poller of shard {}", consumer.getShardId(), e);
            }
        }
        consumerThread.shutdown();
    }

    @Override
    public IngestionShardPointer getCurrentPointer() {
        return currentPointer;
    }

    @Override
    public void resetPointer(IngestionShardPointer batchStartPointer, IngestionShardPointer batchEndPointer) {
        this.batchStartPointer = batchStartPointer;
        this.restoredBatchEndPointer = batchEndPointer;
    }

    @Override
    public void resetPointer() {
        throw new UnsupportedOperationException("reset pointer is not supported");
    }

    @Override
    public boolean isPaused() {
        return paused;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public String getBatchStartPointer() {
        return batchStartPointer.asString();
    }

    @Override
    public String getBatchEndPointer() {
        return batchEndPointer.asString();
    }

    public State getState() {
        return state;
    }

    public enum State {
        NONE,
        CLOSED,
        PAUSED,
        POLLING,
        PROCESSING,
    }
}
