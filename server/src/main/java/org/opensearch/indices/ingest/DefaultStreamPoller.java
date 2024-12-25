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
import org.opensearch.common.Nullable;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;

import java.util.List;
import java.util.Set;
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

    private ResetState resetState;

    // todo: find the default value
    private IngestionShardPointer currentPointer;

    private Set<IngestionShardPointer> persistedPointers;

    // A pointer to the max persisted pointer for optimizing the check
    @Nullable
    private IngestionShardPointer maxPersistedPointer;

    public DefaultStreamPoller(IngestionShardPointer startPointer,
                               Set<IngestionShardPointer> persistedPointers,
                               IngestionShardConsumer consumer,
                               DocumentProcessor processor,
                               ResetState resetState) {
        this.consumer = consumer;
        this.processor = processor;
        this.resetState = resetState;
        batchStartPointer = startPointer;
        this.persistedPointers = persistedPointers;
        if(!this.persistedPointers.isEmpty()) {
            maxPersistedPointer = this.persistedPointers.stream().max(IngestionShardPointer::compareTo).get();
        }
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

                // reset the offset
                if (resetState!= ResetState.NONE) {
                    switch (resetState) {
                        case EARLIEST:
                            batchStartPointer = consumer.earliestPointer();
                            logger.info("Resetting offset by seeking to earliest offset {}", batchStartPointer.asString());
                            break;
                        case LATEST:
                            batchStartPointer = consumer.latestPointer();
                            logger.info("Resetting offset by seeking to latest offset {}", batchStartPointer.asString());
                            break;
                    }
                    resetState = ResetState.NONE;
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
                    continue;
                }

                state = State.PROCESSING;
                // process the records
                // TODO: consider a separate thread to decoupling the polling and processing
                for (IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message> result : results) {
                    // check if the message is already processed
                    if (isProcessed(result.getPointer())) {
                        logger.info("Skipping message with pointer {} as it is already processed", result.getPointer().asString());
                        continue;
                    }
                    processor.accept(result.getMessage());
                    currentPointer = result.getPointer();
                    logger.info("Processed message {} with pointer {}", result.getMessage().getPayload(), currentPointer.asString());
                }
                // update the batch start pointer to the next batch
                batchStartPointer = consumer.nextPointer();
            }  catch (Throwable e) {
                // TODO better error handling
                logger.error("Error in polling the shard {}", consumer.getShardId(), e);
            }
        }
    }

    private boolean isProcessed(IngestionShardPointer pointer) {
        if (maxPersistedPointer == null) {
            return false;
        }
        if(pointer.compareTo(maxPersistedPointer) > 0) {
            return false;
        }
        return persistedPointers.contains(pointer);
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
    public void resetPointer(IngestionShardPointer batchStartPointer) {
        this.batchStartPointer = batchStartPointer;
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
