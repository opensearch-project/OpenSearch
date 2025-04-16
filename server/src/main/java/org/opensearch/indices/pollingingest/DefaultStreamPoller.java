/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.engine.IngestionEngine;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Default implementation of {@link StreamPoller}
 */
public class DefaultStreamPoller implements StreamPoller {
    private static final Logger logger = LogManager.getLogger(DefaultStreamPoller.class);

    public static final long MAX_POLL_SIZE = 1000;
    public static final int POLL_TIMEOUT = 1000;

    private volatile State state = State.NONE;

    // goal state
    private volatile boolean started;
    private volatile boolean closed;
    private volatile boolean paused;
    private volatile IngestionErrorStrategy errorStrategy;

    private IngestionShardConsumer consumer;

    private ExecutorService consumerThread;

    private ExecutorService processorThread;

    // start of the batch, inclusive
    private IngestionShardPointer initialBatchStartPointer;
    private boolean includeBatchStartPointer = false;

    private ResetState resetState;
    private final String resetValue;

    private Set<IngestionShardPointer> persistedPointers;

    private BlockingQueue<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> blockingQueue;

    private MessageProcessorRunnable processorRunnable;

    private final CounterMetric totalPolledCount = new CounterMetric();

    // A pointer to the max persisted pointer for optimizing the check
    @Nullable
    private IngestionShardPointer maxPersistedPointer;

    public DefaultStreamPoller(
        IngestionShardPointer startPointer,
        Set<IngestionShardPointer> persistedPointers,
        IngestionShardConsumer consumer,
        IngestionEngine ingestionEngine,
        ResetState resetState,
        String resetValue,
        IngestionErrorStrategy errorStrategy,
        State initialState,
        long maxPollSize,
        int pollTimeout
    ) {
        this(
            startPointer,
            persistedPointers,
            consumer,
            new MessageProcessorRunnable(new ArrayBlockingQueue<>(100), ingestionEngine, errorStrategy),
            resetState,
            resetValue,
            errorStrategy,
            initialState
        );
    }

    DefaultStreamPoller(
        IngestionShardPointer startPointer,
        Set<IngestionShardPointer> persistedPointers,
        IngestionShardConsumer consumer,
        MessageProcessorRunnable processorRunnable,
        ResetState resetState,
        String resetValue,
        IngestionErrorStrategy errorStrategy,
        State initialState
    ) {
        this.consumer = Objects.requireNonNull(consumer);
        this.resetState = resetState;
        this.resetValue = resetValue;
        this.initialBatchStartPointer = startPointer;
        this.state = initialState;
        this.persistedPointers = persistedPointers;
        if (!this.persistedPointers.isEmpty()) {
            maxPersistedPointer = this.persistedPointers.stream().max(IngestionShardPointer::compareTo).get();
        }
        this.processorRunnable = processorRunnable;
        blockingQueue = processorRunnable.getBlockingQueue();
        this.consumerThread = Executors.newSingleThreadExecutor(
            r -> new Thread(
                r,
                String.format(Locale.ROOT, "stream-poller-consumer-%d-%d", consumer.getShardId(), System.currentTimeMillis())
            )
        );

        // TODO: allow multiple threads for processing the messages in parallel
        this.processorThread = Executors.newSingleThreadExecutor(
            r -> new Thread(
                r,
                String.format(Locale.ROOT, "stream-poller-processor-%d-%d", consumer.getShardId(), System.currentTimeMillis())
            )
        );
        this.errorStrategy = errorStrategy;
    }

    @Override
    public void start() {
        if (closed) {
            throw new RuntimeException("poller is closed!");
        }

        if (started) {
            throw new RuntimeException("poller is already running");
        }

        started = true;
        // when we start, we need to include the batch start pointer in the read for the first read
        includeBatchStartPointer = true;
        consumerThread.submit(this::startPoll);
        processorThread.submit(processorRunnable);
    }

    /**
     * Start the poller. visibile for testing
     */
    protected void startPoll() {
        if (!started) {
            throw new IllegalStateException("poller is not started!");
        }
        if (closed) {
            throw new IllegalStateException("poller is closed!");
        }
        logger.info("Starting poller for shard {}", consumer.getShardId());

        while (true) {
            try {
                if (closed) {
                    state = State.CLOSED;
                    break;
                }

                // reset the offset
                if (resetState != ResetState.NONE) {
                    switch (resetState) {
                        case EARLIEST:
                            initialBatchStartPointer = consumer.earliestPointer();
                            logger.info("Resetting offset by seeking to earliest offset {}", initialBatchStartPointer.asString());
                            break;
                        case LATEST:
                            initialBatchStartPointer = consumer.latestPointer();
                            logger.info("Resetting offset by seeking to latest offset {}", initialBatchStartPointer.asString());
                            break;
                        case REWIND_BY_OFFSET:
                            initialBatchStartPointer = consumer.pointerFromOffset(resetValue);
                            logger.info("Resetting offset by seeking to offset {}", initialBatchStartPointer.asString());
                            break;
                        case REWIND_BY_TIMESTAMP:
                            initialBatchStartPointer = consumer.pointerFromTimestampMillis(Long.parseLong(resetValue));
                            logger.info(
                                "Resetting offset by seeking to timestamp {}, corresponding offset {}",
                                resetValue,
                                initialBatchStartPointer.asString()
                            );
                            break;
                    }
                    resetState = ResetState.NONE;
                }

                if (paused) {
                    state = State.PAUSED;
                    try {
                        // TODO: make sleep time configurable
                        Thread.sleep(100);
                    } catch (Throwable e) {
                        logger.error("Error in pausing the poller of shard {}: {}", consumer.getShardId(), e);
                    }
                    continue;
                }

                state = State.POLLING;

                List<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> results;

                if (includeBatchStartPointer) {
                    results = consumer.readNext(initialBatchStartPointer, true, MAX_POLL_SIZE, POLL_TIMEOUT);
                    includeBatchStartPointer = false;
                } else {
                    results = consumer.readNext(MAX_POLL_SIZE, POLL_TIMEOUT);
                }

                if (results.isEmpty()) {
                    // no new records
                    continue;
                }

                state = State.PROCESSING;
                processRecords(results);
            } catch (Exception e) {
                // Pause ingestion when an error is encountered while polling the streaming source.
                // Currently we do not have a good way to skip past the failing messages.
                // The user will have the option to manually update the offset and resume ingestion.
                // todo: support retry?
                logger.error("Pausing ingestion. Fatal error occurred in polling the shard {}: {}", consumer.getShardId(), e);
                pause();
            }
        }
    }

    private void processRecords(List<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> results) {
        for (IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message> result : results) {
            try {
                // check if the message is already processed
                if (isProcessed(result.getPointer())) {
                    logger.debug("Skipping message with pointer {} as it is already processed", () -> result.getPointer().asString());
                    continue;
                }
                totalPolledCount.inc();
                blockingQueue.put(result);

                logger.debug(
                    "Put message {} with pointer {} to the blocking queue",
                    String.valueOf(result.getMessage().getPayload()),
                    result.getPointer().asString()
                );
            } catch (Exception e) {
                logger.error(
                    "Error in processing a record. Shard {}, pointer {}: {}",
                    consumer.getShardId(),
                    result.getPointer().asString(),
                    e
                );
                errorStrategy.handleError(e, IngestionErrorStrategy.ErrorStage.POLLING);

                if (!errorStrategy.shouldIgnoreError(e, IngestionErrorStrategy.ErrorStage.POLLING)) {
                    // Blocking error encountered. Pause poller to stop processing remaining updates.
                    pause();
                    break;
                }
            }
        }
    }

    private boolean isProcessed(IngestionShardPointer pointer) {
        if (maxPersistedPointer == null) {
            return false;
        }
        if (pointer.compareTo(maxPersistedPointer) > 0) {
            return false;
        }
        return persistedPointers.contains(pointer);
    }

    /**
     * Visible for testing. Get the max persisted pointer
     * @return the max persisted pointer
     */
    protected IngestionShardPointer getMaxPersistedPointer() {
        return maxPersistedPointer;
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
        if (!started) {
            logger.info("consumer thread not started");
            return;
        }
        long startTime = System.currentTimeMillis(); // Record the start time
        long timeout = 5000;
        while (state != State.CLOSED) {
            // Check if the timeout has been reached
            if (System.currentTimeMillis() - startTime > timeout) {
                logger.error("Timeout reached while waiting for shard {} to close", consumer.getShardId());
                break; // Exit the loop if the timeout is reached
            }
            try {
                Thread.sleep(100);
            } catch (Throwable e) {
                logger.error("Error in closing the poller of shard {}: {}", consumer.getShardId(), e);
            }
        }
        blockingQueue.clear();
        consumerThread.shutdown();
        // interrupts the processor
        processorThread.shutdownNow();
        logger.info("closed the poller of shard {}", consumer.getShardId());
    }

    @Override
    public boolean isPaused() {
        return paused;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Returns the batch start pointer from where the poller can resume in case of shard recovery. The poller and
     * processor are decoupled in this implementation, and hence the latest pointer tracked by the processor acts as the
     * recovery/start point. In case the processor has not started tracking, then the initial batchStartPointer used by
     * the poller acts as the start point.
     */
    @Override
    public IngestionShardPointer getBatchStartPointer() {
        IngestionShardPointer currentShardPointer = processorRunnable.getCurrentShardPointer();
        return currentShardPointer == null ? initialBatchStartPointer : currentShardPointer;
    }

    @Override
    public PollingIngestStats getStats() {
        PollingIngestStats.Builder builder = new PollingIngestStats.Builder();
        builder.setTotalPolledCount(totalPolledCount.count());
        builder.setTotalProcessedCount(processorRunnable.getProcessedCounter().count());
        builder.setTotalSkippedCount(processorRunnable.getSkippedCounter().count());
        return builder.build();
    }

    public State getState() {
        return this.state;
    }

    @Override
    public IngestionErrorStrategy getErrorStrategy() {
        return this.errorStrategy;
    }

    @Override
    public void updateErrorStrategy(IngestionErrorStrategy errorStrategy) {
        this.errorStrategy = errorStrategy;
        processorRunnable.setErrorStrategy(errorStrategy);
    }
}
