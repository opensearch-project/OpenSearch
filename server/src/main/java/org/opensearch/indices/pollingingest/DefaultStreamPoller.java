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

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Default implementation of {@link StreamPoller}
 */
public class DefaultStreamPoller implements StreamPoller {
    private static final Logger logger = LogManager.getLogger(DefaultStreamPoller.class);

    private volatile State state = State.NONE;

    // goal state
    private volatile boolean started;
    private volatile boolean closed;
    private volatile boolean paused;
    private volatile IngestionErrorStrategy errorStrategy;

    private volatile long lastPolledMessageTimestamp = 0;

    private IngestionShardConsumer consumer;

    private ExecutorService consumerThread;

    // start of the batch, inclusive
    private IngestionShardPointer initialBatchStartPointer;
    private boolean includeBatchStartPointer = false;

    private ResetState resetState;
    private final String resetValue;

    private long maxPollSize;
    private int pollTimeout;

    private Set<IngestionShardPointer> persistedPointers;

    private final CounterMetric totalPolledCount = new CounterMetric();

    // A pointer to the max persisted pointer for optimizing the check
    @Nullable
    private IngestionShardPointer maxPersistedPointer;

    private PartitionedBlockingQueueContainer blockingQueueContainer;

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
        int pollTimeout,
        int numProcessorThreads
    ) {
        this(
            startPointer,
            persistedPointers,
            consumer,
            new PartitionedBlockingQueueContainer(numProcessorThreads, consumer.getShardId(), ingestionEngine, errorStrategy),
            resetState,
            resetValue,
            errorStrategy,
            initialState,
            maxPollSize,
            pollTimeout
        );
    }

    DefaultStreamPoller(
        IngestionShardPointer startPointer,
        Set<IngestionShardPointer> persistedPointers,
        IngestionShardConsumer consumer,
        PartitionedBlockingQueueContainer blockingQueueContainer,
        ResetState resetState,
        String resetValue,
        IngestionErrorStrategy errorStrategy,
        State initialState,
        long maxPollSize,
        int pollTimeout
    ) {
        this.consumer = Objects.requireNonNull(consumer);
        this.resetState = resetState;
        this.resetValue = resetValue;
        this.initialBatchStartPointer = startPointer;
        this.state = initialState;
        this.persistedPointers = persistedPointers;
        this.maxPollSize = maxPollSize;
        this.pollTimeout = pollTimeout;
        if (!this.persistedPointers.isEmpty()) {
            maxPersistedPointer = this.persistedPointers.stream().max(IngestionShardPointer::compareTo).get();
        }
        this.blockingQueueContainer = blockingQueueContainer;
        this.consumerThread = Executors.newSingleThreadExecutor(
            r -> new Thread(
                r,
                String.format(Locale.ROOT, "stream-poller-consumer-%d-%d", consumer.getShardId(), System.currentTimeMillis())
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
        blockingQueueContainer.startProcessorThreads();
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

        IngestionShardPointer failedShardPointer = null;
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
                    results = consumer.readNext(initialBatchStartPointer, true, maxPollSize, pollTimeout);
                    includeBatchStartPointer = false;
                } else if (failedShardPointer != null) {
                    results = consumer.readNext(failedShardPointer, true, maxPollSize, pollTimeout);
                    failedShardPointer = null;
                } else {
                    results = consumer.readNext(maxPollSize, pollTimeout);
                }

                if (results.isEmpty()) {
                    // no new records
                    continue;
                }

                state = State.PROCESSING;
                failedShardPointer = processRecords(results);
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

    /**
     * Process records and write to the blocking queue. In case of error, return the shard pointer of the failed message.
     */
    private IngestionShardPointer processRecords(
        List<IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message>> results
    ) {
        IngestionShardPointer failedShardPointer = null;

        for (IngestionShardConsumer.ReadResult<? extends IngestionShardPointer, ? extends Message> result : results) {
            try {
                // check if the message is already processed
                if (isProcessed(result.getPointer())) {
                    logger.debug("Skipping message with pointer {} as it is already processed", () -> result.getPointer().asString());
                    continue;
                }
                totalPolledCount.inc();
                blockingQueueContainer.add(result);
                lastPolledMessageTimestamp = result.getMessage().getTimestamp() == null ? 0 : result.getMessage().getTimestamp();
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
                    failedShardPointer = result.getPointer();
                    break;
                }
            }
        }

        return failedShardPointer;
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
        consumerThread.shutdown();
        blockingQueueContainer.close();
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
     * the poller acts as the start point. If multiple processor threads are used, the minimum shard pointer across
     * processors indicates the start point.
     */
    @Override
    public IngestionShardPointer getBatchStartPointer() {
        return blockingQueueContainer.getCurrentShardPointers()
            .stream()
            .filter(Objects::nonNull)
            .min(Comparator.naturalOrder())
            .orElseGet(() -> initialBatchStartPointer);
    }

    @Override
    public PollingIngestStats getStats() {
        PollingIngestStats.Builder builder = new PollingIngestStats.Builder();
        builder.setTotalPolledCount(totalPolledCount.count());
        builder.setTotalProcessedCount(blockingQueueContainer.getTotalProcessedCount());
        builder.setTotalSkippedCount(blockingQueueContainer.getTotalSkippedCount());
        builder.setLagInMillis(computeLag());
        return builder.build();
    }

    /**
     * Returns the lag in milliseconds since the last polled message
     */
    private long computeLag() {
        return System.currentTimeMillis() - lastPolledMessageTimestamp;
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
        blockingQueueContainer.updateErrorStrategy(errorStrategy);
    }
}
