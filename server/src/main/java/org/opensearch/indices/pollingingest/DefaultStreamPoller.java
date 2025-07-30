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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.common.Nullable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IngestionConsumerFactory;
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
    private static final int DEFAULT_POLLER_SLEEP_PERIOD_MS = 100;
    private static final int CONSUMER_INIT_RETRY_INTERVAL_MS = 10000;

    private volatile State state = State.NONE;

    // goal state
    private volatile boolean started;
    private volatile boolean closed;
    private volatile boolean paused;
    private volatile IngestionErrorStrategy errorStrategy;

    // indicates if a local or global cluster write block is in effect
    private volatile boolean isWriteBlockEnabled;

    private volatile long lastPolledMessageTimestamp = 0;

    @Nullable
    private IngestionShardConsumer consumer;
    private IngestionConsumerFactory consumerFactory;
    private String consumerClientId;
    private int shardId;

    private ExecutorService consumerThread;

    // start of the batch, inclusive
    private IngestionShardPointer initialBatchStartPointer;
    private boolean includeBatchStartPointer = false;

    private ResetState resetState;
    private final String resetValue;

    private long maxPollSize;
    private int pollTimeout;

    private Set<IngestionShardPointer> persistedPointers;
    private final String indexName;

    private final CounterMetric totalPolledCount = new CounterMetric();
    private final CounterMetric totalConsumerErrorCount = new CounterMetric();
    private final CounterMetric totalPollerMessageFailureCount = new CounterMetric();
    // indicates number of messages dropped due to error
    private final CounterMetric totalPollerMessageDroppedCount = new CounterMetric();
    // indicates number of duplicate messages that are already processed, and hence skipped
    private final CounterMetric totalDuplicateMessageSkippedCount = new CounterMetric();

    // A pointer to the max persisted pointer for optimizing the check
    @Nullable
    private IngestionShardPointer maxPersistedPointer;

    private PartitionedBlockingQueueContainer blockingQueueContainer;

    public DefaultStreamPoller(
        IngestionShardPointer startPointer,
        Set<IngestionShardPointer> persistedPointers,
        IngestionConsumerFactory consumerFactory,
        String consumerClientId,
        int shardId,
        IngestionEngine ingestionEngine,
        ResetState resetState,
        String resetValue,
        IngestionErrorStrategy errorStrategy,
        State initialState,
        long maxPollSize,
        int pollTimeout,
        int numProcessorThreads,
        int blockingQueueSize
    ) {
        this(
            startPointer,
            persistedPointers,
            consumerFactory,
            consumerClientId,
            shardId,
            new PartitionedBlockingQueueContainer(numProcessorThreads, shardId, ingestionEngine, errorStrategy, blockingQueueSize),
            resetState,
            resetValue,
            errorStrategy,
            initialState,
            maxPollSize,
            pollTimeout,
            ingestionEngine.config().getIndexSettings()
        );
    }

    /**
     * Visible for testing.
     */
    DefaultStreamPoller(
        IngestionShardPointer startPointer,
        Set<IngestionShardPointer> persistedPointers,
        IngestionConsumerFactory consumerFactory,
        String consumerClientId,
        int shardId,
        PartitionedBlockingQueueContainer blockingQueueContainer,
        ResetState resetState,
        String resetValue,
        IngestionErrorStrategy errorStrategy,
        State initialState,
        long maxPollSize,
        int pollTimeout,
        IndexSettings indexSettings
    ) {
        this.consumerFactory = Objects.requireNonNull(consumerFactory);
        this.consumerClientId = Objects.requireNonNull(consumerClientId);
        this.shardId = shardId;
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
            r -> new Thread(r, String.format(Locale.ROOT, "stream-poller-consumer-%d-%d", shardId, System.currentTimeMillis()))
        );
        this.errorStrategy = errorStrategy;
        this.indexName = indexSettings.getIndex().getName();

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
        logger.info("Starting poller for shard {}", shardId);

        IngestionShardPointer failedShardPointer = null;
        while (true) {
            try {
                if (closed) {
                    state = State.CLOSED;
                    break;
                }

                // Initialize consumer if not already initialized
                if (this.consumer == null) {
                    initializeConsumer(CONSUMER_INIT_RETRY_INTERVAL_MS);
                    continue;
                }

                // reset the consumer offset
                handleResetState();

                if (paused || isWriteBlockEnabled) {
                    state = State.PAUSED;
                    try {
                        Thread.sleep(DEFAULT_POLLER_SLEEP_PERIOD_MS);
                    } catch (Throwable e) {
                        logger.error("Error in pausing the poller of shard {}: {}", shardId, e);
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
                logger.error("Pausing ingestion. Fatal error occurred in polling the shard {}: {}", shardId, e);
                totalConsumerErrorCount.inc();
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
                    logger.debug("Skipping message with pointer {} as it is already processed", result.getPointer().asString());
                    totalDuplicateMessageSkippedCount.inc();
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
                logger.error("Error in processing a record. Shard {}, pointer {}: {}", shardId, result.getPointer().asString(), e);
                errorStrategy.handleError(e, IngestionErrorStrategy.ErrorStage.POLLING);
                totalPollerMessageFailureCount.inc();

                if (errorStrategy.shouldIgnoreError(e, IngestionErrorStrategy.ErrorStage.POLLING) == false) {
                    // Blocking error encountered. Pause poller to stop processing remaining updates.
                    pause();
                    failedShardPointer = result.getPointer();
                    break;
                } else {
                    totalPollerMessageDroppedCount.inc();
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
                logger.error("Timeout reached while waiting for shard {} to close", shardId);
                break; // Exit the loop if the timeout is reached
            }
            try {
                Thread.sleep(100);
            } catch (Throwable e) {
                logger.error("Error in closing the poller of shard {}: {}", shardId, e);
            }
        }
        consumerThread.shutdown();
        blockingQueueContainer.close();
        logger.info("closed the poller of shard {}", shardId);
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
        MessageProcessorRunnable.MessageProcessorMetrics processorMetrics = blockingQueueContainer.getMessageProcessorMetrics();
        PollingIngestStats.Builder builder = new PollingIngestStats.Builder();
        // set processor stats
        builder.setTotalProcessedCount(processorMetrics.processedCounter().count());
        builder.setTotalInvalidMessageCount(processorMetrics.invalidMessageCounter().count());
        builder.setTotalProcessorVersionConflictsCount(processorMetrics.versionConflictCounter().count());
        builder.setTotalProcessorFailedCount(processorMetrics.failedMessageCounter().count());
        builder.setTotalProcessorFailuresDroppedCount(processorMetrics.failedMessageDroppedCounter().count());
        builder.setTotalProcessorThreadInterruptCount(processorMetrics.processorThreadInterruptCounter().count());
        // set consumer stats
        builder.setTotalPolledCount(totalPolledCount.count());
        builder.setTotalConsumerErrorCount(totalConsumerErrorCount.count());
        builder.setTotalPollerMessageFailureCount(totalPollerMessageFailureCount.count());
        builder.setTotalPollerMessageDroppedCount(totalPollerMessageDroppedCount.count());
        builder.setTotalDuplicateMessageSkippedCount(totalDuplicateMessageSkippedCount.count());
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

    @Override
    public boolean isWriteBlockEnabled() {
        return isWriteBlockEnabled;
    }

    @Override
    public void setWriteBlockEnabled(boolean isWriteBlockEnabled) {
        this.isWriteBlockEnabled = isWriteBlockEnabled;
    }

    @Nullable
    @Override
    public IngestionShardConsumer getConsumer() {
        return consumer;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            if (event.blocksChanged() == false) {
                return;
            }

            final ClusterState state = event.state();
            isWriteBlockEnabled = state.blocks().indexBlocked(ClusterBlockLevel.WRITE, indexName);

        } catch (Exception e) {
            logger.error("Error applying cluster state in stream poller", e);
            throw e;
        }
    }

    /**
     * Handles the reset state logic, updating the initialBatchStartPointer based on the reset state.
     */
    private void handleResetState() {
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
                case RESET_BY_OFFSET:
                    initialBatchStartPointer = consumer.pointerFromOffset(resetValue);
                    logger.info("Resetting offset by seeking to offset {}", initialBatchStartPointer.asString());
                    break;
                case RESET_BY_TIMESTAMP:
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
    }

    /**
     * Initializes the consumer using the provided consumerFactory. If an error is encountered during initialization,
     * the poller thread sleeps for the provided duration before retrying/proceeding with the polling loop.
     */
    private void initializeConsumer(int sleepDurationOnError) {
        try {
            this.consumer = consumerFactory.createShardConsumer(consumerClientId, shardId);
            logger.info("Successfully initialized consumer for shard {}", shardId);
        } catch (Exception e) {
            logger.warn("Failed to create consumer for shard {}: {}", shardId, e.getMessage());
            totalConsumerErrorCount.inc();
            try {
                Thread.sleep(sleepDurationOnError);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Builder for DefaultStreamPoller
     */
    public static class Builder {
        private IngestionShardPointer startPointer;
        private Set<IngestionShardPointer> persistedPointers;
        private IngestionConsumerFactory consumerFactory;
        private String consumerClientId;
        private int shardId;
        private IngestionEngine ingestionEngine;
        private ResetState resetState = ResetState.LATEST;
        private String resetValue = "";
        private IngestionErrorStrategy errorStrategy;
        private State initialState = State.NONE;
        private long maxPollSize = 1000;
        private int pollTimeout = 1000;
        private int numProcessorThreads = 1;
        private int blockingQueueSize = 100;

        /**
         * Initialize the builder with mandatory parameters
         */
        public Builder(
            IngestionShardPointer startPointer,
            Set<IngestionShardPointer> persistedPointers,
            IngestionConsumerFactory consumerFactory,
            String consumerClientId,
            int shardId,
            IngestionEngine ingestionEngine
        ) {
            this.startPointer = startPointer;
            this.persistedPointers = Objects.requireNonNull(persistedPointers);
            this.consumerFactory = Objects.requireNonNull(consumerFactory);
            this.consumerClientId = Objects.requireNonNull(consumerClientId);
            this.shardId = shardId;
            this.ingestionEngine = Objects.requireNonNull(ingestionEngine);
            this.errorStrategy = new DropIngestionErrorStrategy("poller");
        }

        /**
         * Set error strategy
         */
        public Builder errorStrategy(IngestionErrorStrategy errorStrategy) {
            this.errorStrategy = Objects.requireNonNull(errorStrategy);
            return this;
        }

        /**
         * Set reset state
         */
        public Builder resetState(ResetState resetState) {
            this.resetState = resetState;
            return this;
        }

        /**
         * Set reset value
         */
        public Builder resetValue(String resetValue) {
            this.resetValue = resetValue;
            return this;
        }

        /**
         * Set initial state
         */
        public Builder initialState(State initialState) {
            this.initialState = initialState;
            return this;
        }

        /**
         * Set max poll size
         */
        public Builder maxPollSize(long maxPollSize) {
            this.maxPollSize = maxPollSize;
            return this;
        }

        /**
         * Set poll timeout
         */
        public Builder pollTimeout(int pollTimeout) {
            this.pollTimeout = pollTimeout;
            return this;
        }

        /**
         * Set number of processor threads
         */
        public Builder numProcessorThreads(int numProcessorThreads) {
            this.numProcessorThreads = numProcessorThreads;
            return this;
        }

        /**
         * Set blocking queue size
         */
        public Builder blockingQueueSize(int blockingQueueSize) {
            this.blockingQueueSize = blockingQueueSize;
            return this;
        }

        /**
         * Build the DefaultStreamPoller instance
         */
        public DefaultStreamPoller build() {
            return new DefaultStreamPoller(
                startPointer,
                persistedPointers,
                consumerFactory,
                consumerClientId,
                shardId,
                ingestionEngine,
                resetState,
                resetValue,
                errorStrategy,
                initialState,
                maxPollSize,
                pollTimeout,
                numProcessorThreads,
                blockingQueueSize
            );
        }
    }
}
