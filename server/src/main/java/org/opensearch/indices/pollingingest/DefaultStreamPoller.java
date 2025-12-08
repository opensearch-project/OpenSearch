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
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.Nullable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
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

    // flag to indicate if consumer needs to be reinitialized
    private volatile boolean reinitializeConsumer;

    private volatile long lastPolledMessageTimestamp = 0;
    private volatile long cachedPointerBasedLag = 0;
    private volatile long lastPointerBasedLagUpdateTime = 0;

    @Nullable
    private IngestionShardConsumer consumer;
    private IngestionConsumerFactory consumerFactory;
    private String consumerClientId;
    private int shardId;

    private ExecutorService consumerThread;

    // start of the batch, inclusive
    private IngestionShardPointer initialBatchStartPointer;

    private ResetState resetState;
    private final String resetValue;

    private long maxPollSize;
    private int pollTimeout;
    private long pointerBasedLagUpdateIntervalMs;
    private final IngestionMessageMapper messageMapper;

    private final String indexName;

    private final CounterMetric totalPolledCount = new CounterMetric();
    private final CounterMetric totalConsumerErrorCount = new CounterMetric();
    private final CounterMetric totalPollerMessageFailureCount = new CounterMetric();
    // indicates number of messages dropped due to error
    private final CounterMetric totalPollerMessageDroppedCount = new CounterMetric();

    private PartitionedBlockingQueueContainer blockingQueueContainer;

    // Force the consumer to start reading from this pointer. This is used in case of failures, or during initialization/reinitialization.
    private IngestionShardPointer forcedShardPointer = null;

    private DefaultStreamPoller(
        IngestionShardPointer startPointer,
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
        int blockingQueueSize,
        long pointerBasedLagUpdateIntervalMs,
        IngestionMessageMapper.MapperType mapperType
    ) {
        this(
            startPointer,
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
            pointerBasedLagUpdateIntervalMs,
            ingestionEngine.config().getIndexSettings(),
            IngestionMessageMapper.create(mapperType.getName(), shardId)
        );
    }

    /**
     * Visible for testing.
     */
    DefaultStreamPoller(
        IngestionShardPointer startPointer,
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
        long pointerBasedLagUpdateIntervalMs,
        IndexSettings indexSettings,
        IngestionMessageMapper messageMapper
    ) {
        this.consumerFactory = Objects.requireNonNull(consumerFactory);
        this.consumerClientId = Objects.requireNonNull(consumerClientId);
        this.shardId = shardId;
        this.resetState = resetState;
        this.resetValue = resetValue;
        this.initialBatchStartPointer = startPointer;
        this.state = initialState;
        this.maxPollSize = maxPollSize;
        this.pollTimeout = pollTimeout;
        this.pointerBasedLagUpdateIntervalMs = pointerBasedLagUpdateIntervalMs;
        this.blockingQueueContainer = blockingQueueContainer;
        this.consumerThread = Executors.newSingleThreadExecutor(
            r -> new Thread(r, String.format(Locale.ROOT, "stream-poller-consumer-%d-%d", shardId, System.currentTimeMillis()))
        );
        this.errorStrategy = errorStrategy;
        this.indexName = indexSettings.getIndex().getName();
        this.messageMapper = Objects.requireNonNull(messageMapper);

        // handle initial poller states
        this.paused = initialState == State.PAUSED;
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

        while (true) {
            try {
                if (closed) {
                    state = State.CLOSED;
                    closeConsumer();
                    break;
                }

                // Initialize/reinitialization consumer
                if (this.consumer == null || reinitializeConsumer) {
                    handleConsumerInitialization();
                    continue;
                }

                // Update lag periodically. Lag is updated even if the poller is paused.
                updatePointerBasedLagIfNeeded();

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

                // Force the consumer to start from forcedShardPointer if available
                if (forcedShardPointer != null) {
                    results = consumer.readNext(forcedShardPointer, true, maxPollSize, pollTimeout);
                    forcedShardPointer = null;
                } else {
                    results = consumer.readNext(maxPollSize, pollTimeout);
                }

                if (results.isEmpty()) {
                    // no new records
                    setLastPolledMessageTimestamp(0);
                    Thread.sleep(DEFAULT_POLLER_SLEEP_PERIOD_MS);
                    continue;
                }

                state = State.PROCESSING;
                // processRecords returns failed shard pointers. Update forcedShardPointer to the failed pointer to retry on next iteration
                // in case of failures
                forcedShardPointer = processRecords(results);
            } catch (Exception e) {
                // Pause ingestion when an error is encountered while polling the streaming source.
                // Currently we do not have a good way to skip past the failing messages.
                // The user will have the option to manually update the offset and resume ingestion.
                // todo: support retry?
                logger.error("Pausing ingestion. Fatal error occurred in polling the shard {} for index {}: {}", shardId, indexName, e);
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
                totalPolledCount.inc();

                // Use mapper to create ShardUpdateMessage
                ShardUpdateMessage shardUpdateMessage = messageMapper.mapAndProcess(result.getPointer(), result.getMessage());

                blockingQueueContainer.add(shardUpdateMessage);
                setLastPolledMessageTimestamp(result.getMessage().getTimestamp() == null ? 0 : result.getMessage().getTimestamp());
                logger.debug(
                    "Put message {} with pointer {} to the blocking queue",
                    String.valueOf(result.getMessage().getPayload()),
                    result.getPointer().asString()
                );
            } catch (Exception e) {
                logger.error(
                    "[Default Poller] Error processing record. Index={}, Shard={}, pointer={}: error={}",
                    indexName,
                    shardId,
                    result.getPointer().asString(),
                    e
                );
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
        builder.setLagInMillis(computeTimeBasedLag());
        builder.setPointerBasedLag(cachedPointerBasedLag);
        return builder.build();
    }

    /**
     * Returns the lag in milliseconds since the last polled message
     */
    private long computeTimeBasedLag() {
        if (lastPolledMessageTimestamp == 0 || paused) {
            return 0;
        }

        return System.currentTimeMillis() - lastPolledMessageTimestamp;
    }

    private void setLastPolledMessageTimestamp(long timestamp) {
        if (lastPolledMessageTimestamp != timestamp) {
            lastPolledMessageTimestamp = timestamp;
        }
    }

    /**
     * Update the cached pointer-based lag if enough time has elapsed since the last update.
     * {@code consumer.getPointerBasedLag()} is called from the poller thread, so it's safe to access the consumer.
     * If pointerBasedLagUpdateIntervalMs is 0, pointer-based lag calculation is disabled.
     */
    private void updatePointerBasedLagIfNeeded() {
        // If interval is 0, pointer-based lag is disabled
        if (pointerBasedLagUpdateIntervalMs == 0) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        if (consumer != null && (currentTime - lastPointerBasedLagUpdateTime >= pointerBasedLagUpdateIntervalMs)) {
            try {
                // update the lastPointerBasedLagUpdateTime first, to avoid load on streaming source in case of errors
                lastPointerBasedLagUpdateTime = currentTime;
                cachedPointerBasedLag = consumer.getPointerBasedLag(initialBatchStartPointer);
            } catch (Exception e) {
                logger.warn("Failed to update lag for index {} shard {}: {}", indexName, shardId, e.getMessage());
            }
        }
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

    /**
     * Mark the poller's consumer for reinitialization. A new consumer will be initialized and start consuming from the
     * latest batchStartPointer. This method also reinitializes the consumer factory with the updated ingestion source.
     * @param updatedIngestionSource the updated ingestion source with new configuration parameters
     */
    @Override
    public synchronized void requestConsumerReinitialization(IngestionSource updatedIngestionSource) {
        if (closed) {
            logger.warn("Cannot reinitialize consumer for closed poller of shard {}", shardId);
            return;
        }

        // Reinitialize the consumer factory with updated configuration
        consumerFactory.initialize(updatedIngestionSource);
        logger.info("Configuration parameters updated for index {} shard {}, requesting consumer reinitialization", indexName, shardId);
        reinitializeConsumer = true;
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
     * Handles the reset state logic.
     * Returns the resulting IngestionShardPointer for reset or null if no reset required.
     */
    private IngestionShardPointer getResetShardPointer() {
        IngestionShardPointer resetPointer = null;
        if (resetState != ResetState.NONE && consumer != null) {
            switch (resetState) {
                case EARLIEST:
                    resetPointer = consumer.earliestPointer();
                    logger.info("Resetting pointer by seeking to earliest pointer {}", resetPointer.asString());
                    break;
                case LATEST:
                    resetPointer = consumer.latestPointer();
                    logger.info("Resetting pointer by seeking to latest pointer {}", resetPointer.asString());
                    break;
                case RESET_BY_OFFSET:
                    resetPointer = consumer.pointerFromOffset(resetValue);
                    logger.info("Resetting pointer by seeking to pointer {}", resetPointer.asString());
                    break;
                case RESET_BY_TIMESTAMP:
                    resetPointer = consumer.pointerFromTimestampMillis(Long.parseLong(resetValue));
                    logger.info(
                        "Resetting pointer by seeking to timestamp {}, corresponding pointer {}",
                        resetValue,
                        resetPointer.asString()
                    );
                    break;
            }
            resetState = ResetState.NONE;
        }

        return resetPointer;
    }

    /**
     * Handles consumer initialization and reinitialization logic. Closes existing consumer if available and clears the
     * blocking queues before initializing a new consumer. Also forces the consumer to start reading from the initial
     * batchStartPointer if first time initialization, or from the latest available batchStartPointer on reinitialization.
     */
    private void handleConsumerInitialization() {
        closeConsumer();
        blockingQueueContainer.clearAllQueues();
        initializeConsumer();

        // Handle consumer offset reset the first time an index is created. The reset offset takes precedence if available.
        IngestionShardPointer resetShardPointer = getResetShardPointer();
        if (resetShardPointer != null) {
            initialBatchStartPointer = resetShardPointer;
        }

        // Force the consumer to start from the batchStartPointer. This will be the initialBatchStartPointer for first
        // time initialization, or the latest batchStartPointer based on processed messages.
        forcedShardPointer = getBatchStartPointer();
    }

    /**
     * Initializes the consumer using the provided consumerFactory. If an error is encountered during initialization,
     * the poller thread sleeps for the provided duration before retrying/proceeding with the polling loop.
     */
    private synchronized void initializeConsumer() {
        try {
            reinitializeConsumer = false;
            this.consumer = consumerFactory.createShardConsumer(consumerClientId, shardId);
            logger.info("Successfully initialized consumer for shard {}", shardId);
        } catch (Exception e) {
            logger.warn("Failed to create consumer for shard {}: {}", shardId, e.getMessage());
            totalConsumerErrorCount.inc();
            try {
                Thread.sleep(CONSUMER_INIT_RETRY_INTERVAL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Closes the consumer gracefully. This should be called when reinitializing or shutting down the poller.
     */
    private void closeConsumer() {
        if (this.consumer != null) {
            try {
                logger.info("Closing consumer for index {} shard {}", indexName, shardId);
                this.consumer.close();
                this.consumer = null;
            } catch (Exception e) {
                logger.warn("Error closing consumer for index {} shard {}: {}", indexName, shardId, e.getMessage());
            }
        }
    }

    /**
     * Builder for DefaultStreamPoller
     */
    public static class Builder {
        private IngestionShardPointer startPointer;
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
        private long pointerBasedLagUpdateIntervalMs = 10000;
        private IngestionMessageMapper.MapperType mapperType = IngestionMessageMapper.MapperType.DEFAULT;

        /**
         * Initialize the builder with mandatory parameters
         */
        public Builder(
            IngestionShardPointer startPointer,
            IngestionConsumerFactory consumerFactory,
            String consumerClientId,
            int shardId,
            IngestionEngine ingestionEngine
        ) {
            this.startPointer = startPointer;
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
         * Set pointer-based lag update interval in milliseconds
         */
        public Builder pointerBasedLagUpdateInterval(long pointerBasedLagUpdateInterval) {
            this.pointerBasedLagUpdateIntervalMs = pointerBasedLagUpdateInterval;
            return this;
        }

        /**
         * Set mapper type
         */
        public Builder mapperType(IngestionMessageMapper.MapperType mapperType) {
            this.mapperType = mapperType;
            return this;
        }

        /**
         * Build the DefaultStreamPoller instance
         */
        public DefaultStreamPoller build() {
            return new DefaultStreamPoller(
                startPointer,
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
                blockingQueueSize,
                pointerBasedLagUpdateIntervalMs,
                mapperType
            );
        }
    }
}
