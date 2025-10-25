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
import org.opensearch.core.common.Strings;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.Message;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.mapper.IdFieldMapper;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A partitioned blocking queue approach is used to support multiple writer threads. This class holds a blocking queue
 * per partition. A processor thread is started for each partition to consume updates and write to the lucene index.
 * Messages/records for the same document (ID) are mapped to the same partition for sequential processing. If ID is
 * missing, a new one is auto-generated and used for mapping, and can result in the message/record mapped to a different
 * partition on a retry.
 */
public class PartitionedBlockingQueueContainer {
    private static final Logger logger = LogManager.getLogger(PartitionedBlockingQueueContainer.class);
    private final int numPartitions;

    // partition mappings
    private final Map<Integer, BlockingQueue<ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message>>> partitionToQueueMap;
    private final Map<Integer, MessageProcessorRunnable> partitionToMessageProcessorMap;
    private final Map<Integer, ExecutorService> partitionToProcessorExecutorMap;

    /**
     * Initialize partitions and processor threads for given number of partitions.
     */
    public PartitionedBlockingQueueContainer(
        int numPartitions,
        int shardId,
        IngestionEngine ingestionEngine,
        IngestionErrorStrategy errorStrategy,
        int blockingQueueSize
    ) {
        assert numPartitions > 0 : "Number of processor threads / partitions must be greater than 0";
        partitionToQueueMap = new ConcurrentHashMap<>();
        partitionToMessageProcessorMap = new ConcurrentHashMap<>();
        partitionToProcessorExecutorMap = new ConcurrentHashMap<>();
        this.numPartitions = numPartitions;

        logger.info("Initializing processors for shard {} using {} partitions", shardId, numPartitions);
        String processorThreadNamePrefix = String.format(
            Locale.ROOT,
            "stream-poller-processor-shard-%d-%d",
            shardId,
            System.currentTimeMillis()
        );

        for (int partition = 0; partition < numPartitions; partition++) {
            String processorThreadName = String.format(Locale.ROOT, "%s-partition-%d", processorThreadNamePrefix, partition);
            ExecutorService executorService = Executors.newSingleThreadExecutor(
                r -> new Thread(r, String.format(Locale.ROOT, processorThreadName))
            );
            partitionToProcessorExecutorMap.put(partition, executorService);
            partitionToQueueMap.put(partition, new ArrayBlockingQueue<>(blockingQueueSize));

            MessageProcessorRunnable messageProcessorRunnable = new MessageProcessorRunnable(
                partitionToQueueMap.get(partition),
                ingestionEngine,
                errorStrategy
            );
            partitionToMessageProcessorMap.put(partition, messageProcessorRunnable);
        }
    }

    /**
     *  Visible for testing. Initialize a single partition for the provided messageProcessorRunnable.
     */
    PartitionedBlockingQueueContainer(MessageProcessorRunnable messageProcessorRunnable, int shardId) {
        partitionToQueueMap = new ConcurrentHashMap<>();
        partitionToMessageProcessorMap = new ConcurrentHashMap<>();
        partitionToProcessorExecutorMap = new ConcurrentHashMap<>();
        this.numPartitions = 1;

        partitionToQueueMap.put(0, messageProcessorRunnable.getBlockingQueue());
        partitionToMessageProcessorMap.put(0, messageProcessorRunnable);
        ExecutorService executorService = Executors.newSingleThreadExecutor(
            r -> new Thread(
                r,
                String.format(
                    Locale.ROOT,
                    String.format(Locale.ROOT, "stream-poller-processor-shard-%d-%d-partition-0", shardId, System.currentTimeMillis())
                )
            )
        );
        partitionToProcessorExecutorMap.put(0, executorService);
    }

    /**
     * Starts the processor threads to read updates and write to the index.
     */
    public void startProcessorThreads() {
        for (int partition = 0; partition < numPartitions; partition++) {
            ExecutorService executorService = partitionToProcessorExecutorMap.get(partition);
            MessageProcessorRunnable messageProcessorRunnable = partitionToMessageProcessorMap.get(partition);
            executorService.submit(messageProcessorRunnable);
        }
    }

    /**
     * Add a shard update message to the blocking queue. ID of the document will be used to identify the blocking queue partition.
     */
    public void add(ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message> shardUpdateMessage) throws InterruptedException {
        Map<String, Object> payloadMap = shardUpdateMessage.parsedPayloadMap();
        String id = (String) payloadMap.get(IdFieldMapper.NAME);

        int partition = getPartitionFromID(id);
        partitionToQueueMap.get(partition).put(shardUpdateMessage);
    }

    /**
     * Stop the processor threads and shutdown the executors.
     */
    public void close() {
        partitionToMessageProcessorMap.values().forEach(MessageProcessorRunnable::close);
        partitionToProcessorExecutorMap.values().forEach(ExecutorService::shutdown);
        partitionToQueueMap.clear();
        partitionToMessageProcessorMap.clear();
        partitionToProcessorExecutorMap.clear();
    }

    /**
     * Returns aggregated message processor metrics from all processor threads.
     */
    public MessageProcessorRunnable.MessageProcessorMetrics getMessageProcessorMetrics() {
        return partitionToMessageProcessorMap.values()
            .stream()
            .map(MessageProcessorRunnable::getMessageProcessorMetrics)
            .reduce(MessageProcessorRunnable.MessageProcessorMetrics::combine)
            .orElseGet(MessageProcessorRunnable.MessageProcessorMetrics::create);
    }

    /**
     * Update error strategy in all available message processors.
     */
    public void updateErrorStrategy(IngestionErrorStrategy errorStrategy) {
        partitionToMessageProcessorMap.values().forEach(messageProcessor -> messageProcessor.setErrorStrategy(errorStrategy));
    }

    /**
     * Returns the current shard pointers from each message processor thread.
     */
    public List<IngestionShardPointer> getCurrentShardPointers() {
        return partitionToMessageProcessorMap.values().stream().map(MessageProcessorRunnable::getCurrentShardPointer).toList();
    }

    private int getPartitionFromID(String id) {
        if (Strings.isEmpty(id)) {
            return 0;
        }
        return Math.floorMod(id.hashCode(), numPartitions);
    }

    Map<Integer, MessageProcessorRunnable> getPartitionToMessageProcessorMap() {
        return partitionToMessageProcessorMap;
    }

    Map<Integer, ExecutorService> getPartitionToProcessorExecutorMap() {
        return partitionToProcessorExecutorMap;
    }

    Map<Integer, BlockingQueue<ShardUpdateMessage<? extends IngestionShardPointer, ? extends Message>>> getPartitionToQueueMap() {
        return partitionToQueueMap;
    }
}
