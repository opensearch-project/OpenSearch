/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Kafka consumer that reads from multiple partitions for a single OpenSearch shard.
 * Used when {@code partition_strategy=auto} assigns multiple source partitions to one shard.
 * <p>
 * Uses composition (not inheritance) — holds its own {@link Consumer} instance and implements
 * {@link IngestionShardConsumer} directly. Consumer creation is delegated to
 * {@link KafkaPartitionConsumer#createConsumer(String, KafkaSourceConfig)}.
 */
@SuppressWarnings("removal")
public class KafkaMultiPartitionConsumer implements IngestionShardConsumer<KafkaOffset, KafkaMessage> {

    private static final Logger logger = LogManager.getLogger(KafkaMultiPartitionConsumer.class);

    private final int shardId;
    private final String clientId;
    private final Consumer<byte[], byte[]> consumer;
    private final KafkaSourceConfig config;
    private final List<TopicPartition> assignedPartitions;
    private final Map<Integer, Long> lastFetchedOffsets;
    // TODO: make this configurable
    private final int defaultTimeoutMillis = 1000;

    /**
     * Constructor
     * @param clientId the client ID for this consumer
     * @param config the Kafka source configuration
     * @param shardId the OpenSearch shard ID this consumer belongs to
     * @param partitionIds the list of Kafka partition IDs to consume from
     */
    public KafkaMultiPartitionConsumer(String clientId, KafkaSourceConfig config, int shardId, List<Integer> partitionIds) {
        this(clientId, config, shardId, partitionIds, KafkaPartitionConsumer.createConsumer(clientId, config));
    }

    /**
     * Constructor visible for testing
     */
    protected KafkaMultiPartitionConsumer(
        String clientId,
        KafkaSourceConfig config,
        int shardId,
        List<Integer> partitionIds,
        Consumer<byte[], byte[]> consumer
    ) {
        this.clientId = clientId;
        this.config = config;
        this.shardId = shardId;
        this.consumer = consumer;
        this.lastFetchedOffsets = new HashMap<>();

        String topic = config.getTopic();
        List<PartitionInfo> partitionInfos = AccessController.doPrivileged(
            (PrivilegedAction<List<PartitionInfo>>) () -> consumer.partitionsFor(topic, Duration.ofMillis(defaultTimeoutMillis))
        );
        if (partitionInfos == null) {
            throw new IllegalArgumentException("Topic " + topic + " does not exist");
        }
        int maxPartition = partitionInfos.size();
        for (int partitionId : partitionIds) {
            if (partitionId >= maxPartition) {
                throw new IllegalArgumentException(
                    "Partition " + partitionId + " does not exist in topic " + topic + " (max: " + (maxPartition - 1) + ")"
                );
            }
        }

        this.assignedPartitions = partitionIds.stream()
            .map(p -> new TopicPartition(topic, p))
            .collect(Collectors.toUnmodifiableList());

        consumer.assign(assignedPartitions);
        logger.info("Kafka multi-partition consumer created for topic {} partitions {} (shard {})", topic, partitionIds, shardId);
    }

    @Override
    public List<ReadResult<KafkaOffset, KafkaMessage>> readNext(
        KafkaOffset offset,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        if (offset instanceof KafkaPartitionOffset) {
            KafkaPartitionOffset partitionOffset = (KafkaPartitionOffset) offset;
            TopicPartition tp = new TopicPartition(config.getTopic(), partitionOffset.getPartition());
            long seekOffset = includeStart ? partitionOffset.getOffset() : partitionOffset.getOffset() + 1;
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                consumer.seek(tp, seekOffset);
                return null;
            });
            lastFetchedOffsets.put(partitionOffset.getPartition(), seekOffset - 1);
        } else {
            // Legacy pointer without partition — seek first assigned partition
            TopicPartition tp = assignedPartitions.get(0);
            long seekOffset = includeStart ? offset.getOffset() : offset.getOffset() + 1;
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                consumer.seek(tp, seekOffset);
                return null;
            });
            lastFetchedOffsets.put(tp.partition(), seekOffset - 1);
        }
        return pollAllPartitions(timeoutMillis);
    }

    @Override
    public List<ReadResult<KafkaOffset, KafkaMessage>> readNext(long maxMessages, int timeoutMillis) throws TimeoutException {
        return pollAllPartitions(timeoutMillis);
    }

    // Not thread-safe — must be called from the poller thread only, consistent with KafkaPartitionConsumer contract.
    private List<ReadResult<KafkaOffset, KafkaMessage>> pollAllPartitions(int timeoutMillis) {
        ConsumerRecords<byte[], byte[]> consumerRecords = AccessController.doPrivileged(
            (PrivilegedAction<ConsumerRecords<byte[], byte[]>>) () -> consumer.poll(Duration.ofMillis(timeoutMillis))
        );

        List<ReadResult<KafkaOffset, KafkaMessage>> results = new ArrayList<>();
        for (TopicPartition tp : assignedPartitions) {
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords.records(tp)) {
                int partition = record.partition();
                long currentOffset = record.offset();
                lastFetchedOffsets.put(partition, currentOffset);
                KafkaPartitionOffset pointer = new KafkaPartitionOffset(partition, currentOffset);
                KafkaMessage message = new KafkaMessage(record.key(), record.value(), record.timestamp());
                results.add(new ReadResult<>(pointer, message));
            }
        }
        return results;
    }

    /**
     * Seeks all assigned partitions to the beginning. Used for RESET_TO_EARLIEST.
     * <p>
     * TODO: Add seekToBeginning()/seekToEnd() as default methods on IngestionShardConsumer interface
     * so the poller can call them without instanceof checks. The poller's reset flow
     * (DefaultStreamPoller.getResetShardPointer()) currently calls earliestPointer() + readNext()
     * which only seeks one partition. The poller PR needs to use these methods for proper
     * multi-partition reset.
     */
    public void seekToBeginning() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            consumer.seekToBeginning(assignedPartitions);
            return null;
        });
        lastFetchedOffsets.clear();
    }

    /**
     * Seeks all assigned partitions to the end. Used for RESET_TO_LATEST.
     */
    public void seekToEnd() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            consumer.seekToEnd(assignedPartitions);
            return null;
        });
        lastFetchedOffsets.clear();
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        Map<TopicPartition, Long> beginnings = AccessController.doPrivileged(
            (PrivilegedAction<Map<TopicPartition, Long>>) () -> consumer.beginningOffsets(assignedPartitions)
        );
        // TODO: This returns a single pointer for only the first partition. Callers using this for
        // RESET_TO_EARLIEST will only seek one partition. The poller PR must use seekToBeginning()
        // for proper multi-partition reset instead of earliestPointer() + readNext().
        TopicPartition first = assignedPartitions.get(0);
        return new KafkaPartitionOffset(first.partition(), beginnings.getOrDefault(first, 0L));
    }

    @Override
    public IngestionShardPointer latestPointer() {
        Map<TopicPartition, Long> endings = AccessController.doPrivileged(
            (PrivilegedAction<Map<TopicPartition, Long>>) () -> consumer.endOffsets(assignedPartitions)
        );
        // TODO: Same limitation as earliestPointer() — returns single pointer for last partition only.
        // For multi-partition reset, use seekToEnd() instead.
        TopicPartition last = assignedPartitions.get(assignedPartitions.size() - 1);
        return new KafkaPartitionOffset(last.partition(), endings.getOrDefault(last, 0L));
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        Map<TopicPartition, Long> timestamps = new HashMap<>();
        for (TopicPartition tp : assignedPartitions) {
            timestamps.put(tp, timestampMillis);
        }

        Map<TopicPartition, OffsetAndTimestamp> offsets = AccessController.doPrivileged(
            (PrivilegedAction<Map<TopicPartition, OffsetAndTimestamp>>) () -> consumer.offsetsForTimes(timestamps)
        );

        // Return the first partition offset found for the timestamp
        for (TopicPartition tp : assignedPartitions) {
            OffsetAndTimestamp oat = offsets.get(tp);
            if (oat != null) {
                return new KafkaPartitionOffset(tp.partition(), oat.offset());
            }
        }

        // Fallback to auto.offset.reset policy
        String autoOffsetResetConfig = config.getAutoOffsetResetConfig();
        if (OffsetResetStrategy.EARLIEST.toString().equals(autoOffsetResetConfig)) {
            seekToBeginning();
            return earliestPointer();
        } else if (OffsetResetStrategy.LATEST.toString().equals(autoOffsetResetConfig)) {
            seekToEnd();
            return latestPointer();
        }
        throw new IllegalArgumentException("No message found for timestamp " + timestampMillis + " across any assigned partition");
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        if (offset.contains(":")) {
            String[] parts = offset.split(":");
            return new KafkaPartitionOffset(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
        }
        // Fallback: treat as plain offset for first assigned partition
        long offsetValue = Long.parseLong(offset);
        int firstPartition = assignedPartitions.get(0).partition();
        return new KafkaPartitionOffset(firstPartition, offsetValue);
    }

    /**
     * Seek all assigned partitions to their respective offsets. Used for recovery with per-partition checkpoints.
     * @param partitionOffsets map of partition ID to pointer to seek to
     */
    @Override
    public void seekToPartitionOffsets(Map<Integer, ? extends IngestionShardPointer> partitionOffsets) {
        for (Map.Entry<Integer, ? extends IngestionShardPointer> entry : partitionOffsets.entrySet()) {
            TopicPartition tp = new TopicPartition(config.getTopic(), entry.getKey());
            if (assignedPartitions.contains(tp)) {
                long seekOffset = ((KafkaOffset) entry.getValue()).getOffset();
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    consumer.seek(tp, seekOffset);
                    return null;
                });
                lastFetchedOffsets.put(entry.getKey(), seekOffset - 1);
            }
        }
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    /**
     * Computes the total lag across all assigned partitions as the sum of per-partition lags.
     */
    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        try {
            Map<TopicPartition, Long> endOffsets = AccessController.doPrivileged(
                (PrivilegedAction<Map<TopicPartition, Long>>) () -> consumer.endOffsets(assignedPartitions)
            );

            long totalLag = 0;
            for (TopicPartition tp : assignedPartitions) {
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                Long lastFetched = lastFetchedOffsets.get(tp.partition());
                if (lastFetched == null || lastFetched < 0) {
                    if (expectedStartPointer instanceof KafkaPartitionOffset) {
                        KafkaPartitionOffset startPtr = (KafkaPartitionOffset) expectedStartPointer;
                        if (startPtr.getPartition() == tp.partition()) {
                            totalLag += Math.max(0, endOffset - startPtr.getOffset());
                            continue;
                        }
                    }
                    totalLag += endOffset;
                } else {
                    totalLag += Math.max(0, endOffset - lastFetched - 1);
                }
            }
            return totalLag;
        } catch (Exception e) {
            logger.warn("Failed to calculate pointer based lag for shard {}: {}", shardId, e.getMessage());
            return -1;
        }
    }

    /**
     * Returns the list of assigned partition IDs.
     */
    public List<Integer> getAssignedPartitionIds() {
        return assignedPartitions.stream().map(TopicPartition::partition).collect(Collectors.toUnmodifiableList());
    }

    public String getClientId() {
        return clientId;
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
