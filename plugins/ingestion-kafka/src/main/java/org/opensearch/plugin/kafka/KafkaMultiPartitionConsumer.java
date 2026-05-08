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
 * Used when {@code source_partition_strategy} assigns multiple source partitions to one shard.
 * <p>
 * Uses composition - holds its own {@link Consumer} instance and implements
 * {@link IngestionShardConsumer} directly. Consumer creation is delegated to
 * {@link KafkaPartitionConsumer#createConsumer(String, KafkaSourceConfig)}.
 */
@SuppressWarnings("removal")
public class KafkaMultiPartitionConsumer implements IngestionShardConsumer<KafkaOffset, KafkaMessage> {

    private static final Logger logger = LogManager.getLogger(KafkaMultiPartitionConsumer.class);

    private final int shardId;
    private final Consumer<byte[], byte[]> consumer;
    private final KafkaSourceConfig config;
    private final List<TopicPartition> assignedPartitions;
    private final Map<Integer, Long> lastFetchedOffsets;

    /**
     * Constructor
     * @param clientId the client ID for this consumer
     * @param config the Kafka source configuration
     * @param shardId the OpenSearch shard ID this consumer belongs to
     * @param partitionIds the list of Kafka partition IDs to consume from
     */
    public KafkaMultiPartitionConsumer(String clientId, KafkaSourceConfig config, int shardId, List<Integer> partitionIds) {
        this(config, shardId, partitionIds, KafkaPartitionConsumer.createConsumer(clientId, config));
    }

    /**
     * Constructor visible for testing
     */
    protected KafkaMultiPartitionConsumer(
        KafkaSourceConfig config,
        int shardId,
        List<Integer> partitionIds,
        Consumer<byte[], byte[]> consumer
    ) {
        this.config = config;
        this.shardId = shardId;
        this.consumer = consumer;
        this.lastFetchedOffsets = new HashMap<>();

        String topic = config.getTopic();
        List<PartitionInfo> partitionInfos = AccessController.doPrivileged(
            (PrivilegedAction<List<PartitionInfo>>) () -> consumer.partitionsFor(
                topic,
                Duration.ofMillis(config.getTopicMetadataFetchTimeoutMs())
            )
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

        this.assignedPartitions = partitionIds.stream().map(p -> new TopicPartition(topic, p)).collect(Collectors.toUnmodifiableList());

        consumer.assign(assignedPartitions);
        logger.info("Kafka multi-partition consumer created for topic {} partitions {} (shard {})", topic, partitionIds, shardId);
    }

    /**
     * Single-partition seek-then-poll semantics don't fit a multi-partition consumer — a single
     * pointer can only reposition one of N partitions, leaving the others in an undefined
     * intermediate state that no caller actually wants. Multi-partition recovery should use
     * {@link #seekToPartitionOffsets(Map)} (covers all assigned partitions atomically) followed
     * by {@link #readNext(long, int)} for continuation polls. To seek a single partition,
     * use {@code seekToPartitionOffsets(Map.of(partition, pointer))}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public List<ReadResult<KafkaOffset, KafkaMessage>> readNext(
        KafkaOffset offset,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) throws TimeoutException {
        throw new UnsupportedOperationException(
            "readNext(offset, ...) has single-partition seek semantics that don't fit a multi-partition "
                + "consumer. Use seekToPartitionOffsets(Map) to seek per-partition checkpoints, then "
                + "readNext(maxMessages, timeoutMillis) for continuation polls. To seek a single partition, "
                + "call seekToPartitionOffsets(Map.of(partition, pointer))."
        );
    }

    /**
     * Continue reading from the current consumer position across all assigned partitions.
     *
     * @param maxMessages  NOT honored — the per-poll batch size is governed by Kafka's
     *                     {@code max.poll.records} consumer config, set at consumer initialization.
     *                     Same limitation as {@link KafkaPartitionConsumer#readNext}.
     * @param timeoutMillis maximum time to wait for messages
     */
    @Override
    public List<ReadResult<KafkaOffset, KafkaMessage>> readNext(long maxMessages, int timeoutMillis) throws TimeoutException {
        return pollAllPartitions(timeoutMillis);
    }

    /**
     * Single {@code consumer.poll()} that returns records across ALL assigned partitions, then
     * iterates by partition to wrap each record with a {@link KafkaPartitionOffset}.
     * <p>
     * <b>{@code max.poll.records} is shared across all assigned partitions.</b>
     * A {@code consumer.poll()} call returns up to {@code max.poll.records} records <em>total</em>,
     * not per partition. With N assigned partitions, each effectively gets ~1/N of the records per
     * poll compared to the equivalent single-partition consumer with the same setting. Users
     * migrating from single-partition to multi-partition mode (e.g., 1 shard now consuming N
     * partitions instead of 1) may need to bump {@code max.poll.records} proportionally to
     * maintain per-partition throughput.
     * <p>
     * {@code synchronized} for parity with {@link KafkaPartitionConsumer}'s internal fetch — Kafka
     * itself does not allow concurrent {@code poll()} calls, but the lock is defense-in-depth for
     * the documented single-poller-thread contract.
     */
    private synchronized List<ReadResult<KafkaOffset, KafkaMessage>> pollAllPartitions(int timeoutMillis) {
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
     * TODO: Promote {@code seekToBeginning()} / {@code seekToEnd()} to default methods on
     * {@link IngestionShardConsumer} when the management API for resets lands. Today, the
     * multi-partition consumer's {@link #earliestPointer()} and {@link #readNext(KafkaOffset, boolean, long, int)}
     * throw {@code UnsupportedOperationException}, so callers cannot use the fixed
     * {@code earliestPointer() + readNext(offset, ...)} pattern that
     * {@code DefaultStreamPoller.getResetShardPointer()} relies on for single-partition mode.
     * The multi-partition reset path must call {@code seekToBeginning()} directly via an
     * {@code instanceof KafkaMultiPartitionConsumer} cast in the poller — promoting these methods
     * to interface defaults would eliminate the cast.
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

    /**
     * Single-pointer earliest is unsupported in multi-partition mode — a single pointer can only
     * represent one of N assigned partitions. For RESET_TO_EARLIEST across all partitions, use
     * {@link #seekToBeginning()} directly. The {@link IngestionShardConsumer} interface predates
     * multi-partition consumption; TODO: Subsequent change should add a {@code Map<Integer, IngestionShardPointer>
     * earliestPointers()} default method.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public IngestionShardPointer earliestPointer() {
        throw new UnsupportedOperationException(
            "earliestPointer() returns a single pointer, which can't represent N assigned partitions in "
                + "multi-partition mode. Use seekToBeginning() directly for RESET_TO_EARLIEST."
        );
    }

    /**
     * Single-pointer latest is unsupported in multi-partition mode — same limitation as
     * {@link #earliestPointer()}. For RESET_TO_LATEST across all partitions, use {@link #seekToEnd()}
     * directly.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public IngestionShardPointer latestPointer() {
        throw new UnsupportedOperationException(
            "latestPointer() returns a single pointer, which can't represent N assigned partitions in "
                + "multi-partition mode. Use seekToEnd() directly for RESET_TO_LATEST."
        );
    }

    /**
     * Single-pointer timestamp resolution is unsupported in multi-partition mode — Kafka's
     * {@code offsetsForTimes} returns per-partition offsets and there's no meaningful single-pointer
     * answer across N partitions (the first non-null partition is non-deterministic and useless for
     * resetting all partitions). Subsequent PR should add a per-partition variant on the interface, e.g.
     * {@code Map<Integer, IngestionShardPointer> pointersFromTimestampMillis(long ts)}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        throw new UnsupportedOperationException(
            "pointerFromTimestampMillis() returns a single pointer, which can't represent N assigned "
                + "partitions in multi-partition mode. RESET_BY_TIMESTAMP needs a per-partition variant — "
                + "tracked in subsequent management API work."
        );
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        if (!offset.contains(":")) {
            // Multi-partition mode requires explicit partition info to avoid silent
            // wrong-partition reads. A bare numeric offset is ambiguous (which of the
            // assigned partitions does it belong to?) — fail loudly so callers fix
            // their input rather than getting data from an arbitrary partition.
            throw new IllegalArgumentException(
                "Multi-partition mode requires offset in 'partition:offset' format (e.g., '3:42'), got: " + offset
            );
        }
        String[] parts = offset.split(":", -1);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid multi-partition pointer format. Expected 'partition:offset' (e.g., '3:42'), got: " + offset
            );
        }
        return new KafkaPartitionOffset(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
    }

    /**
     * Seek all assigned partitions to their respective offsets. Used for recovery with per-partition checkpoints.
     * <p>
     * Entries for partitions not in {@link #assignedPartitions} are logged and skipped — they
     * shouldn't normally appear (assignment is fixed at consumer construction), but a stale
     * checkpoint map from recovery could include them if assignment changes between commits.
     * Logging makes the silent-skip visible for debugging.
     *
     * @param partitionOffsets map of partition ID to a pointer to seek to
     */
    @Override
    public void seekToPartitionOffsets(Map<Integer, ? extends IngestionShardPointer> partitionOffsets) {
        for (Map.Entry<Integer, ? extends IngestionShardPointer> entry : partitionOffsets.entrySet()) {
            TopicPartition tp = new TopicPartition(config.getTopic(), entry.getKey());
            if (assignedPartitions.contains(tp) == false) {
                logger.warn(
                    "seekToPartitionOffsets: skipping partition {} (not assigned to shard {}); assigned partitions are {}",
                    entry.getKey(),
                    shardId,
                    assignedPartitions.stream().map(TopicPartition::partition).toList()
                );
                continue;
            }
            long seekOffset = ((KafkaOffset) entry.getValue()).getOffset();
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                consumer.seek(tp, seekOffset);
                return null;
            });
            lastFetchedOffsets.put(entry.getKey(), seekOffset - 1);
        }
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    /**
     * Computes the total lag across all assigned partitions as the sum of per-partition lags.
     * <p>
     * <b>TODO: takes a single {@code expectedStartPointer} but multi-partition mode needs per-partition
     * starts.</b> Today this method best-effort-handles a {@link KafkaPartitionOffset} for the one
     * partition it references, and falls back to "use endOffset (full lag)" for other partitions
     * with no {@code lastFetched} record. Subsequent PR should add a {@code long getPointerBasedLag(Map<Integer,
     * IngestionShardPointer>)} default method on {@link IngestionShardConsumer} so callers can supply
     * per-partition expected starts. Once that exists, this single-pointer overload can either delegate
     * to the per-partition variant or throw, mirroring the {@link #earliestPointer()} pattern.
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
                        if (startPtr.getSourcePartition() == tp.partition()) {
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
        return assignedPartitions.stream().map(TopicPartition::partition).toList();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
