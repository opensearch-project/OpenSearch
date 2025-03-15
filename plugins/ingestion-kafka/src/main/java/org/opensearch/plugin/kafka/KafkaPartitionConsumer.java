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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Kafka consumer to read messages from a Kafka partition
 */
@SuppressWarnings("removal")
public class KafkaPartitionConsumer implements IngestionShardConsumer<KafkaOffset, KafkaMessage> {
    private static final Logger logger = LogManager.getLogger(KafkaPartitionConsumer.class);

    /**
     * The Kafka consumer
     */
    protected final Consumer<byte[], byte[]> consumer;
    // TODO: make this configurable
    private final int timeoutMillis = 1000;

    private long lastFetchedOffset = -1;
    final String clientId;
    final TopicPartition topicPartition;
    final KafkaSourceConfig config;

    /**
     * Constructor
     * @param clientId the client id
     * @param config   the Kafka source config
     * @param partitionId the partition id
     */
    public KafkaPartitionConsumer(String clientId, KafkaSourceConfig config, int partitionId) {
        this(clientId, config, partitionId, createConsumer(clientId, config));
    }

    /**
     * Constructor, visible for testing
     * @param clientId the client id
     * @param config the Kafka source config
     * @param partitionId the partition id
     * @param consumer the created Kafka consumer
     */
    protected KafkaPartitionConsumer(String clientId, KafkaSourceConfig config, int partitionId, Consumer<byte[], byte[]> consumer) {
        this.clientId = clientId;
        this.consumer = consumer;
        this.config = config;
        String topic = config.getTopic();
        List<PartitionInfo> partitionInfos = AccessController.doPrivileged(
            (PrivilegedAction<List<PartitionInfo>>) () -> consumer.partitionsFor(topic, Duration.ofMillis(timeoutMillis))
        );
        if (partitionInfos == null) {
            throw new IllegalArgumentException("Topic " + topic + " does not exist");
        }
        if (partitionId >= partitionInfos.size()) {
            throw new IllegalArgumentException("Partition " + partitionId + " does not exist in topic " + topic);
        }
        topicPartition = new TopicPartition(topic, partitionId);
        consumer.assign(Collections.singletonList(topicPartition));
        logger.info("Kafka consumer created for topic {} partition {}", topic, partitionId);
    }

    /**
     * Create a Kafka consumer. visible for testing
     * @param clientId the client id
     * @param config the Kafka source config
     * @return the Kafka consumer
     */
    protected static Consumer<byte[], byte[]> createConsumer(String clientId, KafkaSourceConfig config) {
        Properties consumerProp = new Properties();
        consumerProp.put("bootstrap.servers", config.getBootstrapServers());
        consumerProp.put("client.id", clientId);

        logger.info("Kafka consumer properties for topic {}: {}", config.getTopic(), config.getConsumerConfigurations());
        consumerProp.putAll(config.getConsumerConfigurations());

        // TODO: why Class org.apache.kafka.common.serialization.StringDeserializer could not be found if set the deserializer as prop?
        // consumerProp.put("key.deserializer",
        // "org.apache.kafka.common.serialization.StringDeserializer");
        // consumerProp.put("value.deserializer",
        // "org.apache.kafka.common.serialization.StringDeserializer");
        //
        // wrap the kafka consumer creation in a privileged block to apply plugin security policies
        return AccessController.doPrivileged(
            (PrivilegedAction<Consumer<byte[], byte[]>>) () -> new KafkaConsumer<>(
                consumerProp,
                new ByteArrayDeserializer(),
                new ByteArrayDeserializer()
            )
        );
    }

    @Override
    public List<ReadResult<KafkaOffset, KafkaMessage>> readNext(KafkaOffset offset, long maxMessages, int timeoutMillis)
        throws TimeoutException {
        List<ReadResult<KafkaOffset, KafkaMessage>> records = AccessController.doPrivileged(
            (PrivilegedAction<List<ReadResult<KafkaOffset, KafkaMessage>>>) () -> fetch(offset.getOffset(), maxMessages, timeoutMillis)
        );
        return records;
    }

    @Override
    public KafkaOffset nextPointer() {
        return new KafkaOffset(lastFetchedOffset + 1);
    }

    @Override
    public KafkaOffset nextPointer(KafkaOffset pointer) {
        return new KafkaOffset(pointer.getOffset() + 1);
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        long startOffset = AccessController.doPrivileged(
            (PrivilegedAction<Long>) () -> consumer.beginningOffsets(Collections.singletonList(topicPartition))
                .getOrDefault(topicPartition, 0L)
        );
        return new KafkaOffset(startOffset);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        long endOffset = AccessController.doPrivileged(
            (PrivilegedAction<Long>) () -> consumer.endOffsets(Collections.singletonList(topicPartition)).getOrDefault(topicPartition, 0L)
        );
        return new KafkaOffset(endOffset);
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        long offset = AccessController.doPrivileged((PrivilegedAction<Long>) () -> {
            Map<TopicPartition, OffsetAndTimestamp> position = consumer.offsetsForTimes(
                Collections.singletonMap(topicPartition, timestampMillis)
            );
            if (position == null || position.isEmpty()) {
                return -1L;
            }
            OffsetAndTimestamp offsetAndTimestamp = position.values().iterator().next();
            if (offsetAndTimestamp == null) {
                return -1L;
            }
            return offsetAndTimestamp.offset();
        });
        if (offset < 0) {
            logger.warn("No message found for timestamp {}, fall back to auto.offset.reset policy", timestampMillis);
            String autoOffsetResetConfig = config.getAutoOffsetResetConfig();
            if (OffsetResetStrategy.EARLIEST.toString().equals(autoOffsetResetConfig)) {
                logger.warn("The auto.offset.reset is set to earliest, seek to earliest pointer");
                return earliestPointer();
            } else if (OffsetResetStrategy.LATEST.toString().equals(autoOffsetResetConfig)) {
                logger.warn("The auto.offset.reset is set to latest, seek to latest pointer");
                return latestPointer();
            } else {
                throw new IllegalArgumentException("No message found for timestamp " + timestampMillis);
            }
        }
        return new KafkaOffset(offset);
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        long offsetValue = Long.parseLong(offset);
        return new KafkaOffset(offsetValue);
    }

    private synchronized List<ReadResult<KafkaOffset, KafkaMessage>> fetch(long startOffset, long maxMessages, int timeoutMillis) {
        if (lastFetchedOffset < 0 || lastFetchedOffset != startOffset - 1) {
            logger.info("Seeking to offset {}", startOffset);
            consumer.seek(topicPartition, startOffset);
            // update the last fetched offset so that we don't need to seek again if no more messages to fetch
            lastFetchedOffset = startOffset - 1;
        }

        ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(timeoutMillis));
        List<ConsumerRecord<byte[], byte[]>> messageAndOffsets = consumerRecords.records(topicPartition);

        long endOffset = startOffset + maxMessages;
        List<ReadResult<KafkaOffset, KafkaMessage>> results = new ArrayList<>();

        for (ConsumerRecord<byte[], byte[]> messageAndOffset : messageAndOffsets) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset >= endOffset) {
                // fetched more message than max
                break;
            }
            lastFetchedOffset = currentOffset;
            KafkaOffset kafkaOffset = new KafkaOffset(currentOffset);
            KafkaMessage message = new KafkaMessage(messageAndOffset.key(), messageAndOffset.value());
            results.add(new ReadResult<>(kafkaOffset, message));
        }
        return results;
    }

    @Override
    public int getShardId() {
        return topicPartition.partition();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    /**
     * Get the client id
     * @return the client id
     */
    public String getClientId() {
        return clientId;
    }
}
