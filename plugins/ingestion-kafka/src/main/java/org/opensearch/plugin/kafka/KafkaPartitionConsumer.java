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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Kafka consumer to read messages from a Kafka partition
 */
public class KafkaPartitionConsumer implements IngestionShardConsumer<KafkaOffset, KafkaMessage> {
    private static final Logger logger = LogManager.getLogger(KafkaPartitionConsumer.class);

    protected final Consumer<byte[], byte[]> consumer;

    private long lastFetchedOffset = -1;
    final String clientId;
    final TopicPartition topicPartition;

    public KafkaPartitionConsumer(String clientId, KafkaSourceConfig config, int partitionId) {
        // TODO: construct props from config
        Properties consumerProp = new Properties();
        consumerProp.put("bootstrap.servers", config.getBootstrapServers());
        // TODO: why Class org.apache.kafka.common.serialization.StringDeserializer could not be found if set the deserializer as prop?
        // consumerProp.put("key.deserializer",
        //     "org.apache.kafka.common.serialization.StringDeserializer");
        // consumerProp.put("value.deserializer",
        //     "org.apache.kafka.common.serialization.StringDeserializer");
        this.clientId = clientId;
        consumer = new KafkaConsumer<>(consumerProp, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        String topic = config.getTopic();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null) {
            throw new IllegalArgumentException("Topic " + topic + " does not exist");
        }
        if(partitionId>=partitionInfos.size()) {
            throw new IllegalArgumentException("Partition " + partitionId + " does not exist in topic " + topic);
        }
        topicPartition = new TopicPartition(topic, partitionId);
        consumer.assign(Collections.singletonList(topicPartition));
        logger.info("Kafka consumer created for topic {} partition {}", topic, partitionId);
    }

    @Override
    public List<ReadResult<KafkaOffset, KafkaMessage>> readNext(KafkaOffset offset, long maxMessages, int timeoutMillis) throws TimeoutException {

        List<ReadResult<KafkaOffset, KafkaMessage>> records = fetch(offset.getOffset(), maxMessages, timeoutMillis);
        return records;
    }

    @Override
    public KafkaOffset nextPointer() {
        return new KafkaOffset(lastFetchedOffset + 1);
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        long startOffset = consumer.beginningOffsets(Collections.singletonList(topicPartition)).getOrDefault(topicPartition, 0L);
        return new KafkaOffset(startOffset);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        long endOffset = consumer.endOffsets(Collections.singletonList(topicPartition)).getOrDefault(topicPartition, 0L);
        return new KafkaOffset(endOffset);
    }

    protected synchronized List<ReadResult<KafkaOffset, KafkaMessage>> fetch(long startOffset, long maxMessages, int timeoutMillis) {
        if (lastFetchedOffset < 0 || lastFetchedOffset !=startOffset - 1) {
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
            if(currentOffset >= endOffset) {
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
}
