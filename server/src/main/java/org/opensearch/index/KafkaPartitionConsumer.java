/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class KafkaPartitionConsumer implements IngestionShardConsumer<KafkaOffset, KafkaMessage>  {

    protected final Consumer<String, String> consumer;

    private long lastFetchedOffset = -1;
    final String clientId;
    final TopicPartition topicPartition;

    public KafkaPartitionConsumer(String clientId, KafkaSourceConfig config, int partitionId) {
        // TODO: construct props from config
        Properties consumerProp = new Properties();
        consumerProp.put("bootstrap.servers", config.getBootstrapServers());
        consumerProp.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProp.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        this.clientId = clientId;
        consumer = new KafkaConsumer<>(consumerProp);
        String topic = config.getTopic();
        topicPartition = new TopicPartition(topic, partitionId);
        consumer.assign(Collections.singletonList(topicPartition));
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

    protected synchronized List<ReadResult<KafkaOffset, KafkaMessage>> fetch(long startOffset, long maxMessages, int timeoutMillis) {
        if (lastFetchedOffset < 0 || lastFetchedOffset !=startOffset - 1) {
            consumer.seek(topicPartition, startOffset);
        }

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(timeoutMillis));
        List<ConsumerRecord<String, String>> messageAndOffsets = consumerRecords.records(topicPartition);

        long endOffset = startOffset + maxMessages;
        List<ReadResult<KafkaOffset, KafkaMessage>> results = new ArrayList<>();

        for (ConsumerRecord<String, String> messageAndOffset : messageAndOffsets) {
            long currentOffset = messageAndOffset.offset();
            if(currentOffset >= endOffset) {
                // fetched more message than max
                break;
            }
            lastFetchedOffset = currentOffset;
            KafkaOffset kafkaOffset = new KafkaOffset(currentOffset);
            KafkaMessage message = new KafkaMessage(messageAndOffset.key(), messageAndOffset.value(), kafkaOffset);
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