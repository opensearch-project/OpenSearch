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
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaPartitionConsumerTests extends OpenSearchTestCase {

    private KafkaSourceConfig config;
    private KafkaConsumer<byte[], byte[]> mockConsumer;
    private KafkaPartitionConsumer consumer;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "test-topic");
        params.put("bootstrap_servers", "localhost:9092");

        config = new KafkaSourceConfig(1000, params);
        mockConsumer = mock(KafkaConsumer.class);
        // Mock the partitionsFor method
        PartitionInfo partitionInfo = new PartitionInfo("test-topic", 0, null, null, null);
        when(mockConsumer.partitionsFor(eq("test-topic"), any(Duration.class))).thenReturn(Collections.singletonList(partitionInfo));
        consumer = new KafkaPartitionConsumer("client1", config, 0, mockConsumer);
    }

    public void testReadNext() throws Exception {
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 0, null, "message".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(topicPartition, Collections.singletonList(record))
        );

        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);

        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> result = consumer.readNext(new KafkaOffset(0), true, 10, 1000);

        assertEquals(1, result.size());
        assertEquals("message", new String(result.get(0).getMessage().getPayload(), StandardCharsets.UTF_8));
        assertEquals(0, consumer.getShardId());
        assertEquals("client1", consumer.getClientId());
    }

    public void testEarliestPointer() {
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        when(mockConsumer.beginningOffsets(Collections.singletonList(topicPartition))).thenReturn(
            Collections.singletonMap(topicPartition, 0L)
        );

        KafkaOffset offset = (KafkaOffset) consumer.earliestPointer();

        assertEquals(0L, offset.getOffset());
    }

    public void testLatestPointer() {
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        when(mockConsumer.endOffsets(Collections.singletonList(topicPartition))).thenReturn(Collections.singletonMap(topicPartition, 10L));

        KafkaOffset offset = (KafkaOffset) consumer.latestPointer();

        assertEquals(10L, offset.getOffset());
    }

    public void testPointerFromTimestampMillis() {
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        when(mockConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, 1000L))).thenReturn(
            Collections.singletonMap(topicPartition, new org.apache.kafka.clients.consumer.OffsetAndTimestamp(5L, 1000L))
        );

        KafkaOffset offset = (KafkaOffset) consumer.pointerFromTimestampMillis(1000);

        assertEquals(5L, offset.getOffset());
    }

    public void testPointerFromOffset() {
        KafkaOffset offset = new KafkaOffset(5L);
        assertEquals(5L, offset.getOffset());
    }

    public void testTopicDoesNotExist() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "non-existent-topic");
        params.put("bootstrap_servers", "localhost:9092");
        var kafkaSourceConfig = new KafkaSourceConfig(1000, params);
        when(mockConsumer.partitionsFor(eq("non-existent-topic"), any(Duration.class))).thenReturn(null);
        try {
            new KafkaPartitionConsumer("client1", kafkaSourceConfig, 0, mockConsumer);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Topic non-existent-topic does not exist", e.getMessage());
        }
    }

    public void testPartitionDoesNotExist() {
        PartitionInfo partitionInfo = new PartitionInfo("test-topic", 0, null, null, null);
        when(mockConsumer.partitionsFor(eq("test-topic"), any(Duration.class))).thenReturn(Collections.singletonList(partitionInfo));
        try {
            new KafkaPartitionConsumer("client1", config, 1, mockConsumer);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Partition 1 does not exist in topic test-topic", e.getMessage());
        }
    }

    public void testCreateConsumer() {
        String clientId = "test-client";
        Consumer<byte[], byte[]> consumer = KafkaPartitionConsumer.createConsumer(clientId, config);

        assertNotNull(consumer);
        assertEquals(KafkaConsumer.class, consumer.getClass());
    }

    public void testGetPointerBasedLagBeforeAnyFetch() {
        // Before any messages are fetched, lag should be calculated from expected start pointer
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        when(mockConsumer.endOffsets(Collections.singletonList(topicPartition))).thenReturn(Collections.singletonMap(topicPartition, 100L));

        // Expected start pointer is offset 0
        KafkaOffset expectedStartPointer = new KafkaOffset(0L);
        long lag = consumer.getPointerBasedLag(expectedStartPointer);

        assertEquals(100L, lag);
    }

    public void testGetPointerBasedLagAfterFetch() throws Exception {
        // Simulate fetching messages up to offset 50
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>(
            "test-topic",
            0,
            50,
            null,
            "message".getBytes(StandardCharsets.UTF_8)
        );
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(topicPartition, Collections.singletonList(record1))
        );

        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);
        consumer.readNext(new KafkaOffset(50), true, 10, 1000);

        // Now endOffset is 100, lastFetchedOffset is 50
        // expected lag is 49
        when(mockConsumer.endOffsets(Collections.singletonList(topicPartition))).thenReturn(Collections.singletonMap(topicPartition, 100L));

        // Expected start pointer doesn't matter since we've already fetched
        KafkaOffset expectedStartPointer = new KafkaOffset(0L);
        long lag = consumer.getPointerBasedLag(expectedStartPointer);

        assertEquals(49L, lag);
    }

    public void testGetPointerBasedLagWhenCaughtUp() throws Exception {
        // Simulate being caught up: lastFetchedOffset = endOffset - 1
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("test-topic", 0, 99, null, "message".getBytes(StandardCharsets.UTF_8));
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(
            Collections.singletonMap(topicPartition, Collections.singletonList(record))
        );

        when(mockConsumer.poll(any(Duration.class))).thenReturn(records);
        consumer.readNext(new KafkaOffset(99), true, 10, 1000);

        // endOffset is 100, lastFetchedOffset is 99
        // expected lag is 0
        when(mockConsumer.endOffsets(Collections.singletonList(topicPartition))).thenReturn(Collections.singletonMap(topicPartition, 100L));

        // Expected start pointer doesn't matter since we've already fetched
        KafkaOffset expectedStartPointer = new KafkaOffset(0L);
        long lag = consumer.getPointerBasedLag(expectedStartPointer);

        assertEquals(0L, lag);
    }

    public void testGetPointerBasedLagHandlesException() {
        // Simulate an exception when calling endOffsets
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        when(mockConsumer.endOffsets(Collections.singletonList(topicPartition))).thenThrow(
            new RuntimeException("Kafka broker unavailable")
        );

        KafkaOffset expectedStartPointer = new KafkaOffset(0L);
        long lag = consumer.getPointerBasedLag(expectedStartPointer);

        // Should return -1 on exception
        assertEquals(-1, lag);
    }
}
