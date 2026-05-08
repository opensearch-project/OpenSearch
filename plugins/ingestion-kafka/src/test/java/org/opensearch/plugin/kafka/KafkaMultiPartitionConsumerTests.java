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
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class KafkaMultiPartitionConsumerTests extends OpenSearchTestCase {

    private static final String TOPIC = "test-topic";

    private Consumer<byte[], byte[]> mockConsumer;
    private KafkaSourceConfig config;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockConsumer = mock(Consumer.class);
        config = mock(KafkaSourceConfig.class);
        when(config.getTopic()).thenReturn(TOPIC);

        // Mock partition metadata — topic has 8 partitions
        List<PartitionInfo> partitionInfos = List.of(
            new PartitionInfo(TOPIC, 0, null, null, null),
            new PartitionInfo(TOPIC, 1, null, null, null),
            new PartitionInfo(TOPIC, 2, null, null, null),
            new PartitionInfo(TOPIC, 3, null, null, null),
            new PartitionInfo(TOPIC, 4, null, null, null),
            new PartitionInfo(TOPIC, 5, null, null, null),
            new PartitionInfo(TOPIC, 6, null, null, null),
            new PartitionInfo(TOPIC, 7, null, null, null)
        );
        when(mockConsumer.partitionsFor(any(), any())).thenReturn(partitionInfos);
    }

    private KafkaMultiPartitionConsumer createConsumer(List<Integer> partitionIds) {
        return new KafkaMultiPartitionConsumer(config, 0, partitionIds, mockConsumer);
    }

    // --- Construction ---

    public void testConstructionAssignsAllPartitions() {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 2, 4, 6));

        List<TopicPartition> expected = List.of(
            new TopicPartition(TOPIC, 0),
            new TopicPartition(TOPIC, 2),
            new TopicPartition(TOPIC, 4),
            new TopicPartition(TOPIC, 6)
        );
        verify(mockConsumer).assign(expected);
        assertEquals(List.of(0, 2, 4, 6), consumer.getAssignedPartitionIds());
    }

    public void testConstructionWithInvalidPartition() {
        expectThrows(
            IllegalArgumentException.class,
            () -> createConsumer(List.of(0, 9)) // partition 9 doesn't exist
        );
    }

    public void testGetShardIdReturnsOpenSearchShardId() {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));
        assertEquals(0, consumer.getShardId()); // shard ID, not partition ID
    }

    // --- readNext (continuation) ---

    public void testReadNextContinuation() throws Exception {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        // Mock poll returning records from both partitions
        TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        TopicPartition tp4 = new TopicPartition(TOPIC, 4);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordMap = new HashMap<>();
        recordMap.put(tp0, List.of(new ConsumerRecord<>(TOPIC, 0, 10L, "key0".getBytes(), "val0".getBytes())));
        recordMap.put(tp4, List.of(new ConsumerRecord<>(TOPIC, 4, 20L, "key4".getBytes(), "val4".getBytes())));
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordMap);
        when(mockConsumer.poll(any())).thenReturn(consumerRecords);

        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> results = consumer.readNext(100, 1000);

        assertEquals(2, results.size());

        // First result from partition 0
        KafkaPartitionOffset ptr0 = (KafkaPartitionOffset) results.get(0).getPointer();
        assertEquals(0, ptr0.getSourcePartition());
        assertEquals(10L, ptr0.getOffset());

        // Second result from partition 4
        KafkaPartitionOffset ptr4 = (KafkaPartitionOffset) results.get(1).getPointer();
        assertEquals(4, ptr4.getSourcePartition());
        assertEquals(20L, ptr4.getOffset());
    }

    public void testReadNextEmptyPoll() throws Exception {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        when(mockConsumer.poll(any())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

        List<IngestionShardConsumer.ReadResult<KafkaOffset, KafkaMessage>> results = consumer.readNext(100, 1000);
        assertTrue(results.isEmpty());
    }

    // --- readNext(offset, ...) is unsupported in multi-partition mode ---

    public void testReadNextWithOffsetThrowsUnsupported() throws Exception {
        // The seeking variant of readNext() has single-partition semantics that don't fit a
        // multi-partition consumer (a single pointer can only reposition one of N partitions).
        // Multi-partition seek must go through seekToPartitionOffsets(Map) instead. Verify the
        // method throws for both KafkaPartitionOffset and plain KafkaOffset inputs.
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        UnsupportedOperationException withPartitionOffset = expectThrows(
            UnsupportedOperationException.class,
            () -> consumer.readNext(new KafkaPartitionOffset(4, 50), true, 100, 1000)
        );
        assertTrue(withPartitionOffset.getMessage().contains("seekToPartitionOffsets"));

        UnsupportedOperationException withLegacyOffset = expectThrows(
            UnsupportedOperationException.class,
            () -> consumer.readNext(new KafkaOffset(50), true, 100, 1000)
        );
        assertTrue(withLegacyOffset.getMessage().contains("seekToPartitionOffsets"));
    }

    public void testPointerFromOffsetRejectsBareOffset() {
        // Multi-partition mode requires explicit "partition:offset" — a bare numeric offset is
        // ambiguous (which assigned partition?) so it must throw rather than silently fall back.
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> consumer.pointerFromOffset("42"));
        assertTrue(e.getMessage().contains("partition:offset"));
    }

    public void testPointerFromOffsetParsesPartitionOffset() {
        // Happy path — "partition:offset" parses to a KafkaPartitionOffset with the right values.
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        IngestionShardPointer pointer = consumer.pointerFromOffset("4:42");
        assertTrue("Should be KafkaPartitionOffset", pointer instanceof KafkaPartitionOffset);
        KafkaPartitionOffset partitionOffset = (KafkaPartitionOffset) pointer;
        assertEquals(4, partitionOffset.getSourcePartition());
        assertEquals(42L, partitionOffset.getOffset());
    }

    public void testPointerFromOffsetRejectsMalformed() {
        // Mirrors KafkaConsumerFactoryTests.testParsePartitionOffsetRejectsMalformed — both parsers
        // must validate identically. Without this, "3:42:99" used to silently truncate to (3, 42)
        // and "3:" used to throw a cryptic ArrayIndexOutOfBoundsException.
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        IllegalArgumentException tooManyParts = expectThrows(
            IllegalArgumentException.class,
            () -> consumer.pointerFromOffset("3:42:99")
        );
        assertTrue(tooManyParts.getMessage().contains("partition:offset"));

        IllegalArgumentException emptyPartition = expectThrows(
            IllegalArgumentException.class,
            () -> consumer.pointerFromOffset(":42")
        );
        assertTrue(emptyPartition.getMessage().contains("partition:offset"));

        IllegalArgumentException emptyOffset = expectThrows(IllegalArgumentException.class, () -> consumer.pointerFromOffset("3:"));
        assertTrue(emptyOffset.getMessage().contains("partition:offset"));
    }

    // --- Single-pointer methods are unsupported in multi-partition mode ---

    public void testEarliestPointerThrowsUnsupported() {
        // earliestPointer() returns a single pointer — meaningless across N assigned partitions.
        // Callers must use seekToBeginning() directly for RESET_TO_EARLIEST.
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, consumer::earliestPointer);
        assertTrue(e.getMessage().contains("seekToBeginning"));
    }

    public void testLatestPointerThrowsUnsupported() {
        // latestPointer() — same single-partition limitation as earliestPointer().
        // Callers must use seekToEnd() directly for RESET_TO_LATEST.
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, consumer::latestPointer);
        assertTrue(e.getMessage().contains("seekToEnd"));
    }

    public void testPointerFromTimestampMillisThrowsUnsupported() {
        // pointerFromTimestampMillis() returns single pointer for the first partition with data —
        // non-deterministic and useless for multi-partition reset. PR 7.5 needs a per-partition variant.
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> consumer.pointerFromTimestampMillis(System.currentTimeMillis())
        );
        assertTrue(e.getMessage().contains("per-partition"));
    }

    // --- seekToPartitionOffsets ---

    public void testSeekToPartitionOffsets() {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        Map<Integer, KafkaPartitionOffset> offsets = Map.of(0, new KafkaPartitionOffset(0, 100), 4, new KafkaPartitionOffset(4, 200));
        consumer.seekToPartitionOffsets(offsets);

        verify(mockConsumer).seek(new TopicPartition(TOPIC, 0), 100L);
        verify(mockConsumer).seek(new TopicPartition(TOPIC, 4), 200L);
    }

    public void testSeekToPartitionOffsetsIgnoresUnassigned() {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        // Partition 7 is not assigned to this consumer — should be ignored
        Map<Integer, KafkaPartitionOffset> offsets = Map.of(
            0,
            new KafkaPartitionOffset(0, 100),
            7,
            new KafkaPartitionOffset(7, 300) // not assigned
        );
        consumer.seekToPartitionOffsets(offsets);

        verify(mockConsumer).seek(new TopicPartition(TOPIC, 0), 100L);
        // partition 7 seek should NOT be called
    }

    // --- seekToBeginning / seekToEnd ---

    public void testSeekToBeginning() {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));
        consumer.seekToBeginning();

        List<TopicPartition> expected = List.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 4));
        verify(mockConsumer).seekToBeginning(expected);
    }

    public void testSeekToEnd() {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));
        consumer.seekToEnd();

        List<TopicPartition> expected = List.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 4));
        verify(mockConsumer).seekToEnd(expected);
    }

    // --- getPointerBasedLag ---

    public void testGetPointerBasedLagSumsAcrossPartitions() {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));

        // Simulate fetched offsets by polling
        TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        TopicPartition tp4 = new TopicPartition(TOPIC, 4);
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordMap = new HashMap<>();
        recordMap.put(tp0, List.of(new ConsumerRecord<>(TOPIC, 0, 90L, null, null)));
        recordMap.put(tp4, List.of(new ConsumerRecord<>(TOPIC, 4, 80L, null, null)));
        when(mockConsumer.poll(any())).thenReturn(new ConsumerRecords<>(recordMap));
        try {
            consumer.readNext(100, 1000);
        } catch (Exception e) {
            // ignore
        }

        // Mock end offsets
        Map<TopicPartition, Long> endOffsets = Map.of(tp0, 100L, tp4, 100L);
        when(mockConsumer.endOffsets(anyCollection())).thenReturn(endOffsets);

        // Lag: (100 - 90 - 1) + (100 - 80 - 1) = 9 + 19 = 28
        long lag = consumer.getPointerBasedLag(new KafkaPartitionOffset(0, 0));
        assertEquals(28, lag);
    }

    // --- close ---

    public void testClose() throws Exception {
        KafkaMultiPartitionConsumer consumer = createConsumer(List.of(0, 4));
        consumer.close();
        verify(mockConsumer).close();
    }
}
