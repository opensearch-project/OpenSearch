/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KinesisShardConsumerTests extends OpenSearchTestCase {
    private KinesisClient mockKinesisClient;
    private KinesisSourceConfig config;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockKinesisClient = Mockito.mock(KinesisClient.class);
        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("endpoint_override", "testEndpoint");
        config = new KinesisSourceConfig(params);
    }

    public void testConstructorAndGetters() {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        KinesisShardConsumer consumer = new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);

        Assert.assertEquals("clientId", consumer.getClientId());
        Assert.assertEquals(0, consumer.getShardId());
    }

    public void testConstructorWithInvalidShardId() {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(
                StreamDescription.builder()
                    .shards(Collections.emptyList()) // No shards in the stream
                    .build()
            )
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        try {
            new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Shard id 0 does not exist in stream testStream", e.getMessage());
        }
    }

    public void testReadNext() throws TimeoutException {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        GetShardIteratorResponse getShardIteratorResponse = GetShardIteratorResponse.builder().shardIterator("shardIterator").build();
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(getShardIteratorResponse);

        GetRecordsResponse getRecordsResponse = GetRecordsResponse.builder()
            .records(Record.builder().sequenceNumber("12345").data(SdkBytes.fromByteArray(new byte[] { 1, 2, 3 })).build())
            .build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(getRecordsResponse);

        KinesisShardConsumer consumer = new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);
        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> results = consumer.readNext(
            new SequenceNumber("12345"),
            true,
            10,
            1000
        );

        Assert.assertEquals(1, results.size());
        Assert.assertEquals("12345", results.get(0).getPointer().getSequenceNumber());
    }

    public void testEarliestPointer() {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        GetShardIteratorResponse getShardIteratorResponse = GetShardIteratorResponse.builder().shardIterator("shardIterator").build();
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(getShardIteratorResponse);

        GetRecordsResponse getRecordsResponse = GetRecordsResponse.builder()
            .records(Record.builder().sequenceNumber("12345").build())
            .build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(getRecordsResponse);

        KinesisShardConsumer consumer = new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);
        IngestionShardPointer pointer = consumer.earliestPointer();

        Assert.assertEquals("12345", ((SequenceNumber) pointer).getSequenceNumber());
    }

    public void testLatestPointer() {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        GetShardIteratorResponse getShardIteratorResponse = GetShardIteratorResponse.builder().shardIterator("shardIterator").build();
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(getShardIteratorResponse);

        GetRecordsResponse getRecordsResponse = GetRecordsResponse.builder()
            .records(Record.builder().sequenceNumber("12345").build())
            .build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(getRecordsResponse);

        KinesisShardConsumer consumer = new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);
        IngestionShardPointer pointer = consumer.latestPointer();

        Assert.assertEquals("12345", ((SequenceNumber) pointer).getSequenceNumber());
    }

    public void testPointerFromTimestampMillis() {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        GetShardIteratorResponse getShardIteratorResponse = GetShardIteratorResponse.builder().shardIterator("shardIterator").build();
        when(mockKinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(getShardIteratorResponse);

        GetRecordsResponse getRecordsResponse = GetRecordsResponse.builder()
            .records(Record.builder().sequenceNumber("12345").build())
            .build();
        when(mockKinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(getRecordsResponse);

        KinesisShardConsumer consumer = new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);
        IngestionShardPointer pointer = consumer.pointerFromTimestampMillis(1234567890L);

        Assert.assertEquals("12345", ((SequenceNumber) pointer).getSequenceNumber());
    }

    public void testPointerFromOffset() {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        KinesisShardConsumer consumer = new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);
        IngestionShardPointer pointer = consumer.pointerFromOffset("12345");

        Assert.assertEquals("12345", ((SequenceNumber) pointer).getSequenceNumber());
    }

    public void testClose() throws IOException {
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);

        KinesisShardConsumer consumer = new KinesisShardConsumer("clientId", config, 0, mockKinesisClient);
        consumer.close();

        verify(mockKinesisClient, times(1)).close();
    }

}
