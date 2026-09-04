/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class KinesisEfoShardConsumerTests extends OpenSearchTestCase {
    private static final String CONSUMER_ARN = "arn:aws:kinesis:us-west-2:123456789012:stream/testStream/consumer/c:1";

    private KinesisClient mockKinesisClient;
    private KinesisAsyncClient mockKinesisAsyncClient;
    private KinesisSourceConfig config;
    private List<SubscribeToShardRequest> capturedRequests;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockKinesisClient = Mockito.mock(KinesisClient.class);
        mockKinesisAsyncClient = Mockito.mock(KinesisAsyncClient.class);
        capturedRequests = new ArrayList<>();

        Map<String, Object> params = new HashMap<>();
        params.put("region", "us-west-2");
        params.put("stream", "testStream");
        params.put("access_key", "testAccessKey");
        params.put("secret_key", "testSecretKey");
        params.put("enable_fanout", true);
        params.put("fanout_consumer_arn", CONSUMER_ARN);
        config = new KinesisSourceConfig(params);

        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
            .streamDescription(StreamDescription.builder().shards(Shard.builder().shardId("shardId-0").build()).build())
            .build();
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(describeStreamResponse);
    }

    private KinesisEfoShardConsumer createConsumer() {
        return new KinesisEfoShardConsumer("clientId", config, 0, CONSUMER_ARN, mockKinesisClient, mockKinesisAsyncClient);
    }

    private static SubscribeToShardEvent eventWithRecords(String... sequenceNumbers) {
        List<Record> records = Arrays.stream(sequenceNumbers)
            .map(s -> Record.builder().sequenceNumber(s).data(SdkBytes.fromByteArray(new byte[] { 1, 2, 3 })).build())
            .collect(Collectors.toList());
        String continuation = sequenceNumbers.length > 0 ? sequenceNumbers[sequenceNumbers.length - 1] : "0";
        return SubscribeToShardEvent.builder().records(records).continuationSequenceNumber(continuation).build();
    }

    /**
     * Stub subscribeToShard so that the Nth call delivers the Nth batch of events (respecting
     * reactive demand) and then completes normally, mimicking the 5-minute subscription expiry.
     * Calls beyond the last batch return an idle subscription that never delivers and never
     * terminates.
     */
    private void stubSubscriptions(List<List<SubscribeToShardEvent>> batches) {
        AtomicInteger callCount = new AtomicInteger();
        when(mockKinesisAsyncClient.subscribeToShard(any(SubscribeToShardRequest.class), any(SubscribeToShardResponseHandler.class)))
            .thenAnswer(invocation -> {
                capturedRequests.add(invocation.getArgument(0));
                SubscribeToShardResponseHandler handler = invocation.getArgument(1);
                int call = callCount.getAndIncrement();
                CompletableFuture<Void> future = new CompletableFuture<>();

                handler.responseReceived(SubscribeToShardResponse.builder().build());
                if (call >= batches.size()) {
                    // idle subscription: registers demand but never delivers or terminates
                    handler.onEventStream(subscriber -> subscriber.onSubscribe(new Subscription() {
                        @Override
                        public void request(long n) {}

                        @Override
                        public void cancel() {}
                    }));
                    return future;
                }

                List<SubscribeToShardEvent> events = batches.get(call);
                handler.onEventStream(subscriber -> subscriber.onSubscribe(new DemandDrivenSubscription(subscriber, events, () -> {
                    handler.complete();
                    future.complete(null);
                })));
                return future;
            });
    }

    private void stubFailingSubscriptions(RuntimeException error) {
        when(mockKinesisAsyncClient.subscribeToShard(any(SubscribeToShardRequest.class), any(SubscribeToShardResponseHandler.class)))
            .thenAnswer(invocation -> {
                capturedRequests.add(invocation.getArgument(0));
                SubscribeToShardResponseHandler handler = invocation.getArgument(1);
                handler.exceptionOccurred(error);
                return CompletableFuture.failedFuture(error);
            });
    }

    /** Delivers the given events one by one as demand is requested, then completes. */
    private static class DemandDrivenSubscription implements Subscription {
        private final Subscriber<? super software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream> subscriber;
        private final List<SubscribeToShardEvent> events;
        private final Runnable onComplete;
        private int index = 0;
        private boolean terminated = false;

        DemandDrivenSubscription(
            Subscriber<? super software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream> subscriber,
            List<SubscribeToShardEvent> events,
            Runnable onComplete
        ) {
            this.subscriber = subscriber;
            this.events = events;
            this.onComplete = onComplete;
        }

        @Override
        public void request(long n) {
            while (n-- > 0 && index < events.size() && terminated == false) {
                subscriber.onNext(events.get(index++));
            }
            if (index >= events.size() && terminated == false) {
                terminated = true;
                subscriber.onComplete();
                onComplete.run();
            }
        }

        @Override
        public void cancel() {
            terminated = true;
        }
    }

    public void testReadNextFromPosition() throws TimeoutException {
        stubSubscriptions(List.of(List.of(eventWithRecords("1", "2"))));
        KinesisEfoShardConsumer consumer = createConsumer();

        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> results = consumer.readNext(
            new SequenceNumber("1"),
            true,
            10,
            1000
        );

        Assert.assertEquals(2, results.size());
        Assert.assertEquals("1", results.get(0).getPointer().getSequenceNumber());
        Assert.assertEquals("2", results.get(1).getPointer().getSequenceNumber());

        SubscribeToShardRequest firstRequest = capturedRequests.get(0);
        Assert.assertEquals(CONSUMER_ARN, firstRequest.consumerARN());
        Assert.assertEquals("shardId-0", firstRequest.shardId());
        Assert.assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER, firstRequest.startingPosition().type());
        Assert.assertEquals("1", firstRequest.startingPosition().sequenceNumber());
    }

    public void testReadFromEarliestUsesTrimHorizon() throws TimeoutException {
        stubSubscriptions(List.of(List.of(eventWithRecords("1"))));
        KinesisEfoShardConsumer consumer = createConsumer();

        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> results = consumer.readNext(
            (SequenceNumber) consumer.earliestPointer(),
            true,
            10,
            1000
        );

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(ShardIteratorType.TRIM_HORIZON, capturedRequests.get(0).startingPosition().type());
        Assert.assertNull("TRIM_HORIZON must not carry a sequence number", capturedRequests.get(0).startingPosition().sequenceNumber());
    }

    public void testReadFromLatestUsesLatest() throws TimeoutException {
        stubSubscriptions(List.of(List.of(eventWithRecords("1"))));
        KinesisEfoShardConsumer consumer = createConsumer();

        consumer.readNext((SequenceNumber) consumer.latestPointer(), true, 10, 1000);

        Assert.assertEquals(ShardIteratorType.LATEST, capturedRequests.get(0).startingPosition().type());
        Assert.assertNull("LATEST must not carry a sequence number", capturedRequests.get(0).startingPosition().sequenceNumber());
    }

    public void testNonExistingSequenceNumberUsesTrimHorizon() throws TimeoutException {
        // an empty shard yields NON_EXISTING_SEQUENCE_NUMBER, which is not a valid API sequence number;
        // it must be mapped to the start of the shard rather than passed through verbatim
        stubSubscriptions(List.of(List.of(eventWithRecords("1"))));
        KinesisEfoShardConsumer consumer = createConsumer();

        consumer.readNext(SequenceNumber.NON_EXISTING_SEQUENCE_NUMBER, true, 10, 1000);

        Assert.assertEquals(ShardIteratorType.TRIM_HORIZON, capturedRequests.get(0).startingPosition().type());
        Assert.assertNull(capturedRequests.get(0).startingPosition().sequenceNumber());
    }

    public void testMaxMessagesOverflowIsBuffered() throws TimeoutException {
        stubSubscriptions(List.of(List.of(eventWithRecords("1", "2", "3"))));
        KinesisEfoShardConsumer consumer = createConsumer();

        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> first = consumer.readNext(
            new SequenceNumber("1"),
            true,
            2,
            1000
        );
        Assert.assertEquals(2, first.size());
        Assert.assertEquals("2", first.get(1).getPointer().getSequenceNumber());
        Assert.assertEquals(1, capturedRequests.size());

        // the third record is served from the buffer by the continuation read
        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> second = consumer.readNext(2, 100);
        Assert.assertEquals(1, second.size());
        Assert.assertEquals("3", second.get(0).getPointer().getSequenceNumber());

        // the renewed subscription must resume after the last fetched record ("3"), not the last
        // delivered one, so buffered records are not fetched twice
        Assert.assertEquals(2, capturedRequests.size());
        SubscribeToShardRequest renewal = capturedRequests.get(1);
        Assert.assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, renewal.startingPosition().type());
        Assert.assertEquals("3", renewal.startingPosition().sequenceNumber());
    }

    public void testResubscribeAfterNormalCompletion() throws TimeoutException {
        stubSubscriptions(List.of(List.of(eventWithRecords("1")), List.of(eventWithRecords("2"))));
        KinesisEfoShardConsumer consumer = createConsumer();

        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> results = consumer.readNext(
            new SequenceNumber("1"),
            true,
            10,
            1000
        );

        // both subscriptions were drained within a single poll
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("1", results.get(0).getPointer().getSequenceNumber());
        Assert.assertEquals("2", results.get(1).getPointer().getSequenceNumber());

        Assert.assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER, capturedRequests.get(0).startingPosition().type());
        Assert.assertEquals("1", capturedRequests.get(0).startingPosition().sequenceNumber());
        Assert.assertEquals(ShardIteratorType.AFTER_SEQUENCE_NUMBER, capturedRequests.get(1).startingPosition().type());
        Assert.assertEquals("1", capturedRequests.get(1).startingPosition().sequenceNumber());
    }

    public void testRepositionDiscardsBufferAndResubscribes() throws TimeoutException {
        stubSubscriptions(List.of(List.of(eventWithRecords("5", "6")), List.of(eventWithRecords("9"))));
        KinesisEfoShardConsumer consumer = createConsumer();

        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> first = consumer.readNext(
            new SequenceNumber("5"),
            true,
            1,
            1000
        );
        Assert.assertEquals(1, first.size());
        Assert.assertEquals("5", first.get(0).getPointer().getSequenceNumber());

        // repositioning must discard the buffered record "6" and resubscribe at the new position
        List<IngestionShardConsumer.ReadResult<SequenceNumber, KinesisMessage>> second = consumer.readNext(
            new SequenceNumber("9"),
            true,
            10,
            1000
        );
        Assert.assertEquals(1, second.size());
        Assert.assertEquals("9", second.get(0).getPointer().getSequenceNumber());

        Assert.assertEquals(ShardIteratorType.AT_SEQUENCE_NUMBER, capturedRequests.get(1).startingPosition().type());
        Assert.assertEquals("9", capturedRequests.get(1).startingPosition().sequenceNumber());
    }

    public void testConsecutiveSubscriptionFailuresSurface() {
        stubFailingSubscriptions(new RuntimeException("subscription failed"));
        KinesisEfoShardConsumer consumer = createConsumer();

        RuntimeException e = expectThrows(RuntimeException.class, () -> consumer.readNext(new SequenceNumber("1"), true, 10, 5000));
        Assert.assertTrue(e.getMessage().contains("consecutive times"));
        Assert.assertEquals(KinesisEfoShardConsumer.MAX_CONSECUTIVE_SUBSCRIPTION_FAILURES, capturedRequests.size());
    }

    public void testContinuationReadWithoutSubscriptionThrows() {
        stubSubscriptions(List.of());
        KinesisEfoShardConsumer consumer = createConsumer();

        expectThrows(IllegalStateException.class, () -> consumer.readNext(10, 100));
    }
}
