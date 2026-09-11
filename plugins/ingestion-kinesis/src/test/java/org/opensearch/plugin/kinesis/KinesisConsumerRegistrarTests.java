/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.ConsumerDescription;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KinesisConsumerRegistrarTests extends OpenSearchTestCase {
    private static final String STREAM = "testStream";
    private static final String NAME = "my-consumer";
    private static final String STREAM_ARN = "arn:aws:kinesis:us-west-2:123456789012:stream/testStream";
    private static final String CONSUMER_ARN = STREAM_ARN + "/consumer/my-consumer:1";

    private KinesisClient client;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = Mockito.mock(KinesisClient.class);
        when(client.describeStreamSummary(any(DescribeStreamSummaryRequest.class))).thenReturn(
            DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(StreamDescriptionSummary.builder().streamARN(STREAM_ARN).build())
                .build()
        );
    }

    private static DescribeStreamConsumerResponse consumerWithStatus(ConsumerStatus status) {
        return DescribeStreamConsumerResponse.builder()
            .consumerDescription(ConsumerDescription.builder().consumerARN(CONSUMER_ARN).consumerStatus(status).build())
            .build();
    }

    public void testReusesExistingConsumer() {
        // both the by-name lookup and the ACTIVE wait see an existing ACTIVE consumer
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenReturn(consumerWithStatus(ConsumerStatus.ACTIVE));

        String arn = KinesisConsumerRegistrar.getOrCreateConsumerArn(client, STREAM, NAME, 3, 3, 0L);

        Assert.assertEquals(CONSUMER_ARN, arn);
        verify(client, never()).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
    }

    public void testRegistersWhenAbsent() {
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenAnswer(invocation -> {
            DescribeStreamConsumerRequest request = invocation.getArgument(0);
            if (request.consumerName() != null) {
                // by-name lookup: consumer does not exist yet
                throw ResourceNotFoundException.builder().message("not found").build();
            }
            // by-ARN wait: ACTIVE
            return consumerWithStatus(ConsumerStatus.ACTIVE);
        });
        when(client.registerStreamConsumer(any(RegisterStreamConsumerRequest.class))).thenReturn(
            RegisterStreamConsumerResponse.builder().consumer(Consumer.builder().consumerARN(CONSUMER_ARN).build()).build()
        );

        String arn = KinesisConsumerRegistrar.getOrCreateConsumerArn(client, STREAM, NAME, 3, 3, 0L);

        Assert.assertEquals(CONSUMER_ARN, arn);
        verify(client).registerStreamConsumer(any(RegisterStreamConsumerRequest.class));
    }

    public void testToleratesConcurrentRegistration() {
        AtomicInteger byNameCalls = new AtomicInteger();
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenAnswer(invocation -> {
            DescribeStreamConsumerRequest request = invocation.getArgument(0);
            if (request.consumerName() != null) {
                // first by-name lookup misses, second (after the ResourceInUse race) finds it
                if (byNameCalls.getAndIncrement() == 0) {
                    throw ResourceNotFoundException.builder().message("not found").build();
                }
                return consumerWithStatus(ConsumerStatus.ACTIVE);
            }
            return consumerWithStatus(ConsumerStatus.ACTIVE);
        });
        when(client.registerStreamConsumer(any(RegisterStreamConsumerRequest.class))).thenThrow(
            ResourceInUseException.builder().message("already exists").build()
        );

        String arn = KinesisConsumerRegistrar.getOrCreateConsumerArn(client, STREAM, NAME, 3, 3, 0L);

        Assert.assertEquals(CONSUMER_ARN, arn);
        Assert.assertEquals("by-name describe should be retried after the race", 2, byNameCalls.get());
    }

    public void testThrowsWhenConsumerNeverBecomesActive() {
        when(client.describeStreamConsumer(any(DescribeStreamConsumerRequest.class))).thenAnswer(invocation -> {
            DescribeStreamConsumerRequest request = invocation.getArgument(0);
            if (request.consumerName() != null) {
                // by-name lookup finds it, but it is stuck in CREATING
                return consumerWithStatus(ConsumerStatus.CREATING);
            }
            return consumerWithStatus(ConsumerStatus.CREATING);
        });

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> KinesisConsumerRegistrar.getOrCreateConsumerArn(client, STREAM, NAME, 3, 3, 0L)
        );
        Assert.assertTrue(e.getMessage().contains("did not become ACTIVE"));
    }
}
