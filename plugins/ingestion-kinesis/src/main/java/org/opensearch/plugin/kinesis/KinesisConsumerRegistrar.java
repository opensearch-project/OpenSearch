/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Registers (or reuses) an enhanced fan-out stream consumer and returns its ARN.
 * <p>
 * This mirrors the KCL {@code FanOutConsumerRegistration} flow: look up the consumer by name and reuse it if it
 * already exists, otherwise register it, tolerating the race where another caller registers it concurrently
 * ({@link ResourceInUseException}), and finally wait until the consumer reaches {@link ConsumerStatus#ACTIVE}.
 * The operation is idempotent: because the consumer name is stable, subsequent calls (including after a restart)
 * find and reuse the existing consumer instead of creating a new one. Consumers are never deregistered here.
 */
@SuppressWarnings("removal")
final class KinesisConsumerRegistrar {
    private static final Logger logger = LogManager.getLogger(KinesisConsumerRegistrar.class);

    // Bounded retries for throttling and for polling until the consumer becomes ACTIVE, mirroring KCL defaults.
    static final int MAX_REGISTER_RETRIES = 10;
    static final int MAX_DESCRIBE_RETRIES = 10;
    static final long RETRY_BACKOFF_MILLIS = 1000L;

    private KinesisConsumerRegistrar() {}

    /**
     * Get the ARN of the enhanced fan-out consumer named {@code consumerName} on {@code streamName}, registering it
     * if it does not already exist and waiting for it to become ACTIVE. Uses the default retry settings.
     * @param client the synchronous Kinesis client
     * @param streamName the stream name
     * @param consumerName the consumer name to register or reuse
     * @return the consumer ARN, guaranteed ACTIVE
     */
    static String getOrCreateConsumerArn(KinesisClient client, String streamName, String consumerName) {
        return getOrCreateConsumerArn(client, streamName, consumerName, MAX_REGISTER_RETRIES, MAX_DESCRIBE_RETRIES, RETRY_BACKOFF_MILLIS);
    }

    /**
     * Same as {@link #getOrCreateConsumerArn(KinesisClient, String, String)} with explicit retry settings. Visible
     * for testing so the backoff can be shortened.
     */
    static String getOrCreateConsumerArn(
        KinesisClient client,
        String streamName,
        String consumerName,
        int maxRegisterRetries,
        int maxDescribeRetries,
        long backoffMillis
    ) {
        String streamArn = describeStreamArn(client, streamName);

        // 1. reuse the consumer if it already exists
        DescribeStreamConsumerResponse existing = describeConsumerByName(client, streamArn, consumerName);
        String consumerArn = existing != null ? existing.consumerDescription().consumerARN() : null;

        // 2. register it otherwise, tolerating a concurrent registration by another shard/node
        if (consumerArn == null) {
            try {
                consumerArn = registerConsumer(client, streamArn, consumerName, maxRegisterRetries, backoffMillis);
            } catch (ResourceInUseException e) {
                logger.debug("Kinesis stream consumer {} already exists (concurrent registration), describing it", consumerName);
                DescribeStreamConsumerResponse raced = describeConsumerByName(client, streamArn, consumerName);
                if (raced == null) {
                    throw e;
                }
                consumerArn = raced.consumerDescription().consumerARN();
            }
        }

        // 3. wait until the consumer is ACTIVE before it is used to subscribe
        waitForActive(client, consumerArn, streamName, consumerName, maxDescribeRetries, backoffMillis);
        return consumerArn;
    }

    private static String describeStreamArn(KinesisClient client, String streamName) {
        return AccessController.doPrivileged(
            (PrivilegedAction<String>) () -> client.describeStreamSummary(
                DescribeStreamSummaryRequest.builder().streamName(streamName).build()
            ).streamDescriptionSummary().streamARN()
        );
    }

    /**
     * Describe the consumer by (stream ARN, name), returning {@code null} if it does not exist yet.
     */
    private static DescribeStreamConsumerResponse describeConsumerByName(KinesisClient client, String streamArn, String consumerName) {
        try {
            return AccessController.doPrivileged(
                (PrivilegedAction<DescribeStreamConsumerResponse>) () -> client.describeStreamConsumer(
                    DescribeStreamConsumerRequest.builder().streamARN(streamArn).consumerName(consumerName).build()
                )
            );
        } catch (ResourceNotFoundException e) {
            return null;
        }
    }

    private static String registerConsumer(
        KinesisClient client,
        String streamArn,
        String consumerName,
        int maxRegisterRetries,
        long backoffMillis
    ) {
        LimitExceededException lastThrottle = null;
        for (int attempt = 0; attempt < maxRegisterRetries; attempt++) {
            try {
                RegisterStreamConsumerResponse response = AccessController.doPrivileged(
                    (PrivilegedAction<RegisterStreamConsumerResponse>) () -> client.registerStreamConsumer(
                        RegisterStreamConsumerRequest.builder().streamARN(streamArn).consumerName(consumerName).build()
                    )
                );
                return response.consumer().consumerARN();
            } catch (LimitExceededException e) {
                logger.debug("RegisterStreamConsumer for {} was throttled, will retry", consumerName);
                lastThrottle = e;
                backoff(backoffMillis);
            }
        }
        throw lastThrottle;
    }

    private static void waitForActive(
        KinesisClient client,
        String consumerArn,
        String streamName,
        String consumerName,
        int maxDescribeRetries,
        long backoffMillis
    ) {
        ConsumerStatus status = null;
        for (int attempt = 0; attempt < maxDescribeRetries; attempt++) {
            DescribeStreamConsumerResponse response = AccessController.doPrivileged(
                (PrivilegedAction<DescribeStreamConsumerResponse>) () -> client.describeStreamConsumer(
                    DescribeStreamConsumerRequest.builder().consumerARN(consumerArn).build()
                )
            );
            status = response.consumerDescription().consumerStatus();
            if (ConsumerStatus.ACTIVE.equals(status)) {
                return;
            }
            logger.info(
                "Waiting for Kinesis stream consumer {} on stream {} to become ACTIVE (current: {})",
                consumerName,
                streamName,
                status
            );
            backoff(backoffMillis);
        }
        throw new IllegalStateException(
            "Kinesis stream consumer "
                + consumerName
                + " on stream "
                + streamName
                + " did not become ACTIVE after "
                + maxDescribeRetries
                + " attempts (last status: "
                + status
                + ")"
        );
    }

    private static void backoff(long backoffMillis) {
        if (backoffMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while registering Kinesis stream consumer", e);
        }
    }
}
