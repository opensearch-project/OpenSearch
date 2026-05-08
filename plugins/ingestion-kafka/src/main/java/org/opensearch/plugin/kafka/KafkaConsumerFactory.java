/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.List;

/**
 * Factory for creating Kafka consumers.
 * <p>
 * The type parameter uses {@code IngestionShardConsumer<KafkaOffset, KafkaMessage>} rather than a
 * concrete consumer class, allowing the factory to return either {@link KafkaPartitionConsumer}
 * (single-partition) or {@link KafkaMultiPartitionConsumer} (multi-partition) from different methods.
 */
@SuppressWarnings("removal")
public class KafkaConsumerFactory implements IngestionConsumerFactory<IngestionShardConsumer<KafkaOffset, KafkaMessage>, KafkaOffset> {

    /**
     * Configuration for the Kafka source
     */
    protected KafkaSourceConfig config;

    private volatile int cachedPartitionCount = -1;

    /**
     * Constructor.
     */
    public KafkaConsumerFactory() {}

    @Override
    public void initialize(IngestionSource ingestionSource) {
        config = new KafkaSourceConfig((int) ingestionSource.getMaxPollSize(), ingestionSource.params());
    }

    @Override
    public IngestionShardConsumer<KafkaOffset, KafkaMessage> createShardConsumer(String clientId, int shardId) {
        assert config != null;
        return new KafkaPartitionConsumer(clientId, config, shardId);
    }

    @Override
    public KafkaOffset parsePointerFromString(String pointer) {
        if (pointer.contains(":")) {
            // Multi-partition format: "partition:offset"
            String[] parts = pointer.split(":", -1);
            if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
                throw new IllegalArgumentException(
                    "Invalid multi-partition pointer format. Expected 'partition:offset' (e.g., '3:42'), got: " + pointer
                );
            }
            return new KafkaPartitionOffset(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
        }
        return new KafkaOffset(Long.valueOf(pointer));
    }

    @Override
    public int getSourcePartitionCount() {
        if (cachedPartitionCount > 0) {
            return cachedPartitionCount;
        }
        assert config != null;
        // TODO: Consider using Kafka AdminClient instead. AdminClient is lighter (no group
        // coordinator handshake). Acceptable for now since the result is cached and called once
        // per shard lifecycle.
        Consumer<byte[], byte[]> rawConsumer = KafkaPartitionConsumer.createConsumer("partition-count-query", config);
        try {
            String topic = config.getTopic();
            List<PartitionInfo> partitionInfos = AccessController.doPrivileged(
                (PrivilegedAction<List<PartitionInfo>>) () -> rawConsumer.partitionsFor(
                    topic,
                    Duration.ofMillis(config.getTopicMetadataFetchTimeoutMs())
                )
            );
            if (partitionInfos == null) {
                throw new IllegalArgumentException("Topic " + topic + " does not exist");
            }
            cachedPartitionCount = partitionInfos.size();
            return cachedPartitionCount;
        } finally {
            rawConsumer.close();
        }
    }

    @Override
    public IngestionShardConsumer<KafkaOffset, KafkaMessage> createMultiPartitionShardConsumer(
        String clientId,
        int shardId,
        List<Integer> partitionIds
    ) {
        assert config != null;
        // Return KafkaMultiPartitionConsumer, so that produced pointers are KafkaPartitionOffset
        return new KafkaMultiPartitionConsumer(clientId, config, shardId, partitionIds);
    }
}
