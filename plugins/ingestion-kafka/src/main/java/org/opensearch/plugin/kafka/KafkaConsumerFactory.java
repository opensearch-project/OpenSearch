/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;

import java.io.IOException;
import java.util.List;

/**
 * Factory for creating Kafka consumers.
 * <p>
 * The type parameter uses {@code IngestionShardConsumer<KafkaOffset, KafkaMessage>} rather than a
 * concrete consumer class, allowing the factory to return either {@link KafkaPartitionConsumer}
 * (single-partition) or {@link KafkaMultiPartitionConsumer} (multi-partition) from different methods.
 */
public class KafkaConsumerFactory
    implements IngestionConsumerFactory<IngestionShardConsumer<KafkaOffset, KafkaMessage>, KafkaOffset> {

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
            String[] parts = pointer.split(":");
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
        // TODO: Consider using Kafka AdminClient instead of creating a full consumer just for
        // partition metadata. AdminClient is lighter (no group coordinator handshake). Acceptable
        // for now since the result is cached and called once per shard lifecycle.
        try (KafkaPartitionConsumer tempConsumer = new KafkaPartitionConsumer(
            "partition-count-query",
            config,
            0
        )) {
            cachedPartitionCount = tempConsumer.getPartitionCount();
            return cachedPartitionCount;
        } catch (IOException e) {
            throw new RuntimeException("Failed to close temporary consumer while querying partition count", e);
        }
    }

    @Override
    public IngestionShardConsumer<KafkaOffset, KafkaMessage> createMultiPartitionShardConsumer(String clientId, int shardId, List<Integer> partitionIds) {
        assert config != null;
        if (partitionIds.size() == 1) {
            return new KafkaPartitionConsumer(clientId, config, partitionIds.get(0));
        }
        return new KafkaMultiPartitionConsumer(clientId, config, shardId, partitionIds);
    }
}
