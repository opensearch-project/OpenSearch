/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import software.amazon.awssdk.services.kinesis.KinesisClient;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionConsumerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating Kinesis consumers.
 */
public class KinesisConsumerFactory implements IngestionConsumerFactory<KinesisShardConsumer, SequenceNumber> {

    // Resolved enhanced fan-out consumer ARNs, keyed by region|stream|consumerName, so the consumer is registered
    // (or looked up) at most once per stream+name and shared across all shards. This factory is a single instance
    // per node (see KinesisPlugin#getIngestionConsumerFactories), so the cache is node-wide.
    private final Map<String, String> consumerArnCache = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public KinesisConsumerFactory() {}

    @Override
    public KinesisShardConsumer createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata) {
        KinesisSourceConfig localConfig = new KinesisSourceConfig(indexMetadata.getIngestionSource().params());
        if (localConfig.isFanoutEnabled()) {
            return new KinesisEfoShardConsumer(clientId, localConfig, shardId, resolveConsumerArn(clientId, localConfig));
        }
        return new KinesisShardConsumer(clientId, localConfig, shardId);
    }

    /**
     * Resolve the enhanced fan-out consumer ARN to subscribe with. If an ARN was configured it is used as-is;
     * otherwise the consumer named in the config is registered (or reused) once per stream+name and cached.
     * Visible for testing.
     */
    String resolveConsumerArn(String clientId, KinesisSourceConfig config) {
        if (config.getFanoutConsumerArn().isEmpty() == false) {
            return config.getFanoutConsumerArn();
        }
        String key = config.getRegion() + "|" + config.getStream() + "|" + config.getFanoutConsumerName();
        return consumerArnCache.computeIfAbsent(key, k -> registerConsumer(clientId, config));
    }

    /**
     * Register (or reuse) the enhanced fan-out consumer named in the config and return its ARN. Visible for testing.
     * @param clientId the client id
     * @param config the Kinesis source config
     * @return the resolved consumer ARN
     */
    protected String registerConsumer(String clientId, KinesisSourceConfig config) {
        KinesisClient client = KinesisShardConsumer.createClient(clientId, config);
        try {
            return KinesisConsumerRegistrar.getOrCreateConsumerArn(client, config.getStream(), config.getFanoutConsumerName());
        } finally {
            client.close();
        }
    }

    @Override
    public SequenceNumber parsePointerFromString(String pointer) {
        return new SequenceNumber(pointer);
    }
}
