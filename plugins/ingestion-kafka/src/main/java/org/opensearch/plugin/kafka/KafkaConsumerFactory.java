/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.index.IngestionConsumerFactory;

import java.util.Map;

/**
 * Factory for creating Kafka consumers
 */
public class KafkaConsumerFactory implements IngestionConsumerFactory<KafkaPartitionConsumer, KafkaOffset> {

    /**
     * Configuration for the Kafka source
     */
    protected KafkaSourceConfig config;

    /**
     * Constructor.
     */
    public KafkaConsumerFactory() {}

    @Override
    public void initialize(Map<String, Object> params) {
        config = new KafkaSourceConfig(params);
    }

    @Override
    public KafkaPartitionConsumer createShardConsumer(String clientId, int shardId) {
        assert config != null;
        return new KafkaPartitionConsumer(clientId, config, shardId);
    }

    @Override
    public KafkaOffset parsePointerFromString(String pointer) {
        return new KafkaOffset(Long.valueOf(pointer));
    }
}
