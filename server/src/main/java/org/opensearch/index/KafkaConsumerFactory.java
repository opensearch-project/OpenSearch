/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import java.util.Map;

public class KafkaConsumerFactory implements IngestionConsumerFactory<KafkaPartitionConsumer> {

    public static final String TYPE = "kafka";

    private KafkaSourceConfig config;

    @Override
    public void initialize(Map<String, Object> params) {
        config = new KafkaSourceConfig(params);
    }

    @Override
    public KafkaPartitionConsumer createShardConsumer(String clientId, int shardId) {
        assert config!=null;
        return new KafkaPartitionConsumer(clientId, config, shardId);
    }
}
