/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Factory for creating Hive shard consumers.
 */
public class HiveConsumerFactory implements IngestionConsumerFactory<HiveShardConsumer, HivePointer> {

    private HiveSourceConfig config;

    public HiveConsumerFactory() {}

    @Override
    public void initialize(IngestionSource ingestionSource) {
        config = new HiveSourceConfig(ingestionSource.params());
    }

    @Override
    public HiveShardConsumer createShardConsumer(String clientId, int shardId) {
        return new HiveShardConsumer(clientId, shardId, config);
    }

    @Override
    public HivePointer parsePointerFromString(String pointer) {
        return HivePointer.fromString(pointer);
    }
}
