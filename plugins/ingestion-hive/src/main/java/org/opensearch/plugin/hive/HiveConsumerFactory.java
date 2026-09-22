/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Factory for creating {@link HiveShardConsumer} instances.
 * Implements the PBI consumer factory interface to integrate with OpenSearch's ingestion framework.
 */
public class HiveConsumerFactory implements IngestionConsumerFactory<HiveShardConsumer, HivePointer> {

    /** Creates a new HiveConsumerFactory instance. */
    public HiveConsumerFactory() {}

    @Override
    public HiveShardConsumer createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata) {
        HiveSourceConfig config = new HiveSourceConfig(indexMetadata.getIngestionSource().params(), indexMetadata.getNumberOfShards());
        return new HiveShardConsumer(clientId, shardId, config);
    }

    @Override
    public HivePointer parsePointerFromString(String pointer) {
        return HivePointer.fromString(pointer);
    }
}
