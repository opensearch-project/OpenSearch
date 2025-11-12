/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Factory for creating file-based ingestion consumers.
 */
public class FileConsumerFactory implements IngestionConsumerFactory<FilePartitionConsumer, FileOffset> {

    private FileSourceConfig config;

    /**
     * Initialize a FileConsumerFactory for file-based indexing.
     */
    public FileConsumerFactory() {}

    @Override
    public void initialize(IngestionSource ingestionSource) {
        this.config = new FileSourceConfig(ingestionSource.params());
    }

    @Override
    public FilePartitionConsumer createShardConsumer(String clientId, int shardId) {
        assert config != null;
        return new FilePartitionConsumer(config, shardId);
    }

    @Override
    public FileOffset parsePointerFromString(String pointer) {
        return new FileOffset(Long.parseLong(pointer));
    }
}
