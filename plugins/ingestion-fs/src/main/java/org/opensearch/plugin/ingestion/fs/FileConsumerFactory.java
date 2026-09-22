/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionConsumerFactory;

/**
 * Factory for creating file-based ingestion consumers.
 */
public class FileConsumerFactory implements IngestionConsumerFactory<FilePartitionConsumer, FileOffset> {

    /**
     * Initialize a FileConsumerFactory for file-based indexing.
     */
    public FileConsumerFactory() {}

    @Override
    public FilePartitionConsumer createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata) {
        FileSourceConfig localConfig = new FileSourceConfig(indexMetadata.getIngestionSource().params());
        return new FilePartitionConsumer(localConfig, shardId);
    }

    @Override
    public FileOffset parsePointerFromString(String pointer) {
        return new FileOffset(Long.parseLong(pointer));
    }
}
