/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;

/**
 * Initialization parameters for creating an {@link IndexingExecutionEngine} via
 * {@link DataFormatPlugin#indexingEngine}. Bundling parameters in a record avoids
 * breaking the plugin SPI when new context is needed.
 *
 * @param committer the committer for durable flush, or null if not available
 * @param mapperService the mapper service for field mapping resolution
 * @param shardPath the shard path for file storage
 * @param indexSettings the index-level settings
 * @param store the shard's store, or null if not available
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record IndexingEngineSettings(Committer committer, MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings,
    Store store) {
}
