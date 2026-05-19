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
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.Store;

import java.util.Map;

/**
 * Initialization parameters for creating an {@link IndexingExecutionEngine} via
 * {@link DataFormatPlugin#indexingEngine}. Bundling parameters in a record avoids
 * breaking the plugin SPI when new context is needed.
 *
 * @param committer the committer for durable flush, or null if not available
 * @param mapperService the mapper service for field mapping resolution
 * @param indexSettings the index-level settings
 * @param store the shard's store, or null if not available
 * @param registry DataFormatRegistry containing information about registered data formats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record IndexingEngineConfig(Committer committer, MapperService mapperService, IndexSettings indexSettings, Store store,
    DataFormatRegistry registry, Map<String, FormatChecksumStrategy> checksumStrategies) {
}
