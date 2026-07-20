/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.ShardCryptoContext;
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
 * @param checksumStrategies Map of checksum strategies per data format
 * @param shardCryptoContext the shard encryption context, or null if unencrypted
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record IndexingEngineConfig(
    Committer committer,
    MapperService mapperService,
    IndexSettings indexSettings,
    Store store,
    DataFormatRegistry registry,
    Map<String, FormatChecksumStrategy> checksumStrategies,
    ShardCryptoContext shardCryptoContext
) {

    /**
     * Backward-compatible 6-argument constructor defaulting {@code shardCryptoContext} to {@code null}.
     */
    public IndexingEngineConfig(
        Committer committer,
        MapperService mapperService,
        IndexSettings indexSettings,
        Store store,
        DataFormatRegistry registry,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) {
        this(committer, mapperService, indexSettings, store, registry, checksumStrategies, null);
    }

    /**
     * Alias for {@link #shardCryptoContext()} conforming to the Layer 0 contract SPI.
     *
     * @return shard crypto context or null if unencrypted
     */
    public ShardCryptoContext shardContext() {
        return shardCryptoContext;
    }

    /**
     * Creates a child configuration for a sub-engine (e.g. inside composite engine)
     * maintaining the parent's encryption context and settings.
     *
     * @param childSettings child index settings if overriding, or null to inherit
     * @return a new IndexingEngineConfig for the child engine
     */
    public IndexingEngineConfig childConfigFor(IndexSettings childSettings) {
        return new IndexingEngineConfig(
            committer,
            mapperService,
            childSettings != null ? childSettings : indexSettings,
            store,
            registry,
            checksumStrategies,
            shardCryptoContext
        );
    }
}
