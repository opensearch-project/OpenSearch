/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

/**
 * Factory interface for creating DataFormatAwareStoreDirectory instances.
 *
 * <p>Follows the existing {@link IndexStorePlugin} pattern to provide a
 * centralized way to create directories that understand multiple data
 * formats. Accepts a {@link IndexStorePlugin.DirectoryFactory} to delegate
 * local directory creation rather than hardcoding a specific implementation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatAwareStoreDirectoryFactory {

    /**
     * Creates a new DataFormatAwareStoreDirectory per shard with automatic
     * format discovery.
     *
     * @param indexSettings          the shard's index settings
     * @param shardId                the shard identifier
     * @param shardPath              the path the shard is using for file storage
     * @param localDirectoryFactory  the factory for creating the underlying local directory
     * @param checksumStrategies     pre-built checksum strategies keyed by format name
     * @return a new DataFormatAwareStoreDirectory
     * @throws IOException if directory creation fails
     */
    DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) throws IOException;

    /**
     * Creates a new DataFormatAwareStoreDirectory for warm nodes with tiered
     * storage support.
     *
     * <p>Implementations that support warm+format override this method to
     * build the full tiered directory stack. The caller creates the
     * {@link org.opensearch.storage.directory.StoreStrategyRegistry} externally
     * and passes it in so that native store handles can be extracted before
     * the directory is created.
     *
     * @param indexSettings          the shard's index settings
     * @param shardId                the shard identifier
     * @param shardPath              the path the shard is using for file storage
     * @param localDirectoryFactory  the factory for creating the underlying local directory
     * @param checksumStrategies     pre-built checksum strategies keyed by format name
     * @param strategies             the pre-built store strategy registry for this shard
     * @param remoteDirectory        the remote segment store directory
     * @param fileCache              the file cache for warm node caching
     * @param threadPool             the thread pool for async operations
     * @return a new DataFormatAwareStoreDirectory
     * @throws IOException if directory creation fails
     */
    default DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies,
        org.opensearch.storage.directory.StoreStrategyRegistry strategies,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        throw new UnsupportedOperationException("Warm-aware directory creation not supported by this factory");
    }
}
