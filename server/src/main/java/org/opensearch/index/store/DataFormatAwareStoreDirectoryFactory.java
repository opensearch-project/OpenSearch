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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

/**
 * Factory interface for creating DataFormatAwareStoreDirectory instances.
 * This interface follows the existing IndexStorePlugin pattern to provide
 * a centralized way to create composite directories with format discovery.
 *
 * <p>Following the same delegation pattern as {@link IndexStorePlugin.CompositeDirectoryFactory},
 * this factory accepts a {@link IndexStorePlugin.DirectoryFactory} to delegate local directory
 * creation rather than hardcoding a specific directory implementation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatAwareStoreDirectoryFactory {

    /**
     * Creates a new DataFormatAwareStoreDirectory per shard with automatic format discovery.
     * <p>
     * The factory will:
     * - Delegate local directory creation to the provided localDirectoryFactory
     * - Use DataFormatRegistry to discover available data format plugins
     * - Create format-specific directories for each discovered format
     * - Provide fallback behavior if no plugins are found
     * - Handle errors gracefully with proper logging
     *
     * @param indexSettings          the shard's index settings containing configuration
     * @param shardId                the shard identifier
     * @param shardPath              the path the shard is using for file storage
     * @param localDirectoryFactory  the factory for creating the underlying local directory, respecting index store type configuration
     * @param checksumStrategies     pre-built checksum strategies keyed by format name
     * @return a new DataFormatAwareStoreDirectory instance supporting all discovered formats
     * @throws IOException if directory creation fails or resources cannot be allocated
     */
    DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) throws IOException;

    /**
     * Creates a new DataFormatAwareStoreDirectory for warm nodes with tiered storage support.
     *
     * <p>This overload accepts additional parameters needed for warm node directory creation,
     * including the remote directory, file cache, and thread pool. The default implementation
     * delegates to the 5-parameter method, ignoring the warm-specific parameters.
     *
     * <p>Implementations that support warm+format (e.g., TieredDataFormatAwareStoreDirectoryFactory)
     * should override this method to build the full tiered directory stack.
     *
     * @param indexSettings          the shard's index settings
     * @param shardId                the shard identifier
     * @param shardPath              the path the shard is using for file storage
     * @param localDirectoryFactory  the factory for creating the underlying local directory
     * @param checksumStrategies     pre-built checksum strategies keyed by format name
     * @param formatStoreHandlers    per-format store handlers (keyed by DataFormat) for tiered storage routing
     * @param remoteDirectory        the remote segment store directory
     * @param fileCache              the file cache for warm node caching
     * @param threadPool             the thread pool for async operations
     * @return a new DataFormatAwareStoreDirectory instance
     * @throws IOException if directory creation fails
     */
    default DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies,
        Map<DataFormat, DataFormatAwareStoreHandler> formatStoreHandlers,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        throw new UnsupportedOperationException("Warm-aware directory creation not supported by this factory");
    }
}
