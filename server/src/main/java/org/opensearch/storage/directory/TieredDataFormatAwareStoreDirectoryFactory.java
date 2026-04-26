/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.DataFormatAwareStoreDirectoryFactory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Factory for creating the warm+format directory stack.
 *
 * <p>This factory builds the full tiered directory stack for warm nodes with pluggable data format
 * support. The resulting directory stack is:
 * <pre>
 *   DataFormatAwareStoreDirectory (checksums, format metadata)
 *     → TieredSubdirectoryAwareDirectory (format routing + tiered storage)
 *       ├── wraps: SubdirectoryAwareDirectory → FSDirectory
 *       ├── holds: TieredDirectory(SubdirectoryAwareDirectory, RemoteDir, FileCache, ThreadPool)
 *       └── holds: Map&lt;String, DataFormatDirectoryDelegator&gt;
 * </pre>
 *
 * <p>This factory is only used for warm+format indices. It is always registered in Node.java
 * under the key "dataformat-tiered", but IndexModule only selects it when both
 * {@code isWarmIndex()} and {@code isPluggableDataFormatEnabled()} are true.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredDataFormatAwareStoreDirectoryFactory implements DataFormatAwareStoreDirectoryFactory {

    /** Factory key for the warm+format tiered directory stack. */
    public static final String FACTORY_KEY = "dataformat-tiered";

    private static final Logger logger = LogManager.getLogger(TieredDataFormatAwareStoreDirectoryFactory.class);

    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;

    /**
     * Creates a new TieredDataFormatAwareStoreDirectoryFactory with the given prefetch settings supplier.
     *
     * @param tieredStoragePrefetchSettingsSupplier supplier for tiered storage prefetch settings
     */
    public TieredDataFormatAwareStoreDirectoryFactory(Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier) {
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }

    /**
     * Hot path: not supported by this factory. This factory is only for warm+format indices.
     *
     * @throws UnsupportedOperationException always — use the warm-aware overload instead
     */
    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        DataFormatRegistry dataFormatRegistry
    ) throws IOException {
        throw new UnsupportedOperationException(
            "TieredDataFormatAwareStoreDirectoryFactory requires warm parameters "
                + "(remoteDirectory, fileCache, threadPool). Use the warm-aware overload."
        );
    }

    /**
     * Creates the warm+format directory stack.
     *
     * <p>Builds: FSDirectory → SubdirectoryAwareDirectory → TieredSubdirectoryAwareDirectory
     * → DataFormatAwareStoreDirectory (direct delegate constructor).
     *
     * @param indexSettings          the shard's index settings
     * @param shardId                the shard identifier
     * @param shardPath              the path the shard is using for file storage
     * @param localDirectoryFactory  the factory for creating the underlying local directory
     * @param dataFormatRegistry     registry of available data format plugins
     * @param remoteDirectory        the remote segment store directory
     * @param fileCache              the file cache for warm node caching
     * @param threadPool             the thread pool for async operations
     * @return a new DataFormatAwareStoreDirectory wrapping the tiered directory stack
     * @throws IOException if directory creation fails
     */
    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        DataFormatRegistry dataFormatRegistry,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        logger.debug("Creating warm+format directory stack for shard [{}]", shardId);

        // 1. Create local directory via factory
        Directory localDir = localDirectoryFactory.newDirectory(indexSettings, shardPath);

        // 2. Wrap in SubdirectoryAwareDirectory for path routing
        SubdirectoryAwareDirectory subdirAware = new SubdirectoryAwareDirectory(localDir, shardPath);

        // 3. Ask each format plugin for a tiered directory
        Map<DataFormat, Directory> tieredDirs = dataFormatRegistry.getTieredDirectories(subdirAware, remoteDirectory, indexSettings);
        Map<String, Directory> formatDirectories = new HashMap<>();
        for (Map.Entry<DataFormat, Directory> entry : tieredDirs.entrySet()) {
            formatDirectories.put(entry.getKey().name(), entry.getValue());
        }

        // 4. Create TieredSubdirectoryAwareDirectory
        TieredSubdirectoryAwareDirectory tieredSubdir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteDirectory,
            fileCache,
            threadPool,
            formatDirectories,
            tieredStoragePrefetchSettingsSupplier
        );

        logger.debug("Created warm+format directory stack for shard [{}] with format directories: {}", shardId, formatDirectories.keySet());

        // 5. Wrap in DataFormatAwareStoreDirectory (direct delegate constructor — no double wrapping)
        return new DataFormatAwareStoreDirectory(indexSettings, tieredSubdir, shardPath, dataFormatRegistry, true);
    }
}
