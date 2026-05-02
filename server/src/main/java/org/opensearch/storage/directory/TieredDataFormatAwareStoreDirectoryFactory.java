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
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.DataFormatAwareStoreDirectoryFactory;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.NativeStoreRepository;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Factory for creating the warm+format directory stack.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredDataFormatAwareStoreDirectoryFactory implements DataFormatAwareStoreDirectoryFactory {

    public static final String FACTORY_KEY = "dataformat-tiered";

    private static final Logger logger = LogManager.getLogger(TieredDataFormatAwareStoreDirectoryFactory.class);

    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;

    public TieredDataFormatAwareStoreDirectoryFactory(Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier) {
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }

    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) throws IOException {
        throw new UnsupportedOperationException(
            "TieredDataFormatAwareStoreDirectoryFactory requires warm parameters. Use the warm-aware overload."
        );
    }

    @Override
    public DataFormatAwareStoreDirectory newDataFormatAwareStoreDirectory(
        IndexSettings indexSettings,
        ShardId shardId,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Map<String, FormatChecksumStrategy> checksumStrategies,
        Map<DataFormat, StoreStrategy> storeStrategies,
        NativeStoreRepository nativeStore,
        boolean isWarm,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        logger.debug(
            "Creating warm+format directory stack for shard [{}] with {} strategies",
            shardId,
            storeStrategies == null ? 0 : storeStrategies.size()
        );

        Directory localDir = localDirectoryFactory.newDirectory(indexSettings, shardPath);
        SubdirectoryAwareDirectory subdirAware = new SubdirectoryAwareDirectory(localDir, shardPath);

        StoreStrategyRegistry strategies = null;
        TieredSubdirectoryAwareDirectory tieredSubdir = null;
        boolean success = false;
        try {
            strategies = StoreStrategyRegistry.open(shardPath, isWarm, nativeStore, storeStrategies, remoteDirectory);
            tieredSubdir = new TieredSubdirectoryAwareDirectory(
                subdirAware,
                remoteDirectory,
                fileCache,
                threadPool,
                strategies,
                shardPath,
                tieredStoragePrefetchSettingsSupplier
            );

            DataFormatAwareStoreDirectory result = DataFormatAwareStoreDirectory.withDirectoryDelegate(
                tieredSubdir,
                shardPath,
                checksumStrategies
            );
            success = true;
            return result;
        } finally {
            if (success == false) {
                if (tieredSubdir != null) {
                    IOUtils.closeWhileHandlingException(tieredSubdir);
                } else if (strategies != null) {
                    IOUtils.closeWhileHandlingException(strategies);
                }
            }
        }
    }
}
