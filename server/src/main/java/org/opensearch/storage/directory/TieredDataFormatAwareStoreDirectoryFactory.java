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
import org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.DataFormatAwareStoreDirectoryFactory;
import org.opensearch.index.store.FormatChecksumStrategy;
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
        Map<DataFormat, DataFormatAwareStoreHandler> formatStoreHandlers,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        logger.debug("Creating warm+format directory stack for shard [{}]", shardId);

        Directory localDir = localDirectoryFactory.newDirectory(indexSettings, shardPath);
        SubdirectoryAwareDirectory subdirAware = new SubdirectoryAwareDirectory(localDir, shardPath);

        // Convert DataFormat-keyed map to String-keyed map for directory routing (resolves by format name from file path)
        Map<String, DataFormatAwareStoreHandler> handlersByName = new HashMap<>();
        for (Map.Entry<DataFormat, DataFormatAwareStoreHandler> entry : formatStoreHandlers.entrySet()) {
            handlersByName.put(entry.getKey().name(), entry.getValue());
        }

        // Seed handlers from remote metadata — register all format files as REMOTE with full remote path.
        // Initial seed needed because RemoteSegmentStoreDirectory.init() runs before we register listeners.
        if (handlersByName.isEmpty() == false) {
            seedHandlersFromRemoteMetadata(remoteDirectory, handlersByName);
            // TODO (writable warm): also scan local disk for format files that exist locally
            // (e.g., after a crash before sync completed) and seed them as LOCAL:
            // seedHandlersFromLocalFiles(subdirAware, shardPath, handlersByName);
        }

        TieredSubdirectoryAwareDirectory tieredSubdir = new TieredSubdirectoryAwareDirectory(
            subdirAware,
            remoteDirectory,
            fileCache,
            threadPool,
            handlersByName,
            shardPath,
            tieredStoragePrefetchSettingsSupplier
        );

        logger.debug("Created warm+format directory stack for shard [{}] with format handlers: {}", shardId, handlersByName.keySet());

        boolean success = false;
        try {
            DataFormatAwareStoreDirectory result = DataFormatAwareStoreDirectory.withDirectoryDelegate(
                tieredSubdir,
                shardPath,
                checksumStrategies
            );
            success = true;
            return result;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(tieredSubdir);
            }
        }
    }

    /**
     * Seeds format store handlers from remote segment metadata.
     * Registers all format files as REMOTE with full remote path (basePath + format/ + blobKey).
     */
    private static void seedHandlersFromRemoteMetadata(
        RemoteSegmentStoreDirectory remoteDirectory,
        Map<String, DataFormatAwareStoreHandler> handlers
    ) {
        String basePath = remoteDirectory.getRemoteBasePath();
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> metadata = remoteDirectory.getSegmentsUploadedToRemoteStore();

        Map<DataFormatAwareStoreHandler, Map<String, String>> handlerFiles = new HashMap<>();
        for (Map.Entry<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> entry : metadata.entrySet()) {
            String file = entry.getKey();
            String format = DataFormatAwareStoreDirectory.toFileMetadata(file).dataFormat();
            DataFormatAwareStoreHandler handler = handlers.get(format);
            if (handler != null) {
                String blobKey = entry.getValue().getUploadedFilename();
                String remotePath = basePath + ((format != null && format.isEmpty() == false) ? format + "/" : "") + blobKey;
                handlerFiles.computeIfAbsent(handler, k -> new HashMap<>()).put(file, remotePath);
            }
        }

        for (Map.Entry<DataFormatAwareStoreHandler, Map<String, String>> entry : handlerFiles.entrySet()) {
            entry.getKey().seedFileLocations(entry.getValue(), DataFormatAwareStoreHandler.FileLocation.REMOTE);
            logger.debug("Seeded {} format files from remote metadata", entry.getValue().size());
        }
    }
}
