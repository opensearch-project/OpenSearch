/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;

/**
 * IndexStoreListener to clean up file cache when the index is deleted. The cached entries will be eligible
 * for eviction when the shard is deleted, but this listener deterministically removes entries from memory and
 * from disk at the time of shard deletion as opposed to waiting for the cache to need to perform eviction.
 *
 * @opensearch.internal
 */
public class FileCacheCleaner implements NodeEnvironment.IndexStoreListener {
    private static final Logger logger = LogManager.getLogger(FileCacheCleaner.class);

    private final Provider<FileCache> fileCacheProvider;

    public FileCacheCleaner(Provider<FileCache> fileCacheProvider) {
        this.fileCacheProvider = fileCacheProvider;
    }

    /**
     * before shard path deleted, cleans up the corresponding index file path entries from FC and delete the corresponding shard file
     * cache path.
     *
     * @param shardId  the shard id
     * @param indexSettings the index settings
     * @param nodeEnvironment the node environment
     */
    @Override
    public void beforeShardPathDeleted(ShardId shardId, IndexSettings indexSettings, NodeEnvironment nodeEnvironment) {
        if (indexSettings.isRemoteSnapshot()) {
            final ShardPath shardPath = ShardPath.loadFileCachePath(nodeEnvironment, shardId);
            cleanupShardFileCache(shardPath);
            deleteShardFileCacheDirectory(shardPath);
        }
    }

    /**
     * Cleans up the corresponding index file path entries from FileCache
     *
     * @param shardPath the shard path
     */
    private void cleanupShardFileCache(ShardPath shardPath) {
        try {
            final FileCache fc = fileCacheProvider.get();
            assert fc != null;
            final Path localStorePath = shardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(localStorePath)) {
                for (Path subPath : ds) {
                    fc.remove(subPath.toRealPath());
                }
            }
        } catch (IOException ioe) {
            logger.error(
                () -> new ParameterizedMessage("Error removing items from cache during shard deletion {}", shardPath.getShardId()),
                ioe
            );
        }
    }

    private void deleteShardFileCacheDirectory(ShardPath shardPath) {
        final Path path = shardPath.getDataPath();
        try {
            if (Files.exists(path)) {
                IOUtils.rm(path);
            }
        } catch (IOException e) {
            logger.error(() -> new ParameterizedMessage("Failed to delete cache path for shard {}", shardPath.getShardId()), e);
        }
    }

    /**
     * before index path deleted, delete the corresponding index file cache path.
     *
     * @param index  the index
     * @param indexSettings the index settings
     * @param nodeEnvironment the node environment
     */
    @Override
    public void beforeIndexPathDeleted(Index index, IndexSettings indexSettings, NodeEnvironment nodeEnvironment) {
        if (indexSettings.isRemoteSnapshot()) {
            final Path indexCachePath = nodeEnvironment.fileCacheNodePath().fileCachePath.resolve(index.getUUID());
            if (Files.exists(indexCachePath)) {
                try {
                    IOUtils.rm(indexCachePath);
                } catch (IOException e) {
                    logger.error(() -> new ParameterizedMessage("Failed to delete cache path for index {}", index), e);
                }
            }
        }
    }
}
