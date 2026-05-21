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
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.IndexStoreListener;
import org.opensearch.plugins.BlockCache;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;

import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;
import static org.opensearch.index.store.remote.utils.FileTypeUtils.INDICES_FOLDER_IDENTIFIER;

/**
 * IndexStoreListener that cleans up all node-level caches when a shard or index is
 * deleted. Handles both the {@link FileCache} (Lucene LRU block-file cache) and any
 * registered {@link BlockCache} instances (e.g. Foyer byte-range cache).
 *
 * <p>Cleanup is deterministic — entries are removed at deletion time rather than
 * waiting for natural LRU eviction pressure.
 *
 * @opensearch.internal
 */
public class NodeCacheServiceCleaner implements IndexStoreListener {

    private static final Logger logger = LogManager.getLogger(NodeCacheServiceCleaner.class);

    private final Supplier<NodeCacheService> nodeCacheServiceSupplier;

    /**
     * @param nodeCacheServiceSupplier lazy supplier — evaluated on first cleanup call,
     *                             after {@code NodeCacheService} is fully initialized.
     *                             Pass {@code () -> this.nodeCacheService} from {@code Node}.
     */
    public NodeCacheServiceCleaner(Supplier<NodeCacheService> nodeCacheServiceSupplier) {
        this.nodeCacheServiceSupplier = nodeCacheServiceSupplier;
    }

    /**
     * Before the shard path is deleted, removes all cache entries for that shard
     * from the FileCache and all registered block caches, then deletes the shard's
     * cache directory from disk.
     */
    @Override
    public void beforeShardPathDeleted(ShardId shardId, IndexSettings indexSettings, NodeEnvironment nodeEnvironment) {
        if (indexSettings.isRemoteSnapshot()) {
            final ShardPath shardPath = ShardPath.loadFileCachePath(nodeEnvironment, shardId);
            cleanupShardCaches(shardPath, false, true);
            deleteShardFileCacheDirectory(shardPath);
        } else if (indexSettings.isWarmIndex()) {
            try {
                final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnvironment, shardId, indexSettings.customDataPath());
                if (shardPath != null) {
                    cleanupShardCaches(shardPath, true, false);
                    deleteShardFileCacheDirectory(shardPath);
                }
            } catch (IOException e) {
                logger.error("failed to clean up shard cache directory", e);
            }
        }
    }

    /**
     * Removes all cache entries for the given shard from all node-level caches.
     *
     * <p>FileCache entries are removed by iterating the shard's local store path.
     * BlockCache entries are evicted by prefix (the shard's data path), which removes
     * all byte-range keys for every file that belonged to this shard.
     */
    private void cleanupShardCaches(ShardPath shardPath, boolean isWarmIndex, boolean isRemoteSnapshot) {
        final NodeCacheService ncs = nodeCacheServiceSupplier.get();
        assert ncs != null : "NodeCacheService must be initialized before cache cleanup";

        // ── FileCache (Lucene LRU block-file cache) ───────────────────────────
        try {
            final Path localStorePath;
            if (isWarmIndex) {
                localStorePath = shardPath.getDataPath().resolve(INDICES_FOLDER_IDENTIFIER);
            } else if (isRemoteSnapshot) {
                localStorePath = shardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
            } else {
                return;
            }

            try (DirectoryStream<Path> ds = Files.newDirectoryStream(localStorePath)) {
                for (Path subPath : ds) {
                    ncs.fileCache().remove(subPath.toRealPath());
                }
            }
        } catch (IOException ioe) {
            String operationType = isWarmIndex ? "warm index" : "remote snapshot";
            logger.error(
                () -> new ParameterizedMessage(
                    "Error removing FileCache entries during {} shard deletion {}",
                    operationType,
                    shardPath.getShardId()
                ),
                ioe
            );
        }

        // ── BlockCaches (byte-range cache, e.g. Foyer) ───────────────────────
        if (isWarmIndex && !ncs.blockCaches().isEmpty()) {
            final String prefix = shardPath.getDataPath().toString();
            for (BlockCache cache : ncs.blockCaches()) {
                cache.evictPrefix(prefix);
            }
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
     * Before the index path is deleted, removes the index-level cache directory from
     * disk. At this point all per-shard caches have already been cleaned up in
     * {@link #beforeShardPathDeleted}.
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
        } else if (indexSettings.isWarmIndex()) {
            final Path indicesPathInCache = nodeEnvironment.fileCacheNodePath().indicesPath.resolve(index.getUUID());
            if (Files.exists(indicesPathInCache)) {
                try {
                    IOUtils.rm(indicesPathInCache);
                } catch (IOException e) {
                    logger.error(() -> new ParameterizedMessage("Failed to delete indices path in cache for index {}", index), e);
                }
            }

            // Defensively evict all block cache entries for this index.
            // beforeShardPathDeleted evicts per-shard, but this catches any stragglers
            // if a shard was absent or deletion was partial.
            final NodeCacheService ncs = nodeCacheServiceSupplier.get();
            if (ncs != null && !ncs.blockCaches().isEmpty()) {
                final String indexPrefix = indicesPathInCache.toString();
                for (BlockCache cache : ncs.blockCaches()) {
                    cache.evictPrefix(indexPrefix);
                }
            }
        }
    }
}
