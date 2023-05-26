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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.indices.cluster.IndicesClusterStateService;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;

/**
 * IndexEventListener to clean up file cache when the index is deleted. The cached entries will be eligible
 * for eviction when the shard is deleted, but this listener deterministically removes entries from memory and
 * from disk at the time of shard deletion as opposed to waiting for the cache to need to perform eviction.
 *
 * @opensearch.internal
 */
public class FileCacheCleaner implements IndexEventListener {
    private static final Logger log = LogManager.getLogger(FileCacheCleaner.class);

    private final NodeEnvironment nodeEnvironment;
    private final FileCache fileCache;

    public FileCacheCleaner(NodeEnvironment nodeEnvironment, FileCache fileCache) {
        this.nodeEnvironment = nodeEnvironment;
        this.fileCache = fileCache;
    }

    /**
     * before shard deleted and after shard closed, cleans up the corresponding index file path entries from FC.
     * @param shardId  The shard id
     * @param settings the shards index settings
     */
    @Override
    public void beforeIndexShardDeleted(ShardId shardId, Settings settings) {
        try {
            if (isRemoteSnapshot(settings)) {
                final ShardPath shardPath = ShardPath.loadFileCachePath(nodeEnvironment, shardId);
                final Path localStorePath = shardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(localStorePath)) {
                    for (Path subPath : ds) {
                        fileCache.remove(subPath.toRealPath());
                    }
                }
            }
        } catch (IOException ioe) {
            log.error(() -> new ParameterizedMessage("Error removing items from cache during shard deletion {}", shardId), ioe);
        }
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings settings) {
        if (isRemoteSnapshot(settings)) {
            final Path path = ShardPath.loadFileCachePath(nodeEnvironment, shardId).getDataPath();
            try {
                if (Files.exists(path)) {
                    IOUtils.rm(path);
                }
            } catch (IOException e) {
                log.error(() -> new ParameterizedMessage("Failed to delete cache path for shard {}", shardId), e);
            }
        }
    }

    @Override
    public void afterIndexRemoved(
        Index index,
        IndexSettings indexSettings,
        IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason
    ) {
        if (isRemoteSnapshot(indexSettings.getSettings())
            && reason == IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED) {
            final Path indexCachePath = nodeEnvironment.fileCacheNodePath().fileCachePath.resolve(index.getUUID());
            if (Files.exists(indexCachePath)) {
                try {
                    IOUtils.rm(indexCachePath);
                } catch (IOException e) {
                    log.error(() -> new ParameterizedMessage("Failed to delete cache path for index {}", index), e);
                }
            }
        }
    }

    private static boolean isRemoteSnapshot(Settings settings) {
        return IndexModule.Type.REMOTE_SNAPSHOT.match(settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
    }
}
