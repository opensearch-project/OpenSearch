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
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;

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

    public FileCacheCleaner(NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = nodeEnvironment;
        this.fileCache = nodeEnvironment.fileCache();
    }

    /**
     * before shard deleted and after shard closed, cleans up the corresponding index file path entries from FC.
     * @param shardId  The shard id
     * @param settings the shards index settings
     */
    @Override
    public void beforeIndexShardDeleted(ShardId shardId, Settings settings) {
        try {
            String storeType = settings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey());
            if (IndexModule.Type.REMOTE_SNAPSHOT.match(storeType)) {
                ShardPath shardPath = ShardPath.loadFileCachePath(nodeEnvironment, shardId);
                Path localStorePath = shardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(localStorePath)) {
                    for (Path subPath : ds) {
                        fileCache.remove(subPath.toRealPath());
                    }
                }
            }
        } catch (IOException ioe) {
            log.error(() -> new ParameterizedMessage("Error removing items from cache during shard deletion {})", shardId), ioe);
        }
    }
}
