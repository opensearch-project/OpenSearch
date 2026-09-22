/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.NodeCacheService;
import org.opensearch.indices.IndicesService;

import static org.opensearch.monitor.fs.FsProbe.adjustForHugeFilesystems;

/**
 * FileSystem service for warm nodes. Reports virtual disk capacity based on
 * total SSD reservation across all caches and their respective data-to-cache
 * ratios, rather than actual physical disk usage.
 *
 * @opensearch.internal
 */
public class WarmFsService extends FsService {

    private static final Logger logger = LogManager.getLogger(WarmFsService.class);

    private final FileCacheSettings fileCacheSettings;
    private final IndicesService indicesService;
    private final NodeCacheService nodeCacheService;

    public WarmFsService(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        FileCacheSettings fileCacheSettings,
        IndicesService indicesService,
        NodeCacheService nodeCacheService
    ) {
        super(settings, nodeEnvironment, nodeCacheService.fileCache());
        this.fileCacheSettings = fileCacheSettings;
        this.indicesService = indicesService;
        this.nodeCacheService = nodeCacheService;
    }

    @Override
    public FsInfo stats() {
        final double dataToFileCacheRatio = fileCacheSettings.getRemoteDataRatio();

        final long fileCacheCapacity = nodeCacheService.fileCache().capacity();
        final long blockCacheCapacity = nodeCacheService.blockCacheCapacityBytes();
        final long totalCacheCapacity = fileCacheCapacity + blockCacheCapacity;

        final long totalBytes = (long) (dataToFileCacheRatio * fileCacheCapacity) + nodeCacheService.virtualBlockCacheBytes();

        // Used bytes from primary shards
        long usedBytes = 0;
        if (indicesService != null) {
            for (IndexService indexService : indicesService) {
                for (IndexShard shard : indexService) {
                    if (shard.routingEntry() != null && shard.routingEntry().primary() && shard.routingEntry().active()) {
                        try {
                            usedBytes += shard.store().stats(0).getSizeInBytes();
                        } catch (Exception e) {
                            logger.error("Unable to get store size for shard {} with error: {}", shard.shardId(), e.getMessage());
                        }
                    }
                }
            }
        }

        long freeBytes = Math.max(0, totalBytes - usedBytes);

        FsInfo.Path warmPath = new FsInfo.Path();
        warmPath.path = "/warm";
        warmPath.mount = "warm";
        warmPath.type = "warm";
        warmPath.total = adjustForHugeFilesystems(totalBytes);
        warmPath.free = adjustForHugeFilesystems(freeBytes);
        warmPath.available = adjustForHugeFilesystems(freeBytes);
        warmPath.fileCacheReserved = totalCacheCapacity > 0 ? adjustForHugeFilesystems(totalCacheCapacity) : -1L;
        warmPath.fileCacheUtilized = adjustForHugeFilesystems(nodeCacheService.cacheUtilizedBytes());

        logger.trace(
            "Warm node disk usage — total: {}, used: {}, free: {}, cacheReserved: {}, cacheUtilized: {}",
            totalBytes,
            usedBytes,
            freeBytes,
            totalCacheCapacity,
            warmPath.fileCacheUtilized
        );

        FsInfo nodeFsInfo = super.stats();
        return new FsInfo(System.currentTimeMillis(), nodeFsInfo.getIoStats(), new FsInfo.Path[] { warmPath });
    }
}
