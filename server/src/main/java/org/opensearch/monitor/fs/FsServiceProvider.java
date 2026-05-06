/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.index.store.remote.filecache.UnifiedCacheService;
import org.opensearch.indices.IndicesService;

/**
 * Factory for creating appropriate FsService implementations based on node type.
 *
 * <p>On warm nodes, creates a {@link WarmFsService} that correctly reports virtual
 * disk capacity and cache reservation across all caches (FileCache + block cache).
 * On non-warm nodes, creates a standard {@link FsService}.
 *
 * @opensearch.internal
 */
public class FsServiceProvider {

    private final Settings settings;
    private final NodeEnvironment nodeEnvironment;
    @Nullable
    private final UnifiedCacheService unifiedCacheService;
    private final FileCacheSettings fileCacheSettings;
    private final IndicesService indicesService;
    private final long virtualBlockCacheBytes;

    public FsServiceProvider(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        UnifiedCacheService unifiedCacheService,
        ClusterSettings clusterSettings,
        IndicesService indicesService,
        long virtualBlockCacheBytes
    ) {
        this.settings = settings;
        this.nodeEnvironment = nodeEnvironment;
        this.unifiedCacheService = unifiedCacheService;
        this.fileCacheSettings = new FileCacheSettings(settings, clusterSettings);
        this.indicesService = indicesService;
        this.virtualBlockCacheBytes = virtualBlockCacheBytes;
    }

    /**
     * Creates the appropriate FsService implementation based on node type.
     *
     * @return FsService instance
     */
    public FsService createFsService() {
        if (DiscoveryNode.isWarmNode(settings)) {
            return new WarmFsService(
                settings,
                nodeEnvironment,
                fileCacheSettings,
                indicesService,
                unifiedCacheService,
                virtualBlockCacheBytes
            );
        }
        // Non-warm nodes: no block cache; unifiedCacheService may be null.
        return new FsService(settings, nodeEnvironment,
            unifiedCacheService != null ? unifiedCacheService.fileCache() : null);
    }
}
