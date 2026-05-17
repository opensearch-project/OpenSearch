/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor.fs;

import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.NodeCacheService;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Functional tests for {@link FsServiceProvider}.
 */
public class FsServiceProviderTests extends OpenSearchTestCase {

    /**
     * On a non-warm node (no WARM_ROLE in settings), the provider must return a plain
     * {@link FsService} — NOT a {@link WarmFsService}.
     */
    public void testCreateFsServiceReturnsStandardFsServiceOnNonWarmNode() throws Exception {
        Settings settings = Settings.EMPTY;  // no warm role
        ClusterSettings clusterSettings = new ClusterSettings(settings, BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = mock(IndicesService.class);
        NodeCacheService orchestrator = null;  // absent on non-warm nodes

        try (var nodeEnv = newNodeEnvironment(settings)) {
            FsServiceProvider provider = new FsServiceProvider(settings, nodeEnv, orchestrator, clusterSettings, indicesService);
            FsService fsService = provider.createFsService();

            assertNotNull(fsService);
            assertFalse("Non-warm node must NOT produce a WarmFsService", fsService instanceof WarmFsService);
        }
    }

    /**
     * On a warm node, the provider must return a {@link WarmFsService}.
     */
    public void testCreateFsServiceReturnsWarmFsServiceOnWarmNode() throws Exception {
        Settings settings = Settings.builder()
            .put("node.roles", DiscoveryNodeRole.WARM_ROLE.roleName())
            .put("node.search.cache.size", "1gb")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = mock(IndicesService.class);

        FileCache fileCache = mock(FileCache.class);
        when(fileCache.capacity()).thenReturn(1024L * 1024 * 1024);
        when(fileCache.usage()).thenReturn(0L);

        NodeCacheService orchestrator = mock(NodeCacheService.class);
        when(orchestrator.fileCache()).thenReturn(fileCache);
        when(orchestrator.blockCacheCapacityBytes()).thenReturn(0L);
        when(orchestrator.virtualBlockCacheBytes()).thenReturn(0L);
        when(orchestrator.cacheUtilizedBytes()).thenReturn(0L);

        try (var nodeEnv = newNodeEnvironment(settings)) {
            FsServiceProvider provider = new FsServiceProvider(settings, nodeEnv, orchestrator, clusterSettings, indicesService);
            FsService fsService = provider.createFsService();

            assertNotNull(fsService);
            assertTrue(
                "Warm node must produce a WarmFsService but got: " + fsService.getClass().getSimpleName(),
                fsService instanceof WarmFsService
            );
        }
    }

    /**
     * On a non-warm node with a non-null orchestrator (mixed-node scenario),
     * the provider still returns a standard FsService since the node is not warm.
     */
    public void testCreateFsServiceReturnsStandardFsServiceWhenOrchestratorPresentButNotWarmNode() throws Exception {
        Settings settings = Settings.EMPTY;  // data node, not warm
        ClusterSettings clusterSettings = new ClusterSettings(settings, BUILT_IN_CLUSTER_SETTINGS);
        IndicesService indicesService = mock(IndicesService.class);

        FileCache fileCache = mock(FileCache.class);
        when(fileCache.capacity()).thenReturn(100L * 1024 * 1024);
        when(fileCache.usage()).thenReturn(0L);

        NodeCacheService orchestrator = mock(NodeCacheService.class);
        when(orchestrator.fileCache()).thenReturn(fileCache);

        try (var nodeEnv = newNodeEnvironment(settings)) {
            FsServiceProvider provider = new FsServiceProvider(settings, nodeEnv, orchestrator, clusterSettings, indicesService);
            FsService fsService = provider.createFsService();

            assertNotNull(fsService);
            assertFalse(
                "Non-warm node must NOT produce WarmFsService even when orchestrator is present",
                fsService instanceof WarmFsService
            );
        }
    }
}
