/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.refresh;

import org.opensearch.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.opensearch.action.resync.TransportResyncReplicationAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexSettings;
import static org.opensearch.index.remote.RemoteStoreTestsHelper.createShardRouting;

public class TransportShardRefreshActionTests extends OpenSearchTestCase {

    public void testGetReplicationModeWithRemoteTranslog() {
        TransportShardRefreshAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(true);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));
        when(indexShard.routingEntry()).thenReturn(createShardRouting(true, true));
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithLocalTranslog() {
        TransportShardRefreshAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(false);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));
        when(indexShard.routingEntry()).thenReturn(createShardRouting(true, false));
        assertEquals(ReplicationMode.FULL_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeDuringMigration() {
        TransportShardRefreshAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(false);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));
        when(indexShard.routingEntry()).thenReturn(createShardRouting(true, true));
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeDuringMigrationOnRemotePrimary() {
        TransportShardRefreshAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(false);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));
        when(indexShard.routingEntry()).thenReturn(createShardRouting(true, true));
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeDuringMigrationOnDocrepReplica() {
        TransportShardRefreshAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(false);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));
        when(indexShard.routingEntry()).thenReturn(createShardRouting(false, false));
        assertEquals(ReplicationMode.FULL_REPLICATION, action.getReplicationMode(indexShard));
    }

    private TransportShardRefreshAction createAction() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new TransportShardRefreshAction(
            Settings.EMPTY,
            mock(TransportService.class),
            clusterService,
            mock(IndicesService.class),
            mock(ThreadPool.class),
            mock(ShardStateAction.class),
            mock(ActionFilters.class)
        );
    }
}
