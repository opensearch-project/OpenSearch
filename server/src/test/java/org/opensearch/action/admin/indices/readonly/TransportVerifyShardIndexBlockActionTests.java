/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

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

import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexSettings;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportVerifyShardIndexBlockActionTests extends OpenSearchTestCase {

    public void testGetReplicationModeWithRemoteTranslog() {
        TransportVerifyShardIndexBlockAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithLocalTranslog() {
        TransportVerifyShardIndexBlockAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));
        assertEquals(ReplicationMode.FULL_REPLICATION, action.getReplicationMode(indexShard));
    }

    private TransportVerifyShardIndexBlockAction createAction() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new TransportVerifyShardIndexBlockAction(
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
