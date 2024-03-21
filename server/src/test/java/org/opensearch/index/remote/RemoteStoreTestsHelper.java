/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;

import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Helper functions for Remote Store tests
 */
public class RemoteStoreTestsHelper {
    static IndexShard createIndexShard(ShardId shardId, boolean remoteStoreEnabled) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, String.valueOf(remoteStoreEnabled))
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        Store store = mock(Store.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.store()).thenReturn(store);
        return indexShard;
    }

    public static IndexSettings createIndexSettings(boolean remote) {
        IndexSettings indexSettings;
        if (remote) {
            Settings nodeSettings = Settings.builder()
                .put("node.name", "xyz")
                .put("node.attr.remote_store.translog.repository", "seg_repo")
                .build();
            indexSettings = IndexSettingsModule.newIndexSettings(new Index("test_index", "_na_"), Settings.EMPTY, nodeSettings);
        } else {
            indexSettings = IndexSettingsModule.newIndexSettings("test_index", Settings.EMPTY);
        }
        return indexSettings;
    }

    public static ShardRouting createShardRouting(boolean isPrimary, boolean remote) {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            randomAlphaOfLength(4),
            isPrimary,
            ShardRoutingState.STARTED
        );
        shardRouting.setAssignedToRemoteStoreNode(remote);
        return shardRouting;
    }
}
