/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;

public class SegmentReplicationWithRemoteIndexShardTests extends OpenSearchIndexLevelReplicationTestCase {
    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
        .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "temp-fs")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "temp-fs")
        .build();

    public void testReplicaSyncingFromRemoteStore() throws IOException {
        ReplicationGroup shards = createGroup(1, settings, indexMapping, new NRTReplicationEngineFactory(), createTempDir());
        final IndexShard primaryShard = shards.getPrimary();
        final IndexShard replicaShard = shards.getReplicas().get(0);
        shards.startPrimary();
        shards.startAll();
        indexDoc(primaryShard, "_doc", "1");
        indexDoc(primaryShard, "_doc", "2");
        primaryShard.refresh("test");
        assertDocs(primaryShard, "1", "2");
        flushShard(primaryShard);

        replicaShard.syncSegmentsFromRemoteSegmentStore(true, true, false);
        assertDocs(replicaShard, "1", "2");
        closeShards(primaryShard, replicaShard);
    }
}
