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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.indices.replication.common.ReplicationType;

public class SegmentReplicationIndexShardTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    public void testIgnoreShardIdle() throws Exception {
        try (ReplicationGroup shards = createGroup(1, settings, new NRTReplicationEngineFactory())) {
            shards.startAll();
            final IndexShard primary = shards.getPrimary();
            final IndexShard replica = shards.getReplicas().get(0);

            final int numDocs = shards.indexDocs(randomInt(10));
            primary.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs);

            primary.scheduledRefresh();
            replica.scheduledRefresh();

            primary.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));
            replica.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));

            // Update the search_idle setting, this will put both shards into search idle.
            Settings updatedSettings = Settings.builder()
                .put(settings)
                .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO)
                .build();
            primary.indexSettings().getScopedSettings().applySettings(updatedSettings);
            replica.indexSettings().getScopedSettings().applySettings(updatedSettings);

            primary.scheduledRefresh();
            replica.scheduledRefresh();

            // Shards without segrep will register a new RefreshListener on the engine and return true when registered,
            // assert with segrep enabled that awaitShardSearchActive does not register a listener.
            primary.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));
            replica.awaitShardSearchActive(b -> assertFalse("A new RefreshListener should not be registered", b));
        }
    }
}
