/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class CompositeDirectoryIT extends RemoteStoreBaseIntegTestCase {
    public void testCompositeDirectory() throws Exception {
        Settings settings = Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.WRITEABLE_REMOTE_INDEX, "true")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), "partial")
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        boolean indexServiceFound = false;
        String[] nodes = internalCluster().getNodeNames();
        for (String node : nodes) {
            IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexService(resolveIndex("test-idx-1"));
            if (indexService == null) {
                continue;
            }
            IndexShard shard = indexService.getShardOrNull(0);
            Directory directory = (((FilterDirectory) (((FilterDirectory) (shard.store().directory())).getDelegate())).getDelegate());
            assertTrue(directory instanceof CompositeDirectory);
            indexServiceFound = true;
        }
        assertTrue(indexServiceFound);
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        assertEquals(ReplicationType.SEGMENT.toString(), indexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals("true", indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals(REPOSITORY_NAME, indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals(REPOSITORY_2_NAME, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
        assertEquals("partial", indexSettings.get("index.store.locality"));

        ensureGreen("test-idx-1");
        indexData(10, false, "test-idx-1");
        ensureGreen("test-idx-1");
    }
}
