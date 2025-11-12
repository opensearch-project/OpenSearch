/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergedSegmentWarmerFactoryTests extends OpenSearchTestCase {

    private TransportService transportService;
    private RecoverySettings recoverySettings;
    private ClusterService clusterService;
    private MergedSegmentWarmerFactory factory;
    private IndexShard indexShard;
    private ShardId shardId;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transportService = mock(TransportService.class);
        recoverySettings = mock(RecoverySettings.class);
        clusterService = mock(ClusterService.class);
        factory = new MergedSegmentWarmerFactory(transportService, recoverySettings, clusterService);

        indexShard = mock(IndexShard.class);
        shardId = new ShardId(new Index("test", "uuid"), 0);
        when(indexShard.shardId()).thenReturn(shardId);
    }

    @Override
    public void tearDown() throws Exception {
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
        super.tearDown();
    }

    public void testGetWithSegmentReplication() {
        IndexSettings indexSettings = createIndexSettings(
            false,
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build()
        );
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        IndexWriter.IndexReaderWarmer warmer = factory.get(indexShard);

        assertNotNull(warmer);
        assertTrue(warmer instanceof MergedSegmentWarmer);
    }

    public void testGetWithRemoteStoreEnabled() {
        IndexSettings indexSettings = createIndexSettings(
            true,
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT).build()
        );
        when(indexShard.indexSettings()).thenReturn(indexSettings);

        IndexWriter.IndexReaderWarmer warmer = factory.get(indexShard);

        assertNotNull(warmer);
        assertTrue(warmer instanceof MergedSegmentWarmer);
    }

    public void testGetWithDocumentReplication() {
        IndexSettings indexSettings = createIndexSettings(
            false,
            Settings.builder().put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT).build()
        );

        when(indexShard.indexSettings()).thenReturn(indexSettings);
        IndexWriter.IndexReaderWarmer warmer = factory.get(indexShard);
        assertNull(warmer);
    }

    public static IndexSettings createIndexSettings(boolean remote, Settings settings) {
        IndexSettings indexSettings;
        if (remote) {
            Settings nodeSettings = Settings.builder()
                .put("node.name", "xyz")
                .put("node.attr.remote_store.translog.repository", "translog_repo")
                .put("node.attr.remote_store.segment.repository", "seg_repo")
                .put("node.attr.remote_store.enabled", "true")
                .build();
            indexSettings = IndexSettingsModule.newIndexSettings(new Index("test_index", "_na_"), settings, nodeSettings);
        } else {
            indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        }
        return indexSettings;
    }
}
