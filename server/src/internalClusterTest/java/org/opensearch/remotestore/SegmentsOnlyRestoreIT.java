/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Locale;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_MODE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Migrating a document replication cluster to {@code segments_only} is done by restoring a snapshot into a cluster
 * that is already running in that mode, so the restore has to convert the index as it lands: segment replication and a
 * segment repository, but no translog repository. This lives apart from {@link SegmentsOnlyRemoteStoreIT} because the
 * nodes have to start without the remote store attributes and only pick them up on restart.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentsOnlyRestoreIT extends OpenSearchIntegTestCase {

    private static final String SEGMENT_REPOSITORY_NAME = "test-segment-repo";
    private static final String SNAPSHOT_REPOSITORY_NAME = "test-snapshot-repo";
    private static final String SNAPSHOT_NAME = "snapshot-1";
    private static final String SOURCE_INDEX_NAME = "document-replication-idx";
    private static final String RESTORED_INDEX_NAME = "restored-idx";

    private Path segmentRepoPath;
    private Path snapshotRepoPath;
    private volatile boolean segmentsOnly = false;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            snapshotRepoPath = randomRepoPath().toAbsolutePath();
        }
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        if (segmentsOnly) {
            settings.put(segmentsOnlyAttributes());
        }
        return settings.build();
    }

    private Settings segmentsOnlyAttributes() {
        String typeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            SEGMENT_REPOSITORY_NAME
        );
        String settingsPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            SEGMENT_REPOSITORY_NAME
        );
        return Settings.builder()
            .put("node.attr." + REMOTE_STORE_MODE_KEY, "segments_only")
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, SEGMENT_REPOSITORY_NAME)
            .put(typeKey, ReloadableFsRepository.TYPE)
            .put(settingsPrefix + "location", segmentRepoPath)
            .build();
    }

    public void testDocumentReplicationSnapshotIsRestoredAsSegmentsOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        assertAcked(
            prepareCreate(SOURCE_INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            )
        );
        ensureGreen(SOURCE_INDEX_NAME);

        int docs = randomIntBetween(20, 50);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(SOURCE_INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        flush(SOURCE_INDEX_NAME);

        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(SNAPSHOT_REPOSITORY_NAME)
                .setType("fs")
                .setSettings(Settings.builder().put("location", snapshotRepoPath))
        );
        CreateSnapshotResponse snapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(SOURCE_INDEX_NAME)
            .get();
        assertEquals(SnapshotState.SUCCESS, snapshot.getSnapshotInfo().state());
        assertAcked(client().admin().indices().prepareDelete(SOURCE_INDEX_NAME));

        // The same nodes come back carrying the segments_only attributes, which is what a migration looks like from
        // the restore's point of view: a cluster already running in the target mode.
        segmentsOnly = true;
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return segmentsOnlyAttributes();
            }
        });
        ensureStableCluster(3);

        RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(SOURCE_INDEX_NAME)
            .setRenamePattern(SOURCE_INDEX_NAME)
            .setRenameReplacement(RESTORED_INDEX_NAME)
            .get();
        assertEquals(0, restore.getRestoreInfo().failedShards());
        ensureGreen(RESTORED_INDEX_NAME);

        GetSettingsResponse settings = client().admin().indices().prepareGetSettings(RESTORED_INDEX_NAME).get();
        assertEquals(
            "a document replication index must be restored as segment replicated",
            ReplicationType.SEGMENT.toString(),
            settings.getSetting(RESTORED_INDEX_NAME, IndexMetadata.SETTING_REPLICATION_TYPE)
        );
        assertEquals("true", settings.getSetting(RESTORED_INDEX_NAME, IndexMetadata.SETTING_REMOTE_STORE_ENABLED));
        assertEquals(
            SEGMENT_REPOSITORY_NAME,
            settings.getSetting(RESTORED_INDEX_NAME, IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY)
        );
        assertNull(
            "a restored segments_only index must not be given a translog repository",
            settings.getSetting(RESTORED_INDEX_NAME, IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY)
        );

        for (String node : internalCluster().getDataNodeNames()) {
            IndexService indexService = internalCluster().getInstance(IndicesService.class, node)
                .indexService(resolveIndex(RESTORED_INDEX_NAME));
            if (indexService == null) {
                continue;
            }
            for (IndexShard shard : indexService) {
                assertFalse("the restored translog must stay local", shard.indexSettings().hasRemoteTranslog());
                assertTrue("the restored segments must be remote backed", shard.indexSettings().isRemoteStoreEnabled());
            }
        }

        assertHitCount(client().prepareSearch(RESTORED_INDEX_NAME).setSize(0).get(), docs);

        // The framework only inspects the first repository when deciding whether to clean or delete, so the system
        // segment repository must be the only one left when the test ends.
        assertAcked(client().admin().cluster().prepareDeleteRepository(SNAPSHOT_REPOSITORY_NAME));
    }
}
