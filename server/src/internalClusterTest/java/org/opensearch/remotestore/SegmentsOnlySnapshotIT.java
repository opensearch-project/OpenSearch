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
import org.opensearch.indices.IndicesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.junit.After;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Snapshotting a {@code segments_only} index and restoring it back into the same cluster, both as a full snapshot
 * and as a shallow one.
 *
 * <p>Shallow snapshots are the kind this mode can support: a shallow snapshot records pointers into the remote
 * segment store instead of copying segments into the snapshot repository, which needs the segments to be remote,
 * and never needs the operation stream, which is local here. The v2 flavour is deliberately not exercised, because
 * its restore path resolves a remote translog repository that a segments_only index does not have.
 *
 * <p>Restoring a document replication snapshot <em>into</em> this mode is a different scenario, covered by
 * {@link SegmentsOnlyRestoreIT}, because there the nodes have to start without the remote store attributes.
 */
public class SegmentsOnlySnapshotIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String SNAPSHOT_REPOSITORY_NAME = "test-snapshot-repo";
    private static final String INDEX_NAME = "segments-only-snapshot-idx";

    /** A full snapshot, which copies the segments into the snapshot repository rather than referencing them. */
    public void testFullSnapshotAndRestore() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = indexDocs(INDEX_NAME, 0, 40);
        refresh(INDEX_NAME);

        createSnapshotRepository(false);
        CreateSnapshotResponse snapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPOSITORY_NAME, "full-1")
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .get();
        assertEquals(SnapshotState.SUCCESS, snapshot.getSnapshotInfo().state());

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));

        RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPOSITORY_NAME, "full-1")
            .setWaitForCompletion(true)
            .get();
        assertEquals(0, restore.getRestoreInfo().failedShards());
        ensureGreen(INDEX_NAME);
        assertBusy(() -> assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs));
    }

    /**
     * A shallow snapshot, restored after the index it referenced was deleted. That delete must not take the remote
     * segments the snapshot points at with it, which is what the lock the snapshot acquires on the commit is for.
     */
    public void testShallowSnapshotIsTakenAndRestored() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = indexDocs(INDEX_NAME, 0, 40);
        // The snapshot records a commit generation, so the operations have to be in a commit and uploaded.
        flushAndRefresh(INDEX_NAME);

        createSnapshotRepository(true);
        CreateSnapshotResponse snapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPOSITORY_NAME, "shallow-1")
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo info = snapshot.getSnapshotInfo();
        assertEquals(SnapshotState.SUCCESS, info.state());
        assertEquals(info.totalShards(), info.successfulShards());
        assertTrue(
            "the snapshot must be shallow, otherwise this is not exercising the remote segment store at all",
            info.isRemoteStoreIndexShallowCopyEnabled()
        );

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
        assertFalse(indexExists(INDEX_NAME));

        RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPOSITORY_NAME, "shallow-1")
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .get();
        assertEquals(0, restore.getRestoreInfo().failedShards());
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docs);

        // The restored index must come back as segments_only rather than as something that expects a remote translog.
        GetSettingsResponse settings = client().admin().indices().prepareGetSettings(INDEX_NAME).get();
        assertEquals("true", settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_STORE_ENABLED));
        assertEquals(SEGMENT_REPOSITORY_NAME, settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertNull(
            "a restored segments_only index must not be given a translog repository",
            settings.getSetting(INDEX_NAME, IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY)
        );
        for (String node : internalCluster().getDataNodeNames()) {
            IndexService service = internalCluster().getInstance(IndicesService.class, node).indexService(resolveIndex(INDEX_NAME));
            if (service == null) {
                continue;
            }
            assertFalse("the restored translog must stay local", service.getShard(0).indexSettings().isTranslogDurableInRemoteStore());
        }
    }

    /**
     * A restored shard has to be a working shard and not just a readable one, so this keeps writing to it and then
     * restarts the cluster, which is the path the local translog has to survive.
     */
    public void testAShallowRestoredIndexAcceptsWritesAndSurvivesARestart() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);

        int docs = indexDocs(INDEX_NAME, 0, 30);
        flushAndRefresh(INDEX_NAME);

        createSnapshotRepository(true);
        CreateSnapshotResponse snapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(SNAPSHOT_REPOSITORY_NAME, "shallow-2")
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .get();
        assertEquals(SnapshotState.SUCCESS, snapshot.getSnapshotInfo().state());
        assertTrue(snapshot.getSnapshotInfo().isRemoteStoreIndexShallowCopyEnabled());

        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
        RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(SNAPSHOT_REPOSITORY_NAME, "shallow-2")
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .get();
        assertEquals(0, restore.getRestoreInfo().failedShards());
        ensureGreen(INDEX_NAME);

        // Write on top of the restored shard and leave the new operations unflushed, so only the local translog has
        // them, then restart.
        int total = indexDocs(INDEX_NAME, docs, 20);
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        String primaryNode = primaryNodeName(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), total);
    }

    private void createSnapshotRepository(boolean shallow) {
        Settings.Builder settings = Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), randomRepoPath().toAbsolutePath());
        if (shallow) {
            settings.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true);
        }
        assertAcked(
            client().admin().cluster().preparePutRepository(SNAPSHOT_REPOSITORY_NAME).setType(FsRepository.TYPE).setSettings(settings)
        );
    }

    /**
     * The framework only inspects the first repository when deciding whether to clean or delete, so the system
     * segment repository must be the only one left when the test ends - including when the test failed.
     */
    @After
    public void deleteSnapshotRepository() {
        boolean exists = client().admin()
            .cluster()
            .prepareGetRepositories()
            .get()
            .repositories()
            .stream()
            .anyMatch(repository -> SNAPSHOT_REPOSITORY_NAME.equals(repository.name()));
        if (exists) {
            assertAcked(client().admin().cluster().prepareDeleteRepository(SNAPSHOT_REPOSITORY_NAME));
        }
    }
}
