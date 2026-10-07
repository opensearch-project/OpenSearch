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
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.translog.Translog;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Batched translog appends are on by default for remote-store segment-replication indexes. A single bulk mixing
 * indexes, updates and deletes of the same ids, with dynamic mapping updates arriving part way through, must leave the
 * shard in request order: visible state, realtime GETs and the translog replayed by a node restart all agree.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreBatchedTranslogIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX = "batched-tlog";
    private static final int DOCS = 300;

    public void testMixedBulkWithUpdatesDeletesAndDynamicMappingsSurvivesRestart() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNodes(1).get(0);
        createIndex(
            INDEX,
            Settings.builder()
                .put(remoteStoreIndexSettings(0))
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                .build()
        );
        ensureGreen(INDEX);
        assertTrue(
            "batching must be the default on an eligible index",
            IndexSettings.INDEX_TRANSLOG_BATCH_APPEND_ENABLED_SETTING.get(
                client().admin().indices().prepareGetSettings(INDEX).get().getIndexToSettings().get(INDEX)
            )
        );

        // One shard bulk, in request order: index every id, then update every third, then delete every fifth. Every
        // fiftieth document introduces a new field, so dynamic mapping updates interrupt the batch several times.
        indexMixedBulk();

        // Realtime GETs before any refresh: every acknowledged operation is already observable.
        verifyState(INDEX, DOCS);
        refresh(INDEX);
        verifyState(INDEX, DOCS);
        verifyDynamicMappings();

        // Restart the only data node: the shard recovers from remote segments plus the remote translog, which replays
        // exactly the batched and inline operations in order.
        internalCluster().restartNode(dataNode);
        ensureGreen(INDEX);
        verifyState(INDEX, DOCS);
        verifyDynamicMappings();
    }

    /**
     * A snapshot taken right after an unrefreshed batched bulk must capture every acknowledged operation. Snapshotting
     * flushes the shard; the flush drains the open chunks before the Lucene commit, so the commit's local checkpoint
     * never advertises an operation the translog lacks, and the remote translog carries whatever the commit does not.
     * Restoring the snapshot (shallow or full copy, chosen at random) must reproduce the state exactly.
     */
    public void testUnrefreshedBatchedBulkIsCapturedBySnapshotAndRestore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createIndex(
            INDEX,
            Settings.builder()
                .put(remoteStoreIndexSettings(0))
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                .build()
        );
        ensureGreen(INDEX);

        final boolean shallowCopy = randomBoolean();
        final String repo = "batched-tlog-snapshots";
        Settings.Builder repoSettings = Settings.builder().put("location", randomRepoPath().toAbsolutePath());
        if (shallowCopy) {
            repoSettings.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true);
        }
        createRepository(repo, "fs", repoSettings);

        indexMixedBulk();
        // No refresh has happened: the chunks have been appended and synced, Lucene is uncommitted.
        verifyState(INDEX, DOCS);

        CreateSnapshotResponse snapshot = client().admin()
            .cluster()
            .prepareCreateSnapshot(repo, "snap")
            .setIndices(INDEX)
            .setWaitForCompletion(true)
            .get();
        assertThat(snapshot.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshot.getSnapshotInfo().successfulShards(), equalTo(snapshot.getSnapshotInfo().totalShards()));

        final String restored = INDEX + "-restored";
        RestoreSnapshotResponse restore = client().admin()
            .cluster()
            .prepareRestoreSnapshot(repo, "snap")
            .setWaitForCompletion(true)
            .setIndices(INDEX)
            .setRenamePattern(INDEX)
            .setRenameReplacement(restored)
            .get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restored);
        verifyState(restored, DOCS);
        verifyState(INDEX, DOCS);
    }

    /**
     * One shard bulk, in request order: index every id, then update every third, then delete every fifth. Every
     * fiftieth document introduces a new field, so dynamic mapping updates interrupt the batch several times.
     */
    private void indexMixedBulk() {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            if (i % 50 == 0) {
                bulk.add(client().prepareIndex(INDEX).setId(id(i)).setSource("value", i, "dyn" + i, "x"));
            } else {
                bulk.add(client().prepareIndex(INDEX).setId(id(i)).setSource("value", i));
            }
        }
        for (int i = 0; i < DOCS; i += 3) {
            bulk.add(client().prepareUpdate(INDEX, id(i)).setDoc("value", i * 10));
        }
        for (int i = 0; i < DOCS; i += 5) {
            bulk.add(client().prepareDelete(INDEX, id(i)));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }

    private static String id(int i) {
        return "doc-" + i;
    }

    private void verifyState(String index, int docs) {
        int live = 0;
        for (int i = 0; i < docs; i++) {
            GetResponse get = client().prepareGet(index, id(i)).setRealtime(true).get();
            if (i % 5 == 0) {
                assertFalse("doc " + i + " was deleted", get.isExists());
                continue;
            }
            live++;
            assertTrue("doc " + i + " must exist", get.isExists());
            int expected = i % 3 == 0 ? i * 10 : i;
            assertThat("doc " + i, ((Number) get.getSourceAsMap().get("value")).intValue(), equalTo(expected));
        }
        refresh(index);
        assertHitCount(client().prepareSearch(index).setSize(0).get(), live);
    }

    @SuppressWarnings("unchecked")
    private void verifyDynamicMappings() {
        MappingMetadata mapping = client().admin().indices().prepareGetMappings(INDEX).get().getMappings().get(INDEX);
        Map<String, Object> properties = (Map<String, Object>) mapping.getSourceAsMap().get("properties");
        for (int i = 0; i < DOCS; i += 50) {
            assertTrue("dynamic field dyn" + i + " must be mapped", properties.containsKey("dyn" + i));
        }
    }
}
