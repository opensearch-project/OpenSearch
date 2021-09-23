/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autorecover;

import org.hamcrest.Matchers;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.snapshots.AbstractSnapshotIntegTestCase;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.BlockClusterStateProcessing;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexSettingUpdateIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .build();
    }

    //Test case for Happy case scenario of index write with no replica
    public void testIndexWrite() {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String indexName = "index-test";
        long beforeTime = client().threadPool().absoluteTimeInMillis();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen();
        createIndexWithContent(indexName);

        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);

        // Validate auto_restore index setting is been updated
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));
    }

    //Test case for Happy case scenario of index write with no replica
    public void testIndexWriteWithReplica() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String dataNode1 = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final String indexName = "index-test";
        final String snapshot = "snap";
        long beforeTime = client().threadPool().absoluteTimeInMillis();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true)
            .put("snapshot.auto_restore_red_indices_repository", repoName)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen();
        Settings setting = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build();
        createIndexWithContent(indexName, setting);
        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);

        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));
    }

    //Test index write fails when index setting update fails
    public void testIndexWriteSettingUpdateFail() throws IOException {
        final String masterNode =internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String indexName = "index-test";

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        ensureGreen();
        createIndex(indexName);

        // Block cluster state processing on the master
        BlockClusterStateProcessing disruption = new BlockClusterStateProcessing(masterNode, random());
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        try {
            IndexResponse response = index(indexName, "_doc", "9", "value", "expectedValue");
            fail("Should have failed because index setting update disrupted ");
        } catch (Exception e) {

            assert(e.getMessage().contains("update-setting"));
            IndexSettings indexSettings = getIndexSettings(indexName, dataNode);

            // Validate auto_restore index setting is not updated
            assertFalse(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.exists(indexSettings.getSettings()));
            assertFalse(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.exists(indexSettings.getSettings()));
        }
    }


    //Test no index setting update (related to autorecover red indices) happens if feature flag `snapshot.auto_restore_red_indices_enable`
    //is disabled during index write
    public void testIndexWriteAutoRestoreDisabled() {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String indexName = "index-test";

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", false));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen();
        createIndexWithContent(indexName);
        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);

        // Ensure index settings is not updated if feature flag is disabled
        assertFalse(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.exists(indexSettings.getSettings()));
        assertFalse(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.exists(indexSettings.getSettings()));
    }

    //Test no index setting update (related to autorecover red indices) happens if feature flag `snapshot.auto_restore_red_indices_enable`
    //is disabled during snapshot completion
    public void testSnapshotAutoRestoreDisabled() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String indexName = "index-test";
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String snapshot = "snap";

        ensureGreen();
        createIndexWithContent(indexName);
        final ActionFuture<CreateSnapshotResponse> createSnapshotFuture =
            startFullSnapshot(repoName, snapshot);
        assertSuccessful(createSnapshotFuture);
        assertThat(createSnapshotFuture.isDone(), is(true));
        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);

        // Ensure index settings is not updated during snapshot if feature flag is disabled
        assertFalse(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.exists(indexSettings.getSettings()));
        assertFalse(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.exists(indexSettings.getSettings()));
    }

    //Test index setting update when index write completes before snapshot start
    public void testIndexWriteBeforeSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexName = "index-test";
        final String snapshot = "snap";
        long beforeTime = client().threadPool().absoluteTimeInMillis();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true)
            .put("snapshot.auto_restore_red_indices_repository", repoName)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen();
        createIndexWithContent(indexName);
        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);

        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));

        final ActionFuture<CreateSnapshotResponse> createSnapshotFuture =
            startFullSnapshot(repoName, snapshot);
        assertSuccessful(createSnapshotFuture);
        assertThat(createSnapshotFuture.isDone(), is(true));
        indexSettings = getIndexSettings(indexName, dataNode);

        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("true"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));
    }

    //Test index setting update when index write happens when snapshot is in_progress
    public void testIndexWriteWithSnapshotInProgress() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexName = "index-test";
        final String firstSnapshot = "snap1";
        final String secondSnapshot = "snap2";
        String expectedValue = "expected";
        // Write a document
        String docId = Integer.toString(randomInt());
        long beforeTime = client().threadPool().absoluteTimeInMillis();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true)
            .put("snapshot.auto_restore_red_indices_repository", repoName)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen();
        createIndexWithContent(indexName);
        final ActionFuture<CreateSnapshotResponse> createSnapshotFuture =
            startFullSnapshot(repoName, firstSnapshot);
        assertSuccessful(createSnapshotFuture);

        assertThat(createSnapshotFuture.isDone(), is(true));
        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("true"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));

        long beforeTime1 = client().threadPool().absoluteTimeInMillis();

        // Will make snapshot in progress state
        blockMasterOnWriteIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> createSnapshotFuture2 =
            startFullSnapshot(repoName, secondSnapshot);
        waitForBlock(masterNode, repoName, TimeValue.timeValueSeconds(30L));

        assertThat(createSnapshotFuture2.isDone(), is(false));
        IndexResponse response = index(indexName, "_doc", docId, "value", expectedValue);
        unblockNode(repoName, masterNode);
        assertSuccessful(createSnapshotFuture2);
        indexSettings = getIndexSettings(indexName, dataNode);
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime1));
    }

    //Test index setting update when index write happens after snapshot completion
    public void testIndexWriteAfterSnapshotComplete() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexName = "index-test";
        final String firstSnapshot = "snap1";
        String expectedValue = "expected";
        // Write a document
        String docId = Integer.toString(randomInt());
        long beforeTime = client().threadPool().absoluteTimeInMillis();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true)
            .put("snapshot.auto_restore_red_indices_repository", repoName)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen();
        createIndexWithContent(indexName);
        final ActionFuture<CreateSnapshotResponse> createSnapshotFuture =
            startFullSnapshot(repoName, firstSnapshot);
        assertSuccessful(createSnapshotFuture);

        assertThat(createSnapshotFuture.isDone(), is(true));
        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("true"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));

        long beforeTime1 = client().threadPool().absoluteTimeInMillis();
        // Write to index
        IndexResponse response = index(indexName, "_doc", docId, "value", expectedValue);
        indexSettings = getIndexSettings(indexName, dataNode);
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime1));
    }

    //Test index setting update when snapshot of index ends in FAILED state
    public void testSnapshotFailure() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexName = "index-test";
        final String snapshot = "snap";
        long beforeTime = client().threadPool().absoluteTimeInMillis();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true)
            .put("snapshot.auto_restore_red_indices_repository", repoName)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        ensureGreen();
        createIndexWithContent(indexName);
        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);

        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));

        final ActionFuture<CreateSnapshotResponse> createSnapshotFuture =
            startFullSnapshotBlockedOnDataNode( snapshot, repoName, dataNode);

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDeleteSnapshot(repoName, snapshot);
        awaitNDeletionsInProgress(1);

        unblockNode(repoName, dataNode);
        final SnapshotInfo firstSnapshotInfo = createSnapshotFuture.get().getSnapshotInfo();
        assertThat(firstSnapshotInfo.state(), Matchers.is(SnapshotState.FAILED));
        assertThat(firstSnapshotInfo.reason(), is("Snapshot was aborted by deletion"));

        indexSettings = getIndexSettings(indexName, dataNode);
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));

        awaitNDeletionsInProgress(0);
    }

    //Tests index setting update when snapshot in PARTIAL state,
    //i.e for some index snapshot is successful and for some it failed.
    public void testSnapshotPartial() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String indexName = "index-test";
        final String snapshot = "snap";
        long beforeTime = client().threadPool().absoluteTimeInMillis();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder()
            .put("snapshot.auto_restore_red_indices_enable", true)
            .put("snapshot.auto_restore_red_indices_repository", repoName)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        createIndex(indexName, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(indexName);
        index(indexName, "_doc", "some_id", "foo", "bar");


        IndexSettings indexSettings = getIndexSettings(indexName, dataNode);
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));

        final ActionFuture<CreateSnapshotResponse> createSnapshotFuture =
            startFullSnapshotBlockedOnDataNode( snapshot, repoName, dataNode);

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDeleteSnapshot(repoName, snapshot);
        awaitNDeletionsInProgress(1);

        unblockNode(repoName, dataNode);
        final SnapshotInfo firstSnapshotInfo = createSnapshotFuture.get().getSnapshotInfo();
        assertThat(firstSnapshotInfo.state(), is(SnapshotState.FAILED));
        assertThat(firstSnapshotInfo.reason(), is("Snapshot was aborted by deletion"));

        indexSettings = getIndexSettings(indexName, dataNode);
        assertThat(indexSettings.getSettings().get("index.auto_restore.clean_to_restore_from_snapshot"), equalTo("false"));
        assertThat(indexSettings.getSettings().getAsLong("index.auto_restore.setting_update_time", -1L), greaterThan(beforeTime));

        awaitNDeletionsInProgress(0);
    }

    private IndexSettings getIndexSettings(String indexName, String nodeName) {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        final IndexService indexService = indicesService.indexService(resolveIndex(indexName));
        return indexService.getIndexSettings();
    }

    private void awaitNDeletionsInProgress(int count) throws Exception {
        logger.info("--> wait for [{}] deletions to show up in the cluster state", count);
        awaitClusterState(state ->
            state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).getEntries().size() == count);
    }
}
