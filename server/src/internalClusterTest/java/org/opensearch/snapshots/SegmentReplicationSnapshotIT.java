/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.BeforeClass;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationSnapshotIT extends AbstractSnapshotIntegTestCase {
    private static final String INDEX_NAME = "test-segrep-idx";
    private static final String RESTORED_INDEX_NAME = INDEX_NAME + "-restored";
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 1;
    private static final int DOC_COUNT = 1010;

    private static final String REPOSITORY_NAME = "test-segrep-repo";
    private static final String SNAPSHOT_NAME = "test-segrep-snapshot";

    @BeforeClass
    public static void assumeFeatureFlag() {
        assumeTrue("Segment replication Feature flag is enabled", Boolean.parseBoolean(System.getProperty(FeatureFlags.REPLICATION_TYPE)));
    }

    public Settings segRepEnableIndexSettings() {
        return getShardSettings().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT).build();
    }

    public Settings docRepEnableIndexSettings() {
        return getShardSettings().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build();
    }

    public Settings.Builder getShardSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void ingestData(int docCount, String indexName) throws Exception {
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(docCount);
            waitForDocs(docCount, indexer);
            refresh(indexName);
        }
    }

    public void startClusterWithSettings(Settings indexSettings) throws Exception {
        // Create 2 node cluster
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);
        // Ingest data
        ingestData(DOC_COUNT, INDEX_NAME);
    }

    public void createSnapshot() {
        // Snapshot declaration
        Path absolutePath = randomRepoPath().toAbsolutePath();
        logger.info("Path [{}]", absolutePath);
        // Create snapshot
        createRepository(REPOSITORY_NAME, "fs", absolutePath);
        logger.info("Perform snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(true)
            .setIndices(INDEX_NAME)
            .get();
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
    }

    public RestoreSnapshotResponse restoreSnapshotWithSettings(Settings indexSettings) {
        RestoreSnapshotRequestBuilder builder = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPOSITORY_NAME, SNAPSHOT_NAME)
            .setWaitForCompletion(false)
            .setRenamePattern(INDEX_NAME)
            .setRenameReplacement(RESTORED_INDEX_NAME);
        if (indexSettings != null) {
            builder.setIndexSettings(indexSettings);
        }
        return builder.get();
    }

    public void testRestoreOnSegRep() throws Exception {
        startClusterWithSettings(segRepEnableIndexSettings());
        createSnapshot();
        // Delete index
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME));
        Thread.sleep(5000);
        assertFalse("index [" + INDEX_NAME + "] should have been deleted", indexExists(INDEX_NAME));

        logger.info("Restore from snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = restoreSnapshotWithSettings(null);

        // Assertions
        assertThat(restoreSnapshotResponse.status(), equalTo(RestStatus.ACCEPTED));
        logger.info("Ensure cluster is green");
        ensureGreen(RESTORED_INDEX_NAME);
        GetSettingsResponse settingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(RESTORED_INDEX_NAME))
            .get();
        assertEquals(settingsResponse.getSetting(RESTORED_INDEX_NAME, "index.replication.type"), "SEGMENT");
        SearchResponse resp = client().prepareSearch(RESTORED_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(resp, DOC_COUNT);
    }

    public void testRestoreOnSegRepDuringIngestion() throws Exception {
        startClusterWithSettings(segRepEnableIndexSettings());
        createSnapshot();
        // Delete index
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME));
        Thread.sleep(5000);
        assertFalse("index [" + INDEX_NAME + "] should have been deleted", indexExists(INDEX_NAME));

        logger.info("Restore from snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = restoreSnapshotWithSettings(null);

        // Assertions
        assertThat(restoreSnapshotResponse.status(), equalTo(RestStatus.ACCEPTED));
        logger.info("Ensure cluster is green");
        ingestData(5000, RESTORED_INDEX_NAME);
        ensureGreen(RESTORED_INDEX_NAME);
        GetSettingsResponse settingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(RESTORED_INDEX_NAME))
            .get();
        assertEquals(settingsResponse.getSetting(RESTORED_INDEX_NAME, "index.replication.type"), "SEGMENT");
        SearchResponse resp = client().prepareSearch(RESTORED_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(resp, DOC_COUNT);
    }

    public void testSnapshotOnDocRep_RestoreOnSegRep() throws Exception {
        startClusterWithSettings(docRepEnableIndexSettings());
        createSnapshot();
        // Delete index
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME));
        Thread.sleep(5000);

        logger.info("Restore from snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = restoreSnapshotWithSettings(segRepEnableIndexSettings());

        // Assertions
        assertThat(restoreSnapshotResponse.status(), equalTo(RestStatus.ACCEPTED));
        logger.info("Ensure cluster is green");
        ensureGreen(RESTORED_INDEX_NAME);
        GetSettingsResponse settingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(RESTORED_INDEX_NAME))
            .get();
        assertEquals(settingsResponse.getSetting(RESTORED_INDEX_NAME, "index.replication.type"), "SEGMENT");

        SearchResponse resp = client().prepareSearch(RESTORED_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(resp, DOC_COUNT);
    }

    public void testSnapshotOnSegRep_RestoreOnDocRep() throws Exception {
        startClusterWithSettings(segRepEnableIndexSettings());
        createSnapshot();
        // Delete index
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME));
        Thread.sleep(5000);

        logger.info("Restore from snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = restoreSnapshotWithSettings(docRepEnableIndexSettings());

        // Assertions
        assertThat(restoreSnapshotResponse.status(), equalTo(RestStatus.ACCEPTED));
        logger.info("Wait for green index health");
        ensureGreen(RESTORED_INDEX_NAME);
        GetSettingsResponse settingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(RESTORED_INDEX_NAME))
            .get();
        assertEquals(settingsResponse.getSetting(RESTORED_INDEX_NAME, "index.replication.type"), "DOCUMENT");
        SearchResponse resp = client().prepareSearch(RESTORED_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(resp, DOC_COUNT);
    }

    public void testSnapshotOnDocRep_RestoreOnDocRep() throws Exception {
        startClusterWithSettings(docRepEnableIndexSettings());
        createSnapshot();
        // Delete index
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME));
        Thread.sleep(5000);

        logger.info("Restore from snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = restoreSnapshotWithSettings(docRepEnableIndexSettings());

        // Assertions
        assertThat(restoreSnapshotResponse.status(), equalTo(RestStatus.ACCEPTED));
        logger.info("Ensure cluster is green");
        ensureGreen(RESTORED_INDEX_NAME);
        GetSettingsResponse settingsResponse = client().admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(RESTORED_INDEX_NAME))
            .get();
        assertEquals(settingsResponse.getSetting(RESTORED_INDEX_NAME, "index.replication.type"), "DOCUMENT");

        SearchResponse resp = client().prepareSearch(RESTORED_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(resp, DOC_COUNT);
    }
}
