/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.remotestore.RemoteSnapshotIT;
import org.opensearch.snapshots.SnapshotRestoreException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReplicaRestoreIT extends RemoteSnapshotIT {

    private static final String INDEX_NAME = "test-idx-1";
    private static final String RESTORED_INDEX_NAME = INDEX_NAME + "-restored";
    private static final String REPOSITORY_NAME = "test-repo";
    private static final String SNAPSHOT_NAME = "test-snapshot";
    private static final String FS_REPOSITORY_TYPE = "fs";
    private static final int DOC_COUNT = 10;

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, true).build();
    }

    public void testSearchReplicaRestore_WhenSnapshotOnSegRep_RestoreOnDocRepWithSearchReplica() throws Exception {
        bootstrapIndexWithOutSearchReplicas(ReplicationType.SEGMENT);
        createRepoAndSnapshot(REPOSITORY_NAME, FS_REPOSITORY_TYPE, SNAPSHOT_NAME, INDEX_NAME);

        SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> restoreSnapshot(
                REPOSITORY_NAME,
                SNAPSHOT_NAME,
                INDEX_NAME,
                RESTORED_INDEX_NAME,
                Settings.builder()
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                    .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                    .build()
            )
        );
        assertTrue(exception.getMessage().contains(getSnapshotExceptionMessage(ReplicationType.SEGMENT, ReplicationType.DOCUMENT)));
    }

    public void testSearchReplicaRestore_WhenSnapshotOnSegRep_RestoreOnSegRepWithSearchReplica() throws Exception {
        bootstrapIndexWithOutSearchReplicas(ReplicationType.SEGMENT);
        createRepoAndSnapshot(REPOSITORY_NAME, FS_REPOSITORY_TYPE, SNAPSHOT_NAME, INDEX_NAME);

        restoreSnapshot(
            REPOSITORY_NAME,
            SNAPSHOT_NAME,
            INDEX_NAME,
            RESTORED_INDEX_NAME,
            Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1).build()
        );
        ensureYellowAndNoInitializingShards(RESTORED_INDEX_NAME);

        internalCluster().startSearchOnlyNode();

        ensureGreen(RESTORED_INDEX_NAME);
        assertEquals(1, getNumberOfSearchReplicas(RESTORED_INDEX_NAME));

        SearchResponse resp = client().prepareSearch(RESTORED_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(resp, DOC_COUNT);
    }

    public void testSearchReplicaRestore_WhenSnapshotOnSegRepWithSearchReplica_RestoreOnDocRep() {
        bootstrapIndexWithSearchReplicas();
        createRepoAndSnapshot(REPOSITORY_NAME, FS_REPOSITORY_TYPE, SNAPSHOT_NAME, INDEX_NAME);

        SnapshotRestoreException exception = expectThrows(
            SnapshotRestoreException.class,
            () -> restoreSnapshot(
                REPOSITORY_NAME,
                SNAPSHOT_NAME,
                INDEX_NAME,
                RESTORED_INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT).build()
            )
        );
        assertTrue(exception.getMessage().contains(getSnapshotExceptionMessage(ReplicationType.SEGMENT, ReplicationType.DOCUMENT)));
    }

    private void bootstrapIndexWithOutSearchReplicas(ReplicationType replicationType) throws InterruptedException {
        internalCluster().startNodes(2);

        Settings settings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, replicationType)
            .build();

        createIndex(INDEX_NAME, settings);
        indexRandomDocs(INDEX_NAME, DOC_COUNT);
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }

    private void bootstrapIndexWithSearchReplicas() {
        internalCluster().startNodes(2);
        internalCluster().startSearchOnlyNode();

        Settings settings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();

        createIndex(INDEX_NAME, settings);

        ensureGreen(INDEX_NAME);
        for (int i = 0; i < DOC_COUNT; i++) {
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", "bar").get();
        }
        flushAndRefresh(INDEX_NAME);
    }

    private void createRepoAndSnapshot(String repositoryName, String repositoryType, String snapshotName, String indexName) {
        createRepository(repositoryName, repositoryType, randomRepoPath().toAbsolutePath());
        createSnapshot(repositoryName, snapshotName, List.of(indexName));
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
        assertFalse("index [" + INDEX_NAME + "] should have been deleted", indexExists(INDEX_NAME));
    }

    private String getSnapshotExceptionMessage(ReplicationType snapshotReplicationType, ReplicationType restoreReplicationType) {
        return "snapshot was created with [index.replication.type] as ["
            + snapshotReplicationType
            + "]. "
            + "To restore with [index.replication.type] as ["
            + restoreReplicationType
            + "], "
            + "[index.number_of_search_only_replicas] must be set to [0]";
    }

    private int getNumberOfSearchReplicas(String index) {
        Metadata metadata = client().admin().cluster().prepareState().get().getState().metadata();
        return Integer.valueOf(metadata.index(index).getSettings().get(SETTING_NUMBER_OF_SEARCH_REPLICAS));
    }
}
