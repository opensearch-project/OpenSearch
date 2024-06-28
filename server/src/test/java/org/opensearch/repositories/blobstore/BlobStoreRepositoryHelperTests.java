/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.RepositoryPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BlobStoreRepositoryHelperTests extends OpenSearchSingleNodeTestCase {

    static final String REPO_TYPE = "fsLike";

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(FsLikeRepoPlugin.class);
    }

    protected String[] getLockFilesInRemoteStore(String remoteStoreIndex, String remoteStoreRepository) throws IOException {
        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository remoteStorerepository = (BlobStoreRepository) repositoriesService.repository(remoteStoreRepository);
        BlobPath shardLevelBlobPath = getShardLevelBlobPath(
            client(),
            remoteStoreIndex,
            remoteStorerepository.basePath(),
            "0",
            SEGMENTS,
            LOCK_FILES
        );
        BlobContainer blobContainer = remoteStorerepository.blobStore().blobContainer(shardLevelBlobPath);
        try (RemoteBufferedOutputDirectory lockDirectory = new RemoteBufferedOutputDirectory(blobContainer)) {
            return Arrays.stream(lockDirectory.listAll())
                .filter(lock -> lock.endsWith(".lock") || lock.endsWith(".v2_lock"))
                .toArray(String[]::new);
        }
    }

    // the reason for this plug-in is to drop any assertSnapshotOrGenericThread as mostly all access in this test goes from test threads
    public static class FsLikeRepoPlugin extends Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            return Collections.singletonMap(
                REPO_TYPE,
                (metadata) -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings) {
                    @Override
                    protected void assertSnapshotOrGenericThread() {
                        // eliminate thread name check as we access blobStore on test/main threads
                    }
                }
            );
        }
    }

    protected void createRepository(Client client, String repoName) {
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repoName)
            .setType(REPO_TYPE)
            .setSettings(
                Settings.builder().put(node().settings()).put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    protected void createRepository(Client client, String repoName, Settings repoSettings) {
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(repoName)
            .setType(REPO_TYPE)
            .setSettings(repoSettings)
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    protected void updateRepository(Client client, String repoName, Settings repoSettings) {
        createRepository(client, repoName, repoSettings);
    }

    protected Settings getRemoteStoreBackedIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
            .put("index.refresh_interval", "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    protected SnapshotInfo createSnapshot(String repositoryName, String snapshot, List<String> indices) {
        logger.info("--> creating snapshot [{}] of {} in [{}]", snapshot, indices, repositoryName);

        final CreateSnapshotResponse response = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshot)
            .setIndices(indices.toArray(Strings.EMPTY_ARRAY))
            .setWaitForCompletion(true)
            .get();
        SnapshotInfo snapshotInfo = response.getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.failedShards(), equalTo(0));
        return snapshotInfo;
    }

    protected void indexDocuments(Client client, String indexName) {
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            client.prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
    }

    protected IndexSettings getIndexSettings(String indexName) {
        final IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        final IndexService indexService = indicesService.indexService(resolveIndex(indexName));
        return indexService.getIndexSettings();
    }

}
