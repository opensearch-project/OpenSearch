/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.blobstore;

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.snapshots.blobstore.RemoteStoreShardShallowCopySnapshot;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotsService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY;
import static org.opensearch.repositories.blobstore.BlobStoreRepository.SHALLOW_SNAPSHOT_V2;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryRemoteIndexTests extends BlobStoreRepositoryHelperTests {
    @Override
    protected Settings nodeSettings() {
        Path tempDir = createTempDir();
        return Settings.builder()
            .put(super.nodeSettings())
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(buildRemoteStoreNodeAttributes("test-rs-repo", tempDir.resolve("repo")))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo"))
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), tempDir.getParent())
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .build();
    }

    private Settings buildRemoteStoreNodeAttributes(String repoName, Path repoPath) {
        String repoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            repoName
        );
        String repoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            repoName
        );

        return Settings.builder()
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, repoName)
            .put(repoTypeAttributeKey, FsRepository.TYPE)
            .put(repoSettingsAttributeKeyPrefix + "location", repoPath)
            .put("node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, repoName)
            .put(repoTypeAttributeKey, FsRepository.TYPE)
            .put(repoSettingsAttributeKeyPrefix + "location", repoPath)
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, repoName)
            .put(repoTypeAttributeKey, FsRepository.TYPE)
            .put(repoSettingsAttributeKeyPrefix + "location", repoPath)
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), false)
            .build();
    }

    // Validate Scenario Normal Snapshot -> remoteStoreShallowCopy Snapshot -> normal Snapshot
    public void testRetrieveShallowCopySnapshotCase1() throws IOException {
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");

        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create first snapshot");
        SnapshotInfo snapshotInfo = createSnapshot(
            snapshotRepositoryName,
            "test-snap-1",
            new ArrayList<>(Arrays.asList(indexName, remoteStoreIndexName))
        );
        final SnapshotId snapshotId1 = snapshotInfo.snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be no lock files present in directory, but found " + Arrays.toString(lockFiles), 0, lockFiles.length);
        logger.info("--> create remote index shallow snapshot");
        Settings snapshotRepoSettingsForShallowCopy = Settings.builder()
            .put(snapshotRepoSettings)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE)
            .build();
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettingsForShallowCopy);

        snapshotInfo = createSnapshot(
            snapshotRepositoryName,
            "test-snap-2",
            new ArrayList<>(Arrays.asList(indexName, remoteStoreIndexName))
        );
        final SnapshotId snapshotId2 = snapshotInfo.snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId2.getUUID() + ".v2_lock"));

        logger.info("--> create another normal snapshot");
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettings);
        snapshotInfo = createSnapshot(
            snapshotRepositoryName,
            "test-snap-3",
            new ArrayList<>(Arrays.asList(indexName, remoteStoreIndexName))
        );
        final SnapshotId snapshotId3 = snapshotInfo.snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId2.getUUID() + ".v2_lock"));

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(snapshotRepositoryName);
        RepositoryData repositoryData = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        IndexId indexId = repositoryData.resolveIndexId(remoteStoreIndexName);

        List<SnapshotId> snapshotIds = repositoryData.getSnapshotIds()
            .stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
            .collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));

        // shallow copy shard metadata - getRemoteStoreShallowCopyShardMetadata
        RemoteStoreShardShallowCopySnapshot shardShallowCopySnapshot = repository.getRemoteStoreShallowCopyShardMetadata(
            snapshotId2,
            indexId,
            new ShardId(remoteStoreIndexName, indexId.getId(), 0)
        );
        assertEquals(shardShallowCopySnapshot.getRemoteStoreRepository(), remoteStoreRepositoryName);
    }

    public void testGetRemoteStoreShallowCopyShardMetadata() throws IOException {
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");

        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create remote index shallow snapshot");
        Settings snapshotRepoSettingsForShallowCopy = Settings.builder()
            .put(snapshotRepoSettings)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE)
            .build();
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettingsForShallowCopy);

        SnapshotInfo snapshotInfo = createSnapshot(snapshotRepositoryName, "test-snap-2", new ArrayList<>(List.of(remoteStoreIndexName)));
        final SnapshotId snapshotId = snapshotInfo.snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("there should be only one lock file, but found " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId.getUUID() + ".v2_lock"));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(snapshotRepositoryName);
        RepositoryData repositoryData = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        IndexSettings indexSetting = getIndexSettings(remoteStoreIndexName);
        IndexId indexId = repositoryData.resolveIndexId(remoteStoreIndexName);
        RemoteStoreShardShallowCopySnapshot shardShallowCopySnapshot = repository.getRemoteStoreShallowCopyShardMetadata(
            snapshotId,
            indexId,
            new ShardId(remoteStoreIndexName, indexSetting.getUUID(), 0)
        );
        assertEquals(shardShallowCopySnapshot.getRemoteStoreRepository(), remoteStoreRepositoryName);
        assertEquals(shardShallowCopySnapshot.getIndexUUID(), indexSetting.getUUID());
        assertEquals(shardShallowCopySnapshot.getRepositoryBasePath(), "");
    }

    // Validate Scenario remoteStoreShallowCopy Snapshot -> remoteStoreShallowCopy Snapshot
    // -> remoteStoreShallowCopy Snapshot -> normal snapshot
    public void testRetrieveShallowCopySnapshotCase2() throws IOException {
        final Client client = client();
        final String snapshotRepositoryName = "test-repo";
        final String remoteStoreRepositoryName = "test-rs-repo";

        logger.info("-->  creating snapshot repository");
        Settings snapshotRepoSettings = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .build();
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);

        GetRepositoriesResponse updatedGetRepositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(snapshotRepositoryName)
            .get();

        RepositoryMetadata updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);

        assertFalse(updatedRepositoryMetadata.settings().getAsBoolean(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), false));

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        logger.info("--> creating a remote store enabled index and indexing documents");
        final String remoteStoreIndexName = "test-rs-idx";
        Settings indexSettings = getRemoteStoreBackedIndexSettings();
        createIndex(remoteStoreIndexName, indexSettings);
        indexDocuments(client, remoteStoreIndexName);

        logger.info("--> create first remote index shallow snapshot");

        Settings snapshotRepoSettingsForShallowCopy = Settings.builder()
            .put(snapshotRepoSettings)
            .put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .build();
        updateRepository(client, snapshotRepositoryName, snapshotRepoSettingsForShallowCopy);

        updatedGetRepositoriesResponse = client.admin().cluster().prepareGetRepositories(snapshotRepositoryName).get();

        updatedRepositoryMetadata = updatedGetRepositoriesResponse.repositories().get(0);

        assertTrue(updatedRepositoryMetadata.settings().getAsBoolean(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), false));

        SnapshotInfo snapshotInfo = createSnapshot(
            snapshotRepositoryName,
            "test-snap-1",
            new ArrayList<>(Arrays.asList(indexName, remoteStoreIndexName))
        );
        final SnapshotId snapshotId1 = snapshotInfo.snapshotId();

        String[] lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("lock files are " + Arrays.toString(lockFiles), 1, lockFiles.length);
        assertTrue(lockFiles[0].endsWith(snapshotId1.getUUID() + ".v2_lock"));

        logger.info("--> create second remote index shallow snapshot");
        snapshotInfo = createSnapshot(
            snapshotRepositoryName,
            "test-snap-2",
            new ArrayList<>(Arrays.asList(indexName, remoteStoreIndexName))
        );
        final SnapshotId snapshotId2 = snapshotInfo.snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("lock files are " + Arrays.toString(lockFiles), 2, lockFiles.length);
        List<SnapshotId> shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assertTrue(lockFiles[0].contains(snapshotId.getUUID()) || lockFiles[1].contains(snapshotId.getUUID()));
        }
        logger.info("--> create third remote index shallow snapshot");
        snapshotInfo = createSnapshot(
            snapshotRepositoryName,
            "test-snap-3",
            new ArrayList<>(Arrays.asList(indexName, remoteStoreIndexName))
        );
        final SnapshotId snapshotId3 = snapshotInfo.snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals(3, lockFiles.length);
        shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assertTrue(
                lockFiles[0].contains(snapshotId.getUUID())
                    || lockFiles[1].contains(snapshotId.getUUID())
                    || lockFiles[2].contains(snapshotId.getUUID())
            );
        }
        logger.info("--> create normal snapshot");
        createRepository(client, snapshotRepositoryName, snapshotRepoSettings);
        snapshotInfo = createSnapshot(
            snapshotRepositoryName,
            "test-snap-4",
            new ArrayList<>(Arrays.asList(indexName, remoteStoreIndexName))
        );
        final SnapshotId snapshotId4 = snapshotInfo.snapshotId();

        lockFiles = getLockFilesInRemoteStore(remoteStoreIndexName, remoteStoreRepositoryName);
        assertEquals("lock files are " + Arrays.toString(lockFiles), 3, lockFiles.length);
        shallowCopySnapshotIDs = Arrays.asList(snapshotId1, snapshotId2, snapshotId3);
        for (SnapshotId snapshotId : shallowCopySnapshotIDs) {
            assertTrue(
                lockFiles[0].contains(snapshotId.getUUID())
                    || lockFiles[1].contains(snapshotId.getUUID())
                    || lockFiles[2].contains(snapshotId.getUUID())
            );
        }

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2, snapshotId3, snapshotId4);

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(snapshotRepositoryName);
        List<SnapshotId> snapshotIds = OpenSearchBlobStoreRepositoryIntegTestCase.getRepositoryData(repository)
            .getSnapshotIds()
            .stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
            .collect(Collectors.toList());
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testRepositoryCreationShallowV2() throws Exception {
        Client client = client();

        Settings snapshotRepoSettings1 = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .put(REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(SHALLOW_SNAPSHOT_V2.getKey(), true)
            .build();

        String invalidRepoName = "test" + SnapshotsService.SNAPSHOT_PINNED_TIMESTAMP_DELIMITER + "repo-1";
        try {
            createRepository(client, invalidRepoName, snapshotRepoSettings1);
        } catch (RepositoryException e) {
            assertEquals(
                "["
                    + invalidRepoName
                    + "] setting shallow_snapshot_v2 cannot be enabled for repository with __ in the name as this delimiter is used to create pinning entity",
                e.getMessage()
            );
        }

        // Create repo with shallow snapshot V2 enabled
        createRepository(client, "test-repo-1", snapshotRepoSettings1);

        logger.info("--> verify the repository");
        VerifyRepositoryResponse verifyRepositoryResponse = client.admin().cluster().prepareVerifyRepository("test-repo-1").get();
        assertNotNull(verifyRepositoryResponse.getNodes());

        GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().prepareGetRepositories("test-repo-1").get();
        assertTrue(SHALLOW_SNAPSHOT_V2.get(getRepositoriesResponse.repositories().get(0).settings()));

        Settings snapshotRepoSettings2 = Settings.builder()
            .put(node().settings())
            .put("location", OpenSearchIntegTestCase.randomRepoPath(node().settings()))
            .put(REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true)
            .put(SHALLOW_SNAPSHOT_V2.getKey(), true)
            .build();

        // Create another repo with shallow snapshot V2 enabled, this should fail.
        try {
            createRepository(client, "test-repo-2", snapshotRepoSettings2);
        } catch (RepositoryException e) {
            assertEquals(
                "[test-repo-2] setting shallow_snapshot_v2 cannot be enabled as this setting can be enabled only on one repository  and one or more repositories in the cluster have the setting as enabled",
                e.getMessage()
            );
        }

        // Disable shallow snapshot V2 setting on test-repo-1
        updateRepository(
            client,
            "test-repo-1",
            Settings.builder().put(snapshotRepoSettings1).put(SHALLOW_SNAPSHOT_V2.getKey(), false).build()
        );
        getRepositoriesResponse = client.admin().cluster().prepareGetRepositories("test-repo-1").get();
        assertFalse(SHALLOW_SNAPSHOT_V2.get(getRepositoriesResponse.repositories().get(0).settings()));

        // Create test-repo-2 with shallow snapshot V2 enabled, this should pass now.
        createRepository(client, "test-repo-2", snapshotRepoSettings2);
        getRepositoriesResponse = client.admin().cluster().prepareGetRepositories("test-repo-2").get();
        assertTrue(SHALLOW_SNAPSHOT_V2.get(getRepositoriesResponse.repositories().get(0).settings()));

        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        indexDocuments(client, indexName);

        // Create pinned timestamp snapshot in test-repo-2
        SnapshotInfo snapshotInfo = createSnapshot("test-repo-2", "test-snap-2", new ArrayList<>());
        assertNotNull(snapshotInfo.snapshotId());

        // As snapshot is present, even after disabling shallow snapshot setting in test-repo-2, we will not be able to
        // enable shallow snapshot v2 setting in test-repo-1
        updateRepository(
            client,
            "test-repo-2",
            Settings.builder().put(snapshotRepoSettings2).put(SHALLOW_SNAPSHOT_V2.getKey(), false).build()
        );
        getRepositoriesResponse = client.admin().cluster().prepareGetRepositories("test-repo-2").get();
        assertFalse(SHALLOW_SNAPSHOT_V2.get(getRepositoriesResponse.repositories().get(0).settings()));

        try {
            updateRepository(client, "test-repo-1", snapshotRepoSettings1);
        } catch (RepositoryException e) {
            assertEquals(
                "[test-repo-1] setting shallow_snapshot_v2 cannot be enabled if there are existing snapshots created with shallow V2 setting using different repository.",
                e.getMessage()
            );
        }

        // After deleting the snapshot, we will be able to enable shallow snapshot v2 setting in test-repo-1
        AcknowledgedResponse deleteSnapshotResponse = client().admin().cluster().prepareDeleteSnapshot("test-repo-2", "test-snap-2").get();

        assertAcked(deleteSnapshotResponse);

        updateRepository(client, "test-repo-1", snapshotRepoSettings1);
        getRepositoriesResponse = client.admin().cluster().prepareGetRepositories("test-repo-1").get();
        assertTrue(SHALLOW_SNAPSHOT_V2.get(getRepositoriesResponse.repositories().get(0).settings()));

        // Having a snapshot in the same repo should allow disabling and re-enabling shallow snapshot v2 setting
        snapshotInfo = createSnapshot("test-repo-1", "test-snap-1", new ArrayList<>());
        assertNotNull(snapshotInfo.snapshotId());
        updateRepository(
            client,
            "test-repo-1",
            Settings.builder().put(snapshotRepoSettings1).put(SHALLOW_SNAPSHOT_V2.getKey(), false).build()
        );
        getRepositoriesResponse = client.admin().cluster().prepareGetRepositories("test-repo-1").get();
        assertFalse(SHALLOW_SNAPSHOT_V2.get(getRepositoriesResponse.repositories().get(0).settings()));

        updateRepository(client, "test-repo-1", snapshotRepoSettings1);
        getRepositoriesResponse = client.admin().cluster().prepareGetRepositories("test-repo-1").get();
        assertTrue(SHALLOW_SNAPSHOT_V2.get(getRepositoriesResponse.repositories().get(0).settings()));
    }
}
