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

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class CorruptedBlobStoreRepositoryIT extends AbstractSnapshotIntegTestCase {

    public void testConcurrentlyChangeRepositoryContents() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                // Don't cache repository data because the test manually modifies the repository data
                .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex().setIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex().setIndex("test-idx-2").setSource("foo", "bar")
        );

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        Files.move(repo.resolve("index-" + repositoryData.getGenId()), repo.resolve("index-" + (repositoryData.getGenId() + 1)));

        assertRepositoryBlocked(client, repoName, snapshot);

        if (randomBoolean()) {
            logger.info("--> move index-N blob back to initial generation");
            Files.move(repo.resolve("index-" + (repositoryData.getGenId() + 1)), repo.resolve("index-" + repositoryData.getGenId()));

            logger.info("--> verify repository remains blocked");
            assertRepositoryBlocked(client, repoName, snapshot);
        }

        logger.info("--> remove repository");
        assertAcked(client.admin().cluster().prepareDeleteRepository(repoName));

        logger.info("--> recreate repository");
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(repoName)
                .setType("fs")
                .setSettings(
                    Settings.builder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                )
        );

        startDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots(repoName).addSnapshots(snapshot).get()
        );
    }

    public void testConcurrentlyChangeRepositoryContentsInBwCMode() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        logger.info("-->  creating repository at {}", repo.toAbsolutePath());
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(repoName)
                .setType("fs")
                .setSettings(
                    Settings.builder()
                        .put("location", repo)
                        .put("compress", false)
                        .put(BlobStoreRepository.ALLOW_CONCURRENT_MODIFICATION.getKey(), true)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                )
        );

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex().setIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex().setIndex("test-idx-2").setSource("foo", "bar")
        );

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        final Repository repository = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class).repository(repoName);

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repository);
        final long beforeMoveGen = repositoryData.getGenId();
        Files.move(repo.resolve("index-" + beforeMoveGen), repo.resolve("index-" + (beforeMoveGen + 1)));

        logger.info("--> verify index-N blob is found at the new location");
        assertThat(getRepositoryData(repository).getGenId(), is(beforeMoveGen + 1));

        final SnapshotsService snapshotsService = internalCluster().getCurrentClusterManagerNodeInstance(SnapshotsService.class);
        logger.info("--> wait for all listeners on snapshots service to be resolved to avoid snapshot task batching causing a conflict");
        assertBusy(() -> assertTrue(snapshotsService.assertAllListenersResolved()));

        logger.info("--> delete snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> verify index-N blob is found at the expected location");
        assertThat(getRepositoryData(repository).getGenId(), is(beforeMoveGen + 2));

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots(repoName).addSnapshots(snapshot).get()
        );
    }

    public void testFindDanglingLatestGeneration() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex().setIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex().setIndex("test-idx-2").setSource("foo", "bar")
        );

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        final Repository repository = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
            .repository(repoName);

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final long beforeMoveGen = repositoryData.getGenId();
        Files.move(repo.resolve("index-" + beforeMoveGen), repo.resolve("index-" + (beforeMoveGen + 1)));

        logger.info("--> set next generation as pending in the cluster state");
        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .metadata(
                    Metadata.builder(currentState.getMetadata())
                        .putCustom(
                            RepositoriesMetadata.TYPE,
                            currentState.metadata()
                                .<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE)
                                .withUpdatedGeneration(repository.getMetadata().name(), beforeMoveGen, beforeMoveGen + 1)
                        )
                        .build()
                )
                .build()
        );

        logger.info("--> full cluster restart");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> verify index-N blob is found at the new location");
        assertThat(getRepositoryData(repoName).getGenId(), is(beforeMoveGen + 1));

        startDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> verify index-N blob is found at the expected location");
        assertThat(getRepositoryData(repoName).getGenId(), is(beforeMoveGen + 2));

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().prepareGetSnapshots(repoName).addSnapshots(snapshot).get()
        );
    }

    public void testMountCorruptedRepositoryData() throws Exception {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository contents");
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repo)
                // Don't cache repository data because the test manually modifies the repository data
                .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false)
                .put("compress", false)
        );

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> corrupt index-N blob");
        final Repository repository = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
            .repository(repoName);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        Files.write(repo.resolve("index-" + repositoryData.getGenId()), randomByteArrayOfLength(randomIntBetween(1, 100)));

        logger.info("--> verify loading repository data throws RepositoryException");
        expectThrows(RepositoryException.class, () -> getRepositoryData(repository));

        final String otherRepoName = "other-repo";
        createRepository(otherRepoName, "fs", Settings.builder().put("location", repo).put("compress", false));
        final Repository otherRepo = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
            .repository(otherRepoName);

        logger.info("--> verify loading repository data from newly mounted repository throws RepositoryException");
        expectThrows(RepositoryException.class, () -> getRepositoryData(otherRepo));
    }

    /**
     * Tests that a shard snapshot with a corrupted shard index file can still be used for restore and incremental snapshots.
     */
    public void testSnapshotWithCorruptedShardIndexFile() throws Exception {
        final Client client = client();
        final Path repo = randomRepoPath();
        final String indexName = "test-idx";
        final int nDocs = randomIntBetween(1, 10);

        logger.info("-->  creating index [{}] with [{}] documents in it", indexName, nDocs);
        assertAcked(prepareCreate(indexName).setSettings(indexSettingsNoReplicas(1)));

        final IndexRequestBuilder[] documents = new IndexRequestBuilder[nDocs];
        for (int j = 0; j < nDocs; j++) {
            documents[j] = client.prepareIndex(indexName).setSource("foo", "bar");
        }
        indexRandom(true, documents);
        flushAndRefresh();

        createRepository("test-repo", "fs", repo);

        final String snapshot1 = "test-snap-1";
        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", snapshot1);
        assertThat(snapshotInfo.indices(), hasSize(1));

        final RepositoryData repositoryData = getRepositoryData("test-repo");
        final Map<String, IndexId> indexIds = repositoryData.getIndices();
        assertThat(indexIds.size(), equalTo(1));

        final IndexId corruptedIndex = indexIds.get(indexName);
        final Path shardIndexFile = repo.resolve("indices")
            .resolve(corruptedIndex.getId())
            .resolve("0")
            .resolve("index-" + repositoryData.shardGenerations().getShardGen(corruptedIndex, 0));

        logger.info("-->  truncating shard index file [{}]", shardIndexFile);
        try (SeekableByteChannel outChan = Files.newByteChannel(shardIndexFile, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        logger.info("-->  verifying snapshot state for [{}]", snapshot1);
        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo(snapshot1));

        logger.info("-->  deleting index [{}]", indexName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("-->  restoring snapshot [{}]", snapshot1);
        clusterAdmin().prepareRestoreSnapshot("test-repo", snapshot1)
            .setRestoreGlobalState(randomBoolean())
            .setWaitForCompletion(true)
            .get();
        ensureGreen();

        assertDocCount(indexName, nDocs);

        logger.info("-->  indexing [{}] more documents into [{}]", nDocs, indexName);
        for (int j = 0; j < nDocs; j++) {
            documents[j] = client.prepareIndex(indexName).setSource("foo2", "bar2");
        }
        indexRandom(true, documents);

        final String snapshot2 = "test-snap-2";
        logger.info("-->  creating snapshot [{}]", snapshot2);
        final SnapshotInfo snapshotInfo2 = clusterAdmin().prepareCreateSnapshot("test-repo", snapshot2)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo2.state(), equalTo(SnapshotState.PARTIAL));
        assertThat(snapshotInfo2.failedShards(), equalTo(1));
        assertThat(snapshotInfo2.successfulShards(), equalTo(snapshotInfo.totalShards() - 1));
        assertThat(snapshotInfo2.indices(), hasSize(1));
    }

    public void testDeleteSnapshotWithMissingIndexAndShardMetadata() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        createRepository("test-repo", "fs", repo);

        final String[] indices = { "test-idx-1", "test-idx-2" };
        createIndex(indices);
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indices)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        final Map<String, IndexId> indexIds = getRepositoryData("test-repo").getIndices();
        final Path indicesPath = repo.resolve("indices");

        logger.info("--> delete index metadata and shard metadata");
        for (String index : indices) {
            Path shardZero = indicesPath.resolve(indexIds.get(index).getId()).resolve("0");
            if (randomBoolean()) {
                Files.delete(
                    shardZero.resolve("index-" + getRepositoryData("test-repo").shardGenerations().getShardGen(indexIds.get(index), 0))
                );
            }
            Files.delete(shardZero.resolve("snap-" + snapshotInfo.snapshotId().getUUID() + ".dat"));
        }

        startDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");

        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1").get()
        );

        for (String index : indices) {
            assertTrue(Files.notExists(indicesPath.resolve(indexIds.get(index).getId())));
        }
    }

    public void testDeleteSnapshotWithMissingMetadata() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        createRepository("test-repo", "fs", repo);

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> delete global state metadata");
        Path metadata = repo.resolve("meta-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        Files.delete(metadata);

        startDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1").get()
        );
    }

    public void testDeleteSnapshotWithCorruptedSnapshotFile() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        createRepository(
            "test-repo",
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> truncate snapshot file to make it unreadable");
        Path snapshotPath = repo.resolve("snap-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        try (SeekableByteChannel outChan = Files.newByteChannel(snapshotPath, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }
        startDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1").get().getSnapshots()
        );

        logger.info("--> make sure that we can create the snapshot again");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
    }

    /** Tests that a snapshot with a corrupted global state file can still be deleted */
    public void testDeleteSnapshotWithCorruptedGlobalState() throws Exception {
        final Path repo = randomRepoPath();

        createRepository(
            "test-repo",
            "fs",
            Settings.builder().put("location", repo).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );
        flushAndRefresh("test-idx-1", "test-idx-2");

        SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");

        final Path globalStatePath = repo.resolve("meta-" + snapshotInfo.snapshotId().getUUID() + ".dat");
        if (randomBoolean()) {
            // Delete the global state metadata file
            IOUtils.deleteFilesIgnoringExceptions(globalStatePath);
        } else {
            // Truncate the global state metadata file
            try (SeekableByteChannel outChan = Files.newByteChannel(globalStatePath, StandardOpenOption.WRITE)) {
                outChan.truncate(randomInt(10));
            }
        }

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo("test-snap"));

        SnapshotsStatusResponse snapshotStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").get();
        assertThat(snapshotStatusResponse.getSnapshots(), hasSize(1));
        assertThat(snapshotStatusResponse.getSnapshots().get(0).getSnapshot().getSnapshotId().getName(), equalTo("test-snap"));

        assertAcked(startDeleteSnapshot("test-repo", "test-snap").get());
        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareGetSnapshots("test-repo").addSnapshots("test-snap").get());
        assertRequestBuilderThrows(
            clusterAdmin().prepareSnapshotStatus("test-repo").addSnapshots("test-snap"),
            SnapshotMissingException.class
        );

        createFullSnapshot("test-repo", "test-snap");
    }

    public void testSnapshotWithMissingShardLevelIndexFile() throws Exception {
        disableRepoConsistencyCheck("This test uses a purposely broken repository so it would fail consistency checks");

        Path repo = randomRepoPath();
        createRepository("test-repo", "fs", repo);

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();

        logger.info("--> deleting shard level index file");
        final Path indicesPath = repo.resolve("indices");
        for (IndexId indexId : getRepositoryData("test-repo").getIndices().values()) {
            final Path shardGen;
            try (Stream<Path> shardFiles = Files.list(indicesPath.resolve(indexId.getId()).resolve("0"))) {
                shardGen = shardFiles.filter(file -> file.getFileName().toString().startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Failed to find shard index blob"));
            }
            Files.delete(shardGen);
        }

        logger.info("--> creating another snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices("test-idx-1")
            .get();
        assertEquals(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            createSnapshotResponse.getSnapshotInfo().totalShards() - 1
        );

        logger.info(
            "--> restoring the first snapshot, the repository should not have lost any shard data despite deleting index-N, "
                + "because it uses snap-*.data files and not the index-N to determine what files to restore"
        );
        client().admin().indices().prepareDelete("test-idx-1", "test-idx-2").get();
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .get();
        assertEquals(0, restoreSnapshotResponse.getRestoreInfo().failedShards());
    }

    private void assertRepositoryBlocked(Client client, String repo, String existingSnapshot) {
        logger.info("--> try to delete snapshot");
        final RepositoryException repositoryException3 = expectThrows(
            RepositoryException.class,
            () -> client.admin().cluster().prepareDeleteSnapshot(repo, existingSnapshot).execute().actionGet()
        );
        assertThat(
            repositoryException3.getMessage(),
            containsString("Could not read repository data because the contents of the repository do not match its expected state.")
        );

        logger.info("--> try to create snapshot");
        final RepositoryException repositoryException4 = expectThrows(
            RepositoryException.class,
            () -> client.admin().cluster().prepareCreateSnapshot(repo, existingSnapshot).execute().actionGet()
        );
        assertThat(
            repositoryException4.getMessage(),
            containsString("Could not read repository data because the contents of the repository do not match its expected state.")
        );
    }
}
