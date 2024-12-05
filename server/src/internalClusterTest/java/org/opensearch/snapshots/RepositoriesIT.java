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

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.RepositoryVerificationException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class RepositoriesIT extends AbstractSnapshotIntegTestCase {
    public void testRepositoryCreation() throws Exception {
        Client client = client();

        Path location = randomRepoPath();

        createRepository("test-repo-1", "fs", location);

        logger.info("--> verify the repository");
        int numberOfFiles = FileSystemUtils.files(location).length;
        VerifyRepositoryResponse verifyRepositoryResponse = client.admin().cluster().prepareVerifyRepository("test-repo-1").get();
        assertThat(verifyRepositoryResponse.getNodes().size(), equalTo(cluster().numDataAndClusterManagerNodes()));

        logger.info("--> verify that we didn't leave any files as a result of verification");
        assertThat(FileSystemUtils.files(location).length, equalTo(numberOfFiles));

        logger.info("--> check that repository is really there");
        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().clear().setMetadata(true).get();
        Metadata metadata = clusterStateResponse.getState().getMetadata();
        RepositoriesMetadata repositoriesMetadata = metadata.custom(RepositoriesMetadata.TYPE);
        assertThat(repositoriesMetadata, notNullValue());
        assertThat(repositoriesMetadata.repository("test-repo-1"), notNullValue());
        assertThat(repositoriesMetadata.repository("test-repo-1").type(), equalTo("fs"));

        logger.info("-->  creating another repository");
        createRepository("test-repo-2", "fs");

        logger.info("--> check that both repositories are in cluster state");
        clusterStateResponse = client.admin().cluster().prepareState().clear().setMetadata(true).get();
        metadata = clusterStateResponse.getState().getMetadata();
        repositoriesMetadata = metadata.custom(RepositoriesMetadata.TYPE);
        assertThat(repositoriesMetadata, notNullValue());
        assertThat(repositoriesMetadata.repositories().size(), equalTo(2));
        assertThat(repositoriesMetadata.repository("test-repo-1"), notNullValue());
        assertThat(repositoriesMetadata.repository("test-repo-1").type(), equalTo("fs"));
        assertThat(repositoriesMetadata.repository("test-repo-2"), notNullValue());
        assertThat(repositoriesMetadata.repository("test-repo-2").type(), equalTo("fs"));

        logger.info("--> check that both repositories can be retrieved by getRepositories query");
        GetRepositoriesResponse repositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(randomFrom("_all", "*", "test-repo-*"))
            .get();
        assertThat(repositoriesResponse.repositories().size(), equalTo(2));
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-1"), notNullValue());
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-2"), notNullValue());

        RepositoryMetadata testRepo1Md = findRepository(repositoriesResponse.repositories(), "test-repo-1");

        logger.info("--> check that trying to create a repository with the same settings repeatedly does not update cluster state");
        String beforeStateUuid = clusterStateResponse.getState().stateUUID();
        createRepository("test-repo-1", "fs", Settings.builder().put("location", location));
        repositoriesResponse = client.admin().cluster().prepareGetRepositories(randomFrom("_all", "*", "test-repo-*")).get();
        RepositoryMetadata testRepo1MdAfterUpdate = findRepository(repositoriesResponse.repositories(), "test-repo-1");

        if (testRepo1Md.settings().equals(testRepo1MdAfterUpdate.settings())) {
            assertEquals(beforeStateUuid, client.admin().cluster().prepareState().clear().get().getState().stateUUID());
        }

        logger.info("--> delete repository test-repo-1");
        client.admin().cluster().prepareDeleteRepository("test-repo-1").get();
        repositoriesResponse = client.admin().cluster().prepareGetRepositories().get();
        assertThat(repositoriesResponse.repositories().size(), equalTo(1));
        assertThat(findRepository(repositoriesResponse.repositories(), "test-repo-2"), notNullValue());

        logger.info("--> delete repository test-repo-2");
        client.admin().cluster().prepareDeleteRepository("test-repo-2").get();
        repositoriesResponse = client.admin().cluster().prepareGetRepositories().get();
        assertThat(repositoriesResponse.repositories().size(), equalTo(0));
    }

    public void testResidualStaleIndicesAreDeletedByConsecutiveDelete() throws Exception {
        Client client = client();
        Path repositoryPath = randomRepoPath();
        final String repositoryName = "test-repo";
        final String snapshotToBeDeletedLastName = "test-snapshot-to-be-deleted-last";
        final String bulkSnapshotsPattern = "test-snap-*";

        logger.info("-->  creating repository at {}", repositoryPath.toAbsolutePath());
        createRepository(repositoryName, "mock", repositoryPath);

        int numberOfFiles = numberOfFiles(repositoryPath);

        logger.info("--> creating index-0 and ingest data");
        createIndex("test-idx-0");
        ensureGreen();
        for (int j = 0; j < 10; j++) {
            index("test-idx-0", "_doc", Integer.toString(10 + j), "foo", "bar" + 10 + j);
        }
        refresh();

        logger.info("--> creating first snapshot");
        createFullSnapshot(repositoryName, snapshotToBeDeletedLastName);

        // Create more snapshots to be deleted in bulk
        int maxThreadsForSnapshotDeletion = internalCluster().getClusterManagerNodeInstance(ThreadPool.class)
            .info(ThreadPool.Names.SNAPSHOT)
            .getMax();
        for (int i = 1; i <= maxThreadsForSnapshotDeletion + 1; i++) {
            String snapshotName = "test-snap-" + i;
            String testIndexName = "test-idx-" + i;
            logger.info("--> creating index-" + i + " and ingest data");
            createIndex(testIndexName);
            ensureGreen();
            for (int j = 0; j < 10; j++) {
                index(testIndexName, "_doc", Integer.toString(10 + j), "foo", "bar" + 10 + j);
            }
            refresh();

            logger.info("--> creating snapshot: {}", snapshotName);
            createFullSnapshot(repositoryName, snapshotName);
        }

        // Make repository to throw exception when trying to delete stale indices
        // This will make sure stale indices stays in repository after snapshot delete
        String clusterManagerNode = internalCluster().getClusterManagerName();
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repository("test-repo"))
            .setThrowExceptionWhileDelete(true);

        logger.info("--> delete the bulk of the snapshots");
        client.admin().cluster().prepareDeleteSnapshot(repositoryName, bulkSnapshotsPattern).get();

        // Make repository to work normally
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repository("test-repo"))
            .setThrowExceptionWhileDelete(false);

        // This snapshot should delete last snapshot's residual stale indices as well
        logger.info("--> delete first snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repositoryName, snapshotToBeDeletedLastName).get();

        logger.info("--> make sure that number of files is back to what it was when the first snapshot was made");
        assertFileCount(repositoryPath, numberOfFiles + 2);

        logger.info("--> done");
    }

    private RepositoryMetadata findRepository(List<RepositoryMetadata> repositories, String name) {
        for (RepositoryMetadata repository : repositories) {
            if (repository.name().equals(name)) {
                return repository;
            }
        }
        return null;
    }

    public void testMisconfiguredRepository() throws Exception {
        Client client = client();

        logger.info("--> trying creating repository with incorrect settings");
        try {
            client.admin().cluster().preparePutRepository("test-repo").setType("fs").get();
            fail("Shouldn't be here");
        } catch (RepositoryException ex) {
            assertThat(ex.toString(), containsString("missing location"));
        }

        logger.info("--> trying creating fs repository with location that is not registered in path.repo setting");
        Path invalidRepoPath = createTempDir().toAbsolutePath();
        String location = invalidRepoPath.toString();
        try {
            createRepository("test-repo", "fs", Settings.builder().put("location", location));
            fail("Shouldn't be here");
        } catch (RepositoryException ex) {
            assertThat(
                ex.toString(),
                containsString("location [" + location + "] doesn't match any of the locations specified " + "by path.repo")
            );
        }
    }

    public void testRepositoryAckTimeout() throws Exception {
        logger.info("-->  creating repository test-repo-1 with 0s timeout - shouldn't ack");

        Settings.Builder settings = Settings.builder()
            .put("location", randomRepoPath())
            .put("compress", randomBoolean())
            .put("chunk_size", randomIntBetween(5, 100), ByteSizeUnit.BYTES);
        PutRepositoryRequestBuilder requestBuilder = OpenSearchIntegTestCase.putRepositoryRequestBuilder(
            client().admin().cluster(),
            "test-repo-1",
            "fs",
            true,
            settings,
            "0s",
            false
        );
        assertFalse(requestBuilder.get().isAcknowledged());

        logger.info("-->  creating repository test-repo-2 with standard timeout - should ack");
        settings = Settings.builder()
            .put("location", randomRepoPath())
            .put("compress", randomBoolean())
            .put("chunk_size", randomIntBetween(5, 100), ByteSizeUnit.BYTES);
        createRepository("test-repo-2", "fs", settings);

        logger.info("-->  deleting repository test-repo-2 with 0s timeout - shouldn't ack");
        AcknowledgedResponse deleteRepositoryResponse = client().admin()
            .cluster()
            .prepareDeleteRepository("test-repo-2")
            .setTimeout("0s")
            .get();
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(false));

        logger.info("-->  deleting repository test-repo-1 with standard timeout - should ack");
        deleteRepositoryResponse = client().admin().cluster().prepareDeleteRepository("test-repo-1").get();
        assertThat(deleteRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    public void testRepositoryVerification() throws Exception {
        disableRepoConsistencyCheck("This test does not create any data in the repository.");

        Client client = client();

        Settings settings = Settings.builder().put("location", randomRepoPath()).put("random_control_io_exception_rate", 1.0).build();
        Settings readonlySettings = Settings.builder().put(settings).put("readonly", true).build();
        logger.info("-->  creating repository that cannot write any files - should fail");
        assertRequestBuilderThrows(
            OpenSearchIntegTestCase.putRepositoryRequestBuilder(
                client.admin().cluster(),
                "test-repo-1",
                "mock",
                true,
                Settings.builder().put(settings),
                null,
                false
            ),
            RepositoryVerificationException.class
        );

        logger.info("-->  creating read-only repository that cannot read any files - should fail");
        assertRequestBuilderThrows(
            OpenSearchIntegTestCase.putRepositoryRequestBuilder(
                client.admin().cluster(),
                "test-repo-2",
                "mock",
                true,
                Settings.builder().put(readonlySettings),
                null,
                false
            ),
            RepositoryVerificationException.class
        );

        logger.info("-->  creating repository that cannot write any files, but suppress verification - should be acked");
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), "test-repo-1", "mock", false, Settings.builder().put(settings));

        logger.info("-->  verifying repository");
        assertRequestBuilderThrows(client.admin().cluster().prepareVerifyRepository("test-repo-1"), RepositoryVerificationException.class);

        logger.info("-->  creating read-only repository that cannot read any files, but suppress verification - should be acked");
        OpenSearchIntegTestCase.putRepository(
            client.admin().cluster(),
            "test-repo-2",
            "mock",
            false,
            Settings.builder().put(readonlySettings)
        );

        logger.info("-->  verifying repository");
        assertRequestBuilderThrows(client.admin().cluster().prepareVerifyRepository("test-repo-2"), RepositoryVerificationException.class);

        Path location = randomRepoPath();

        logger.info("-->  creating repository");
        try {
            Settings.Builder settingsBuilder = Settings.builder().put("location", location).put("localize_location", true);
            createRepository("test-repo-1", "mock", settingsBuilder);
            fail("RepositoryVerificationException wasn't generated");
        } catch (RepositoryVerificationException ex) {
            assertThat(ex.getMessage(), containsString("is not shared"));
        }
    }

    public void testSnapshotShardBlobDelete() throws Exception {
        Client client = client();
        Path repositoryPath = randomRepoPath();
        final String repositoryName = "test-repo";
        final String firstSnapshot = "first-snapshot";
        final String secondSnapshot = "second-snapshot";
        final String indexName = "test-idx";

        logger.info("-->  creating repository at {}", repositoryPath.toAbsolutePath());
        int maxShardBlobDeleteBatchSize = randomIntBetween(1, 1000);
        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", repositoryPath)
                .put(BlobStoreRepository.MAX_SNAPSHOT_SHARD_BLOB_DELETE_BATCH_SIZE.getKey(), maxShardBlobDeleteBatchSize)
        );

        logger.info("--> creating index-0 and ingest data");
        createIndex(indexName);
        ensureGreen();
        for (int j = 0; j < randomIntBetween(1, 1000); j++) {
            index(indexName, "_doc", Integer.toString(j), "foo", "bar" + j);
        }
        refresh();

        logger.info("--> creating first snapshot");
        createFullSnapshot(repositoryName, firstSnapshot);

        int numberOfFiles = numberOfFiles(repositoryPath);

        logger.info("--> adding some more documents to test index");
        for (int j = 0; j < randomIntBetween(100, 10000); ++j) {
            final BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < randomIntBetween(100, 1000); ++i) {
                bulkRequest.add(new IndexRequest(indexName).source("foo" + j, "bar" + i));
            }
            client().bulk(bulkRequest).get();
        }
        refresh();

        logger.info("--> creating second snapshot");
        createFullSnapshot(repositoryName, secondSnapshot);

        // Delete second snapshot
        logger.info("--> delete second snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repositoryName, secondSnapshot).get();

        logger.info("--> make sure that number of files is back to what it was when the first snapshot was made");
        assertFileCount(repositoryPath, numberOfFiles);

        logger.info("--> done");
    }

    public void testSnapshotShardBlobDeletionRepositoryThrowingError() throws Exception {
        Client client = client();
        Path repositoryPath = randomRepoPath();
        final String repositoryName = "test-repo";
        final String firstSnapshot = "first-snapshot";
        final String secondSnapshot = "second-snapshot";
        final String indexName = "test-idx";

        logger.info("-->  creating repository at {}", repositoryPath.toAbsolutePath());
        int maxShardBlobDeleteBatchSize = randomIntBetween(1, 1000);
        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", repositoryPath)
                .put(BlobStoreRepository.MAX_SNAPSHOT_SHARD_BLOB_DELETE_BATCH_SIZE.getKey(), maxShardBlobDeleteBatchSize)
        );

        logger.info("--> creating index-0 and ingest data");
        createIndex(indexName);
        ensureGreen();
        for (int j = 0; j < randomIntBetween(1, 1000); j++) {
            index(indexName, "_doc", Integer.toString(j), "foo", "bar" + j);
        }
        refresh();

        logger.info("--> creating first snapshot");
        createFullSnapshot(repositoryName, firstSnapshot);

        logger.info("--> adding some more documents to test index");
        for (int j = 0; j < randomIntBetween(100, 1000); ++j) {
            final BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < randomIntBetween(100, 1000); ++i) {
                bulkRequest.add(new IndexRequest(indexName).source("foo" + j, "bar" + i));
            }
            client().bulk(bulkRequest).get();
        }
        refresh();

        logger.info("--> creating second snapshot");
        createFullSnapshot(repositoryName, secondSnapshot);

        // Make repository to throw exception when trying to delete stale snapshot shard blobs
        String clusterManagerNode = internalCluster().getMasterName();
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repository("test-repo"))
            .setThrowExceptionWhileDelete(true);

        // Delete second snapshot
        logger.info("--> delete second snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repositoryName, secondSnapshot).get();

        // Make repository to work normally
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManagerNode).repository("test-repo"))
            .setThrowExceptionWhileDelete(false);

        // This snapshot should delete last snapshot's residual stale shard blobs as well
        logger.info("--> delete first snapshot");
        client.admin().cluster().prepareDeleteSnapshot(repositoryName, firstSnapshot).get();

        // Expect two files to remain in the repository:
        // (1) index-(N+1)
        // (2) index-latest
        assertFileCount(repositoryPath, 2);

        logger.info("--> done");
    }
}
