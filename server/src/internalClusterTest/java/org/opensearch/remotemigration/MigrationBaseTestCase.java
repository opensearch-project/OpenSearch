/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.repositories.fs.ReloadableFsRepository.REPOSITORIES_FAILRATE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class MigrationBaseTestCase extends OpenSearchIntegTestCase {
    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;
    boolean addRemote = false;

    private final List<String> documentKeys = List.of(
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5)
    );

    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (addRemote) {
            logger.info("Adding remote store node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                .build();
        } else {
            logger.info("Adding docrep node");
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
        }
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
    }

    protected void setFailRate(String repoName, int value) throws ExecutionException, InterruptedException {
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { repoName });
        GetRepositoriesResponse res = client().admin().cluster().getRepositories(gr).get();
        RepositoryMetadata rmd = res.repositories().get(0);
        Settings.Builder settings = Settings.builder()
            .put("location", rmd.settings().get("location"))
            .put(REPOSITORIES_FAILRATE_SETTING.getKey(), value);
        assertAcked(
            client().admin().cluster().preparePutRepository(repoName).setType(ReloadableFsRepository.TYPE).setSettings(settings).get()
        );
    }

    public void initDocRepToRemoteMigration() {
        assertTrue(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
                        .put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store")
                )
                .get()
                .isAcknowledged()
        );
    }

    public BulkResponse indexBulk(String indexName, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            final IndexRequest request = client().prepareIndex(indexName)
                .setId(UUIDs.randomBase64UUID())
                .setSource(documentKeys.get(randomIntBetween(0, documentKeys.size() - 1)), randomAlphaOfLength(5))
                .request();
            bulkRequest.add(request);
        }
        return client().bulk(bulkRequest).actionGet();
    }

    public class AsyncIndexingService {
        private AtomicBoolean finished = new AtomicBoolean();
        private AtomicInteger numAutoGenDocs = new AtomicInteger();
        private Thread indexingThread;
        private String indexName;

        AsyncIndexingService(String indexName) {
            this(indexName, Integer.MAX_VALUE);
        }

        AsyncIndexingService(String indexName, int maxDocs) {
            indexingThread = new Thread(() -> {
                while (finished.get() == false && numAutoGenDocs.get() < maxDocs) {
                    IndexResponse indexResponse = client().prepareIndex(indexName).setId("id").setSource("field", "value").get();
                    assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                    DeleteResponse deleteResponse = client().prepareDelete("test", "id").get();
                    assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                    client().prepareIndex(indexName).setSource("auto", true).get();
                    numAutoGenDocs.incrementAndGet();
                    logger.info("Indexed {} docs here", numAutoGenDocs.get());
                }
            });
        }

        public void stopIndexing() throws InterruptedException {
            finished.set(true);
            indexingThread.join();
        }

        public int totalIndexedDocs() {
            return numAutoGenDocs.get();
        }

        public void startIndexing() {
            indexingThread.start();
        }

        public Thread getIndexingThread() {
            return indexingThread;
        }
    }

    public class SyncIndexingService {
        private int maxDocs;
        private int currentIndexedDocs;
        private boolean forceStop;
        private String indexName;

        SyncIndexingService(String indexName) {
            this(indexName, Integer.MAX_VALUE);
        }

        SyncIndexingService(String indexName, int maxDocs) {
            this.indexName = indexName;
            this.maxDocs = maxDocs;
            this.forceStop = false;
        }

        public void forceStopIndexing() throws InterruptedException {
            this.forceStop = true;
        }

        public int getCurrentIndexedDocs() {
            return currentIndexedDocs;
        }

        public void startIndexing() {
            while (currentIndexedDocs < maxDocs && forceStop == false) {
                IndexResponse indexResponse = client().prepareIndex(indexName).setId("id").setSource("field", "value").get();
                assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                DeleteResponse deleteResponse = client().prepareDelete(indexName, "id").get();
                assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                client().prepareIndex(indexName).setSource("auto", true).get();
                currentIndexedDocs += 1;
                logger.info("Indexed {} docs here", currentIndexedDocs);
            }
        }
    }
}
