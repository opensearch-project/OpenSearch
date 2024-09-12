/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.repositories.fs.ReloadableFsRepository.REPOSITORIES_FAILRATE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class MigrationBaseTestCase extends OpenSearchIntegTestCase {
    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;
    boolean addRemote = false;
    Settings extraSettings = Settings.EMPTY;

    private final List<String> documentKeys = List.of(
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5)
    );

    void setAddRemote(boolean addRemote) {
        this.addRemote = addRemote;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        setAddRemote(false);
    }

    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (addRemote) {
            logger.info("Adding remote store node");
            return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(extraSettings)
                .put(remoteStoreClusterSettings(REPOSITORY_NAME, super.segmentRepoPath, REPOSITORY_2_NAME, super.translogRepoPath))
                .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
                .build();
        } else {
            logger.info("Adding docrep node");
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true).build();
        }
    }

    protected void setFailRate(String repoName, int value) throws ExecutionException, InterruptedException {
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { repoName });
        GetRepositoriesResponse res = client().admin().cluster().getRepositories(gr).get();
        RepositoryMetadata rmd = res.repositories().get(0);
        Settings.Builder settings = Settings.builder()
            .put("location", rmd.settings().get("location"))
            .put(REPOSITORIES_FAILRATE_SETTING.getKey(), value);
        createRepository(repoName, ReloadableFsRepository.TYPE, settings);
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

    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(60), indices);
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

    Map<String, Integer> getShardCountByNodeId() {
        final Map<String, Integer> shardCountByNodeId = new HashMap<>();
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (final RoutingNode node : clusterState.getRoutingNodes()) {
            logger.info(
                "----> node {} has {} shards",
                node.nodeId(),
                clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards()
            );
            shardCountByNodeId.put(node.nodeId(), clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }
        return shardCountByNodeId;
    }

    private void indexSingleDoc(String indexName) {
        IndexResponse indexResponse = client().prepareIndex(indexName).setId("id").setSource("field", "value").get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
        DeleteResponse deleteResponse = client().prepareDelete(indexName, "id").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
        client().prepareIndex(indexName).setSource("auto", true).get();
    }

    public class AsyncIndexingService {
        private String indexName;
        private AtomicLong indexedDocs = new AtomicLong(0);
        private AtomicBoolean finished = new AtomicBoolean();
        private Thread indexingThread;

        private int refreshFrequency = 3;

        AsyncIndexingService(String indexName) {
            this.indexName = indexName;
        }

        public void startIndexing() {
            indexingThread = getIndexingThread();
            indexingThread.start();
        }

        public void stopIndexing() throws InterruptedException {
            finished.set(true);
            indexingThread.join();
        }

        public long getIndexedDocs() {
            return indexedDocs.get();
        }

        private Thread getIndexingThread() {
            return new Thread(() -> {
                while (finished.get() == false) {
                    indexSingleDoc(indexName);
                    long currentDocCount = indexedDocs.incrementAndGet();
                    if (currentDocCount > 0 && currentDocCount % refreshFrequency == 0) {
                        if (rarely()) {
                            client().admin().indices().prepareFlush(indexName).get();
                            logger.info("Completed ingestion of {} docs. Flushing now", currentDocCount);
                        } else {
                            client().admin().indices().prepareRefresh(indexName).get();
                        }
                    }
                }
            });
        }

        public void setRefreshFrequency(int refreshFrequency) {
            this.refreshFrequency = refreshFrequency;
        }
    }

    public void excludeNodeSet(String attr, String value) {
        assertAcked(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.exclude._" + attr, value))
                .get()
        );
    }

    public void stopShardRebalancing() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none").build())
                .get()
        );
    }

    public ClusterHealthStatus waitForRelocation() {
        ClusterHealthRequest request = Requests.clusterHealthRequest()
            .waitForNoRelocatingShards(true)
            .timeout(TimeValue.timeValueSeconds(60))
            .waitForEvents(Priority.LANGUID);
        ClusterHealthResponse actionGet = client().admin().cluster().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "waitForRelocation timed out, cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get()
            );
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }

    public ClusterHealthStatus waitForRelocation(TimeValue t) {
        ClusterHealthRequest request = Requests.clusterHealthRequest()
            .waitForNoRelocatingShards(true)
            .timeout(t)
            .waitForEvents(Priority.LANGUID);
        ClusterHealthResponse actionGet = client().admin().cluster().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "waitForRelocation timed out, cluster state:\n{}\n{}",
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get()
            );
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        return actionGet.getStatus();
    }

    protected IndexShard getIndexShard(String dataNode, String indexName) throws ExecutionException, InterruptedException {
        String clusterManagerName = internalCluster().getClusterManagerName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNode);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = indicesService.indexService(new Index(indexName, uuid));
        return indexService.getShard(0);
    }

    public void changeReplicaCountAndEnsureGreen(int replicaCount, String indexName) {
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount))
        );
        ensureGreen(indexName);
    }

    public void completeDocRepToRemoteMigration() {
        assertTrue(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .putNull(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey())
                        .putNull(MIGRATION_DIRECTION_SETTING.getKey())
                )
                .get()
                .isAcknowledged()
        );
    }
}
