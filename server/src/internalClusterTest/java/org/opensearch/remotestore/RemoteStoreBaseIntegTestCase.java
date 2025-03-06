/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.remotestore.translogmetadata.mocks.MockFsMetadataSupportedRepositoryPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.routing.allocation.decider.SearchReplicaAllocationDecider.SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.repositories.fs.ReloadableFsRepository.REPOSITORIES_FAILRATE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class RemoteStoreBaseIntegTestCase extends OpenSearchIntegTestCase {
    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";
    protected static final String REMOTE_ROUTING_TABLE_REPO = "remote-routing-table-repo";
    protected static final int SHARD_COUNT = 1;
    protected static int REPLICA_COUNT = 1;
    protected static final String TOTAL_OPERATIONS = "total-operations";
    protected static final String REFRESHED_OR_FLUSHED_OPERATIONS = "refreshed-or-flushed-operations";
    protected static final String MAX_SEQ_NO_TOTAL = "max-seq-no-total";
    protected static final String MAX_SEQ_NO_REFRESHED_OR_FLUSHED = "max-seq-no-refreshed-or-flushed";

    protected Path segmentRepoPath;
    protected Path translogRepoPath;
    protected boolean clusterSettingsSuppliedByTest = false;
    protected boolean asyncUploadMockFsRepo = randomBoolean();
    private boolean metadataSupportedType = randomBoolean();
    private final List<String> documentKeys = List.of(
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5)
    );

    protected Map<String, Long> indexData(int numberOfIterations, boolean invokeFlush, String index) {
        return indexData(numberOfIterations, invokeFlush, false, index);
    }

    protected Map<String, Long> indexData(int numberOfIterations, boolean invokeFlush, boolean emptyTranslog, String index) {
        long totalOperations = 0;
        long refreshedOrFlushedOperations = 0;
        long maxSeqNo = -1;
        long maxSeqNoRefreshedOrFlushed = -1;
        int shardId = 0;
        Map<String, Long> indexingStats = new HashMap<>();
        for (int i = 0; i < numberOfIterations; i++) {
            if (invokeFlush) {
                flushAndRefresh(index);
            } else {
                refresh(index);
            }

            // skip indexing if last iteration as we dont want to have any data in remote translog
            if (emptyTranslog && i == numberOfIterations - 1) {
                continue;
            }
            maxSeqNoRefreshedOrFlushed = maxSeqNo;
            indexingStats.put(MAX_SEQ_NO_REFRESHED_OR_FLUSHED + "-shard-" + shardId, maxSeqNoRefreshedOrFlushed);
            refreshedOrFlushedOperations = totalOperations;
            int numberOfOperations = randomIntBetween(20, 50);
            int numberOfBulk = randomIntBetween(1, 5);
            for (int j = 0; j < numberOfBulk; j++) {
                BulkResponse res = indexBulk(index, numberOfOperations);
                for (BulkItemResponse singleResp : res.getItems()) {
                    indexingStats.put(
                        MAX_SEQ_NO_TOTAL + "-shard-" + singleResp.getResponse().getShardId().id(),
                        singleResp.getResponse().getSeqNo()
                    );
                    maxSeqNo = singleResp.getResponse().getSeqNo();
                }
                totalOperations += numberOfOperations;
            }
        }

        indexingStats.put(TOTAL_OPERATIONS, totalOperations);
        indexingStats.put(REFRESHED_OR_FLUSHED_OPERATIONS, refreshedOrFlushedOperations);
        indexingStats.put(MAX_SEQ_NO_TOTAL, maxSeqNo);
        indexingStats.put(MAX_SEQ_NO_REFRESHED_OR_FLUSHED, maxSeqNoRefreshedOrFlushed);
        return indexingStats;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        if (!clusterSettingsSuppliedByTest && asyncUploadMockFsRepo) {
            if (metadataSupportedType) {
                return Stream.concat(super.nodePlugins().stream(), Stream.of(MockFsMetadataSupportedRepositoryPlugin.class))
                    .collect(Collectors.toList());
            } else {
                return Stream.concat(super.nodePlugins().stream(), Stream.of(MockFsRepositoryPlugin.class)).collect(Collectors.toList());
            }
        }
        return super.nodePlugins();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        if (clusterSettingsSuppliedByTest) {
            return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
        } else {
            if (asyncUploadMockFsRepo) {
                String repoType = metadataSupportedType ? MockFsMetadataSupportedRepositoryPlugin.TYPE_MD : MockFsRepositoryPlugin.TYPE;
                return Settings.builder()
                    .put(super.nodeSettings(nodeOrdinal))
                    .put(
                        remoteStoreClusterSettings(
                            REPOSITORY_NAME,
                            segmentRepoPath,
                            repoType,
                            REPOSITORY_2_NAME,
                            translogRepoPath,
                            repoType
                        )
                    )
                    .build();
            } else {
                return Settings.builder()
                    .put(super.nodeSettings(nodeOrdinal))
                    .put(remoteStoreClusterSettings(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, translogRepoPath))
                    .build();
            }
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

    public Settings indexSettings() {
        return defaultIndexSettings();
    }

    protected IndexResponse indexSingleDoc(String indexName) {
        return indexSingleDoc(indexName, false);
    }

    protected IndexResponse indexSingleDoc(String indexName, boolean forceRefresh) {
        IndexRequestBuilder indexRequestBuilder = client().prepareIndex(indexName)
            .setId(UUIDs.randomBase64UUID())
            .setSource(documentKeys.get(randomIntBetween(0, documentKeys.size() - 1)), randomAlphaOfLength(5));
        if (forceRefresh) {
            indexRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        return indexRequestBuilder.get();
    }

    protected BulkResponse indexBulk(String indexName, int numDocs) {
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

    Settings defaultIndexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas, int numberOfShards) {
        return Settings.builder()
            .put(defaultIndexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas) {
        return remoteStoreIndexSettings(numberOfReplicas, 1);
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas, long totalFieldLimit, int refresh) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(numberOfReplicas))
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), totalFieldLimit)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), String.valueOf(refresh))
            .build();
    }

    @After
    public void teardown() {
        clusterSettingsSuppliedByTest = false;
        asyncUploadMockFsRepo = randomBoolean();
        metadataSupportedType = randomBoolean();
        assertRemoteStoreRepositoryOnAllNodes(REPOSITORY_NAME);
        assertRemoteStoreRepositoryOnAllNodes(REPOSITORY_2_NAME);
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
        clusterAdmin().prepareCleanupRepository(REPOSITORY_2_NAME).get();
    }

    public RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String type = nodeAttributes.get(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));

        String settingsAttributeKeyPrefix = String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name);
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));
        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build());
    }

    public void assertRemoteStoreRepositoryOnAllNodes(String repositoryName) {
        RepositoriesMetadata repositories = internalCluster().getInstance(ClusterService.class, internalCluster().getNodeNames()[0])
            .state()
            .metadata()
            .custom(RepositoriesMetadata.TYPE);
        RepositoryMetadata actualRepository = repositories.repository(repositoryName);

        final RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(repositoryName);

        for (String nodeName : internalCluster().getNodeNames()) {
            ClusterService clusterService = internalCluster().getInstance(ClusterService.class, nodeName);
            DiscoveryNode node = clusterService.localNode();
            RepositoryMetadata expectedRepository = buildRepositoryMetadata(node, repositoryName);

            // Validated that all the restricted settings are entact on all the nodes.
            repository.getRestrictedSystemRepositorySettings()
                .stream()
                .forEach(
                    setting -> assertEquals(
                        String.format(Locale.ROOT, "Restricted Settings mismatch [%s]", setting.getKey()),
                        setting.get(actualRepository.settings()),
                        setting.get(expectedRepository.settings())
                    )
                );
        }
    }

    public static int getFileCount(Path path) throws IOException {
        final AtomicInteger filesExisting = new AtomicInteger(0);
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException impossible) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                filesExisting.incrementAndGet();
                return FileVisitResult.CONTINUE;
            }
        });

        return filesExisting.get();
    }

    protected IndexShard getIndexShard(String dataNode, String indexName) throws ExecutionException, InterruptedException {
        String clusterManagerName = internalCluster().getClusterManagerName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNode);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        String uuid = getIndexResponse.getSettings().get(indexName).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = indicesService.indexService(new Index(indexName, uuid));
        return indexService.getShard(0);
    }

    protected void restore(boolean restoreAllShards, String... indices) {
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(indices).restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
    }

    protected void prepareCluster(int numClusterManagerNodes, int numDataOnlyNodes, String indices, int replicaCount, int shardCount) {
        prepareCluster(numClusterManagerNodes, numDataOnlyNodes, indices, replicaCount, shardCount, Settings.EMPTY);
    }

    protected void prepareCluster(
        int numClusterManagerNodes,
        int numDataOnlyNodes,
        String indices,
        int replicaCount,
        int shardCount,
        Settings settings
    ) {
        prepareCluster(numClusterManagerNodes, numDataOnlyNodes, settings);
        for (String index : indices.split(",")) {
            createIndex(index, remoteStoreIndexSettings(replicaCount, shardCount));
            ensureYellowAndNoInitializingShards(index);
            ensureGreen(index);
        }
    }

    protected void prepareCluster(int numClusterManagerNodes, int numDataOnlyNodes, Settings settings) {
        internalCluster().startClusterManagerOnlyNodes(numClusterManagerNodes, settings);
        internalCluster().startDataOnlyNodes(numDataOnlyNodes, settings);
    }

    protected void setSearchDedicatedNodeSettings(String nodeName) {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", nodeName))
            .execute()
            .actionGet();
    }
}
