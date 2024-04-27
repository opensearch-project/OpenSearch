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

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.NodeClosedException;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;
import org.junit.After;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public abstract class AbstractSnapshotIntegTestCase extends OpenSearchIntegTestCase {

    protected final static String TEST_REMOTE_STORE_REPO_SUFFIX = "__rs";
    private static final String OLD_VERSION_SNAPSHOT_PREFIX = "old-version-snapshot-";

    // Large snapshot pool settings to set up nodes for tests involving multiple repositories that need to have enough
    // threads so that blocking some threads on one repository doesn't block other repositories from doing work
    protected static final Settings LARGE_SNAPSHOT_POOL_SETTINGS = Settings.builder()
        .put("thread_pool.snapshot.core", 5)
        .put("thread_pool.snapshot.max", 5)
        .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Rebalancing is causing some checks after restore to randomly fail
            // due to https://github.com/elastic/elasticsearch/issues/9421
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockRepository.Plugin.class);
    }

    @After
    public void assertConsistentHistoryInLuceneIndex() throws Exception {
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
    }

    @After
    public void verifyNoLeakedListeners() throws Exception {
        assertBusy(() -> {
            for (SnapshotsService snapshotsService : internalCluster().getInstances(SnapshotsService.class)) {
                assertTrue(snapshotsService.assertAllListenersResolved());
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private String skipRepoConsistencyCheckReason;

    @After
    public void assertRepoConsistency() {
        if (skipRepoConsistencyCheckReason == null) {
            clusterAdmin().prepareGetRepositories()
                .get()
                .repositories()
                .stream()
                .filter(repositoryMetadata -> !repositoryMetadata.name().endsWith(TEST_REMOTE_STORE_REPO_SUFFIX))
                .forEach(repositoryMetadata -> {
                    final String name = repositoryMetadata.name();
                    if (repositoryMetadata.settings().getAsBoolean("readonly", false) == false) {
                        clusterAdmin().prepareDeleteSnapshot(name, OLD_VERSION_SNAPSHOT_PREFIX + "*").get();
                        clusterAdmin().prepareCleanupRepository(name).get();
                    }
                    BlobStoreTestUtil.assertRepoConsistency(internalCluster(), name);
                });
        } else {
            logger.info("--> skipped repo consistency checks because [{}]", skipRepoConsistencyCheckReason);
        }
    }

    protected void disableRepoConsistencyCheck(String reason) {
        assertNotNull(reason);
        skipRepoConsistencyCheckReason = reason;
    }

    protected RepositoryData getRepositoryData(String repository) {
        return getRepositoryData(internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class).repository(repository));
    }

    protected RepositoryData getRepositoryData(Repository repository) {
        return PlainActionFuture.get(repository::getRepositoryData);
    }

    public static long getFailureCount(String repository) {
        long failureCount = 0;
        for (RepositoriesService repositoriesService : internalCluster().getDataOrClusterManagerNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            failureCount += mockRepository.getFailureCount();
        }
        return failureCount;
    }

    public static void assertFileCount(Path dir, int expectedCount) throws IOException {
        final List<Path> found = new ArrayList<>();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                found.add(file);
                return FileVisitResult.CONTINUE;
            }
        });
        assertEquals("Unexpected file count, found: [" + found + "].", expectedCount, found.size());
    }

    public static int numberOfFiles(Path dir) throws IOException {
        final AtomicInteger count = new AtomicInteger();
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                count.incrementAndGet();
                return FileVisitResult.CONTINUE;
            }
        });
        return count.get();
    }

    public static void stopNode(final String node) throws IOException {
        internalCluster().stopRandomNode(settings -> settings.get("node.name").equals(node));
    }

    public void waitForBlock(String node, String repository, TimeValue timeout) throws InterruptedException {
        logger.info("--> waiting for [{}] to be blocked on node [{}]", repository, node);
        long start = System.currentTimeMillis();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, node);
        MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
        while (System.currentTimeMillis() - start < timeout.millis()) {
            if (mockRepository.blocked()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timeout waiting for node [" + node + "] to be blocked");
    }

    public SnapshotInfo waitForCompletion(String repository, String snapshotName, TimeValue timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeout.millis()) {
            final SnapshotInfo snapshotInfo = getSnapshot(repository, snapshotName);
            if (snapshotInfo.state().completed()) {
                // Make sure that snapshot clean up operations are finished
                ClusterStateResponse stateResponse = clusterAdmin().prepareState().get();
                boolean found = false;
                for (SnapshotsInProgress.Entry entry : stateResponse.getState()
                    .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                    .entries()) {
                    final Snapshot curr = entry.snapshot();
                    if (curr.getRepository().equals(repository) && curr.getSnapshotId().getName().equals(snapshotName)) {
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    return snapshotInfo;
                }
            }
            Thread.sleep(100);
        }
        fail("Timeout!!!");
        return null;
    }

    public static String blockClusterManagerFromFinalizingSnapshotOnIndexFile(final String repositoryName) {
        final String clusterManagerName = internalCluster().getClusterManagerName();
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManagerName).repository(repositoryName))
            .setBlockAndFailOnWriteIndexFile();
        return clusterManagerName;
    }

    public static String blockClusterManagerOnWriteIndexFile(final String repositoryName) {
        final String clusterManagerName = internalCluster().getClusterManagerName();
        ((MockRepository) internalCluster().getClusterManagerNodeInstance(RepositoriesService.class).repository(repositoryName))
            .setBlockOnWriteIndexFile();
        return clusterManagerName;
    }

    public static void blockClusterManagerFromDeletingIndexNFile(String repositoryName) {
        final String clusterManagerName = internalCluster().getClusterManagerName();
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManagerName).repository(repositoryName))
            .setBlockOnDeleteIndexFile();
    }

    public static String blockClusterManagerFromFinalizingSnapshotOnSnapFile(final String repositoryName) {
        final String clusterManagerName = internalCluster().getClusterManagerName();
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, clusterManagerName).repository(repositoryName))
            .setBlockAndFailOnWriteSnapFiles(true);
        return clusterManagerName;
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #blockClusterManagerFromFinalizingSnapshotOnIndexFile(String)} */
    @Deprecated
    public static String blockMasterFromFinalizingSnapshotOnIndexFile(final String repositoryName) {
        return blockClusterManagerFromFinalizingSnapshotOnIndexFile(repositoryName);
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #blockClusterManagerOnWriteIndexFile(String)} */
    @Deprecated
    public static String blockMasterOnWriteIndexFile(final String repositoryName) {
        return blockClusterManagerOnWriteIndexFile(repositoryName);
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #blockClusterManagerFromDeletingIndexNFile(String)} */
    @Deprecated
    public static void blockMasterFromDeletingIndexNFile(String repositoryName) {
        blockClusterManagerFromDeletingIndexNFile(repositoryName);
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #blockClusterManagerFromFinalizingSnapshotOnSnapFile(String)} */
    @Deprecated
    public static String blockMasterFromFinalizingSnapshotOnSnapFile(final String repositoryName) {
        return blockClusterManagerFromFinalizingSnapshotOnSnapFile(repositoryName);
    }

    public static String blockNodeWithIndex(final String repositoryName, final String indexName) {
        for (String node : internalCluster().nodesInclude(indexName)) {
            ((MockRepository) internalCluster().getInstance(RepositoriesService.class, node).repository(repositoryName)).blockOnDataFiles(
                true
            );
            return node;
        }
        fail("No nodes for the index " + indexName + " found");
        return null;
    }

    public static void blockNodeOnAnyFiles(String repository, String nodeName) {
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, nodeName).repository(repository)).setBlockOnAnyFiles(
            true
        );
    }

    public static void blockNodeOnAnySegmentFile(String repository, String nodeName) {
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, nodeName).repository(repository)).blockOnSegmentFiles(
            true
        );
    }

    public static void blockDataNode(String repository, String nodeName) {
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, nodeName).repository(repository)).blockOnDataFiles(true);
    }

    public static void blockAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repository)).blockOnDataFiles(true);
        }
    }

    public static void unblockAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            ((MockRepository) repositoriesService.repository(repository)).unblock();
        }
    }

    public static void failReadsAllDataNodes(String repository) {
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
            mockRepository.setFailReadsAfterUnblock(true);
        }
    }

    public static void waitForBlockOnAnyDataNode(String repository, TimeValue timeout) throws InterruptedException {
        final boolean blocked = waitUntil(() -> {
            for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
                MockRepository mockRepository = (MockRepository) repositoriesService.repository(repository);
                if (mockRepository.blocked()) {
                    return true;
                }
            }
            return false;
        }, timeout.millis(), TimeUnit.MILLISECONDS);

        assertTrue("No repository is blocked waiting on a data node", blocked);
    }

    public void unblockNode(final String repository, final String node) {
        logger.info("--> unblocking [{}] on node [{}]", repository, node);
        ((MockRepository) internalCluster().getInstance(RepositoriesService.class, node).repository(repository)).unblock();
    }

    protected void createRepository(String repoName, String type, Settings.Builder settings) {
        logger.info("--> creating repository [{}] [{}]", repoName, type);
        assertAcked(clusterAdmin().preparePutRepository(repoName).setType(type).setSettings(settings));
    }

    protected void updateRepository(String repoName, String type, Settings.Builder settings) {
        logger.info("--> updating repository [{}] [{}]", repoName, type);
        assertAcked(clusterAdmin().preparePutRepository(repoName).setType(type).setSettings(settings));
    }

    protected void createRepository(String repoName, String type, Path location) {
        createRepository(repoName, type, Settings.builder().put("location", location));
    }

    protected Settings.Builder getRepositorySettings(Path location, boolean shallowCopyEnabled) {
        Settings.Builder settingsBuilder = randomRepositorySettings();
        settingsBuilder.put("location", location);
        if (shallowCopyEnabled) {
            settingsBuilder.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true);
        }
        return settingsBuilder;
    }

    protected Settings.Builder getRepositorySettings(Path location, String basePath, boolean shallowCopyEnabled) {
        Settings.Builder settingsBuilder = randomRepositorySettings();
        settingsBuilder.put("location", location);
        if (shallowCopyEnabled) {
            settingsBuilder.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true);
        }
        if (basePath != null) {
            settingsBuilder.put("base_path", basePath);
        }
        return settingsBuilder;
    }

    protected void createRepository(String repoName, String type) {
        createRepository(repoName, type, randomRepositorySettings());
    }

    protected Settings.Builder randomRepositorySettings() {
        final Settings.Builder settings = Settings.builder();
        final boolean compress = randomBoolean();
        settings.put("location", randomRepoPath()).put("compress", compress);
        if (compress) {
            settings.put("compression_type", randomFrom(CompressorRegistry.registeredCompressors().keySet()));
        }
        if (rarely()) {
            settings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
        }
        return settings;
    }

    protected Settings.Builder snapshotRepoSettingsForShallowCopy() {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", randomRepoPath());
        settings.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE);
        return settings;
    }

    protected static Settings.Builder indexSettingsNoReplicas(int shards) {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
    }

    /**
     * Randomly write an empty snapshot of an older version to an empty repository to simulate an older repository metadata format.
     */
    protected void maybeInitWithOldSnapshotVersion(String repoName, Path repoPath) throws IOException {
        if (randomBoolean() && randomBoolean()) {
            initWithSnapshotVersion(repoName, repoPath, VersionUtils.randomIndexCompatibleVersion(random()));
        }
    }

    /**
     * Workaround to simulate BwC situation: taking a snapshot without indices here so that we don't create any new version shard
     * generations (the existence of which would short-circuit checks for the repo containing old version snapshots)
     */
    protected String initWithSnapshotVersion(String repoName, Path repoPath, Version version) throws IOException {
        assertThat("This hack only works on an empty repository", getRepositoryData(repoName).getSnapshotIds(), empty());
        final String oldVersionSnapshot = OLD_VERSION_SNAPSHOT_PREFIX + version.id;
        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(repoName, oldVersionSnapshot)
            .setIndices("does-not-exist-for-sure-*")
            .setWaitForCompletion(true)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), is(0));

        logger.info("--> writing downgraded RepositoryData for repository metadata version [{}]", version);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final XContentBuilder jsonBuilder = JsonXContent.contentBuilder();
        repositoryData.snapshotsToXContent(jsonBuilder, version);
        final RepositoryData downgradedRepoData = RepositoryData.snapshotsFromXContent(
            JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                jsonBuilder.toString().replace(Version.CURRENT.toString(), version.toString())
            ),
            repositoryData.getGenId(),
            randomBoolean()
        );
        Files.write(
            repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + repositoryData.getGenId()),
            BytesReference.toBytes(BytesReference.bytes(downgradedRepoData.snapshotsToXContent(XContentFactory.jsonBuilder(), version))),
            StandardOpenOption.TRUNCATE_EXISTING
        );
        return oldVersionSnapshot;
    }

    protected SnapshotInfo createFullSnapshot(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] in [{}]", snapshotName, repoName);
        final CreateSnapshotResponse response = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = response.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        return snapshotInfo;
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

    protected void createIndexWithRandomDocs(String indexName, int docCount) throws InterruptedException {
        createIndex(indexName);
        ensureGreen();
        indexRandomDocs(indexName, docCount);
    }

    protected void indexRandomDocs(String index, int numdocs) throws InterruptedException {
        logger.info("--> indexing [{}] documents into [{}]", numdocs, index);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(index).setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        flushAndRefresh(index);
        assertDocCount(index, numdocs);
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

    protected Settings.Builder snapshotRepoSettingsForShallowCopy(Path path) {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", path);
        settings.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), Boolean.TRUE);
        return settings;
    }

    protected long getCountForIndex(String indexName) {
        return client().search(
            new SearchRequest(new SearchRequest(indexName).source(new SearchSourceBuilder().size(0).trackTotalHits(true)))
        ).actionGet().getHits().getTotalHits().value;
    }

    protected void assertDocCount(String index, long count) {
        assertEquals(getCountForIndex(index), count);
    }

    protected String[] getLockFilesInRemoteStore(String remoteStoreIndex, String remoteStoreRepositoryName) throws IOException {
        final RepositoriesService repositoriesService = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class);
        final BlobStoreRepository remoteStoreRepository = (BlobStoreRepository) repositoriesService.repository(remoteStoreRepositoryName);
        BlobPath shardLevelBlobPath = getShardLevelBlobPath(
            client(),
            remoteStoreIndex,
            remoteStoreRepository.basePath(),
            "0",
            SEGMENTS,
            LOCK_FILES
        );
        BlobContainer blobContainer = remoteStoreRepository.blobStore().blobContainer(shardLevelBlobPath);
        try (RemoteBufferedOutputDirectory lockDirectory = new RemoteBufferedOutputDirectory(blobContainer)) {
            return lockDirectory.listAll();
        }
    }

    /**
     * Adds a snapshot in state {@link SnapshotState#FAILED} to the given repository.
     *
     * @param repoName     repository to add snapshot to
     * @param snapshotName name for the new failed snapshot
     * @param metadata     snapshot metadata to write (as returned by {@link SnapshotInfo#userMetadata()})
     */
    protected void addBwCFailedSnapshot(String repoName, String snapshotName, Map<String, Object> metadata) throws Exception {
        final ClusterState state = clusterAdmin().prepareState().get().getState();
        final RepositoriesMetadata repositoriesMetadata = state.metadata().custom(RepositoriesMetadata.TYPE);
        assertNotNull(repositoriesMetadata);
        final RepositoryMetadata initialRepoMetadata = repositoriesMetadata.repository(repoName);
        assertNotNull(initialRepoMetadata);
        assertThat(
            "We can only manually insert a snapshot into a repository that does not have a generation tracked in the CS",
            initialRepoMetadata.generation(),
            is(RepositoryData.UNKNOWN_REPO_GEN)
        );
        final Repository repo = internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class).repository(repoName);
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID(random()));
        logger.info("--> adding old version FAILED snapshot [{}] to repository [{}]", snapshotId, repoName);
        final SnapshotInfo snapshotInfo = new SnapshotInfo(
            snapshotId,
            Collections.emptyList(),
            Collections.emptyList(),
            SnapshotState.FAILED,
            "failed on purpose",
            SnapshotsService.OLD_SNAPSHOT_FORMAT,
            0L,
            0L,
            0,
            0,
            Collections.emptyList(),
            randomBoolean(),
            metadata,
            false
        );
        PlainActionFuture.<RepositoryData, Exception>get(
            f -> repo.finalizeSnapshot(
                ShardGenerations.EMPTY,
                getRepositoryData(repoName).getGenId(),
                state.metadata(),
                snapshotInfo,
                SnapshotsService.OLD_SNAPSHOT_FORMAT,
                Function.identity(),
                f
            )
        );
    }

    protected void awaitNoMoreRunningOperations() throws Exception {
        awaitNoMoreRunningOperations(internalCluster().getClusterManagerName());
    }

    protected void awaitNoMoreRunningOperations(String viaNode) throws Exception {
        logger.info("--> verify no more operations in the cluster state");
        awaitClusterState(
            viaNode,
            state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().isEmpty()
                && state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).hasDeletionsInProgress() == false
        );
    }

    protected void awaitClusterState(Predicate<ClusterState> statePredicate) throws Exception {
        awaitClusterState(internalCluster().getClusterManagerName(), statePredicate);
    }

    protected void awaitClusterState(String viaNode, Predicate<ClusterState> statePredicate) throws Exception {
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, viaNode);
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, viaNode);
        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
        if (statePredicate.test(observer.setAndGetObservedState()) == false) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    future.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    future.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    future.onFailure(new TimeoutException());
                }
            }, statePredicate);
            future.get(30L, TimeUnit.SECONDS);
        }
    }

    protected ActionFuture<CreateSnapshotResponse> startFullSnapshotBlockedOnDataNode(String snapshotName, String repoName, String dataNode)
        throws InterruptedException {
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> fut = startFullSnapshot(repoName, snapshotName);
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }

    protected ActionFuture<CreateSnapshotResponse> startFullSnapshot(String repoName, String snapshotName) {
        return startFullSnapshot(repoName, snapshotName, false);
    }

    protected ActionFuture<CreateSnapshotResponse> startFullSnapshot(String repoName, String snapshotName, boolean partial) {
        logger.info("--> creating full snapshot [{}] to repo [{}]", snapshotName, repoName);
        return clusterAdmin().prepareCreateSnapshot(repoName, snapshotName).setWaitForCompletion(true).setPartial(partial).execute();
    }

    protected void awaitNumberOfSnapshotsInProgress(int count) throws Exception {
        logger.info("--> wait for [{}] snapshots to show up in the cluster state", count);
        awaitClusterState(state -> state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries().size() == count);
    }

    protected static SnapshotInfo assertSuccessful(ActionFuture<CreateSnapshotResponse> future) throws Exception {
        final SnapshotInfo snapshotInfo = future.get().getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        return snapshotInfo;
    }

    private static final Settings SINGLE_SHARD_NO_REPLICA = indexSettingsNoReplicas(1).build();

    protected void createIndexWithContent(String indexName) {
        createIndexWithContent(indexName, SINGLE_SHARD_NO_REPLICA);
    }

    protected void createIndexWithContent(String indexName, Settings indexSettings) {
        logger.info("--> creating index [{}]", indexName);
        createIndex(indexName, indexSettings);
        ensureGreen(indexName);
        index(indexName, "_doc", "some_id", "foo", "bar");
    }

    protected ActionFuture<AcknowledgedResponse> startDeleteSnapshot(String repoName, String snapshotName) {
        logger.info("--> deleting snapshot [{}] from repo [{}]", snapshotName, repoName);
        return clusterAdmin().prepareDeleteSnapshot(repoName, snapshotName).execute();
    }

    protected void updateClusterState(final Function<ClusterState, ClusterState> updater) throws Exception {
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        final ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);
        clusterService.submitStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updater.apply(currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                future.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                future.onResponse(null);
            }
        });
        future.get();
    }

    protected SnapshotInfo getSnapshot(String repository, String snapshot) {
        final List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots(repository).setSnapshots(snapshot).get().getSnapshots();
        assertThat(snapshotInfos, hasSize(1));
        return snapshotInfos.get(0);
    }

    protected void awaitClusterManagerFinishRepoOperations() throws Exception {
        logger.info("--> waiting for cluster-manager to finish all repo operations on its SNAPSHOT pool");
        final ThreadPool clusterManagerThreadPool = internalCluster().getClusterManagerNodeInstance(ThreadPool.class);
        assertBusy(() -> {
            for (ThreadPoolStats.Stats stat : clusterManagerThreadPool.stats()) {
                if (ThreadPool.Names.SNAPSHOT.equals(stat.getName())) {
                    assertEquals(stat.getActive(), 0);
                    break;
                }
            }
        });
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #awaitClusterManagerFinishRepoOperations()} */
    @Deprecated
    protected void awaitMasterFinishRepoOperations() throws Exception {
        awaitClusterManagerFinishRepoOperations();
    }
}
