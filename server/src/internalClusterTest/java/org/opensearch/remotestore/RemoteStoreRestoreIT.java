/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThan;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoteStoreRestoreIT extends BaseRemoteStoreRestoreIT {

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRemoteTranslogRestoreWithNoDataPostCommit() throws Exception {
        testRestoreFlow(1, true, randomIntBetween(1, 5));
    }

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRemoteTranslogRestoreWithNoDataPostRefresh() throws Exception {
        testRestoreFlow(1, false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRemoteTranslogRestoreWithRefreshedData() throws Exception {
        testRestoreFlow(randomIntBetween(2, 5), false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRemoteTranslogRestoreWithCommittedData() throws Exception {
        testRestoreFlow(randomIntBetween(2, 5), true, randomIntBetween(1, 5));
    }

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRTSRestoreWithNoDataPostCommitPrimaryReplicaDown() throws Exception {
        testRestoreFlowBothPrimaryReplicasDown(1, true, randomIntBetween(1, 5));
    }

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRTSRestoreWithNoDataPostRefreshPrimaryReplicaDown() throws Exception {
        testRestoreFlowBothPrimaryReplicasDown(1, false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRTSRestoreWithRefreshedDataPrimaryReplicaDown() throws Exception {
        testRestoreFlowBothPrimaryReplicasDown(randomIntBetween(2, 5), false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRTSRestoreWithCommittedDataPrimaryReplicaDown() throws Exception {
        testRestoreFlowBothPrimaryReplicasDown(randomIntBetween(2, 5), true, randomIntBetween(1, 5));
    }

    private void restoreAndVerify(int shardCount, int replicaCount, Map<String, Long> indexStats) throws Exception {
        restore(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        // This is required to get updated number from already active shards which were not restored
        assertEquals(shardCount * (1 + replicaCount), getNumShards(INDEX_NAME).totalNumShards);
        assertEquals(replicaCount, getNumShards(INDEX_NAME).numReplicas);
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    /**
     * Helper function to test restoring an index with no replication from remote store. Only primary node is dropped.
     * @param numberOfIterations Number of times a refresh/flush should be invoked, followed by indexing some data.
     * @param invokeFlush If true, a flush is invoked. Otherwise, a refresh is invoked.
     * @throws IOException IO Exception.
     */
    private void testRestoreFlow(int numberOfIterations, boolean invokeFlush, int shardCount) throws Exception {
        prepareCluster(1, 3, INDEX_NAME, 0, shardCount);
        Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);

        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(REFRESHED_OR_FLUSHED_OPERATIONS));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        restoreAndVerify(shardCount, 0, indexStats);
    }

    /**
     * Helper function to test restoring an index having replicas from remote store when all the nodes housing the primary/replica drop.
     * @param numberOfIterations Number of times a refresh/flush should be invoked, followed by indexing some data.
     * @param invokeFlush If true, a flush is invoked. Otherwise, a refresh is invoked.
     * @throws IOException IO Exception.
     */
    private void testRestoreFlowBothPrimaryReplicasDown(int numberOfIterations, boolean invokeFlush, int shardCount) throws Exception {
        prepareCluster(1, 2, INDEX_NAME, 1, shardCount);
        Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, INDEX_NAME);
        assertEquals(shardCount * 2, getNumShards(INDEX_NAME).totalNumShards);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(INDEX_NAME)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);
        internalCluster().startDataOnlyNodes(2);

        restoreAndVerify(shardCount, 1, indexStats);
    }

    /**
     * Helper function to test restoring multiple indices from remote store when all the nodes housing the primary/replica drop.
     * @param numberOfIterations Number of times a refresh/flush should be invoked, followed by indexing some data.
     * @param invokeFlush If true, a flush is invoked. Otherwise, a refresh is invoked.
     * @throws IOException IO Exception.
     */
    private void testRestoreFlowMultipleIndices(int numberOfIterations, boolean invokeFlush, int shardCount) throws Exception {
        prepareCluster(1, 3, INDEX_NAMES, 1, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount * 2, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            ClusterHealthStatus indexHealth = ensureRed(index);
            if (ClusterHealthStatus.RED.equals(indexHealth)) {
                continue;
            }

            if (ClusterHealthStatus.GREEN.equals(indexHealth)) {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(index)));
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(INDEX_NAMES_WILDCARD.split(",")).restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
        ensureGreen(indices);
        for (String index : indices) {
            assertEquals(shardCount * 2, getNumShards(index).totalNumShards);
            verifyRestoredData(indicesStats.get(index), index);
        }
    }

    public void testRestoreFlowAllShardsNoRedIndex() throws InterruptedException {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAME, 0, shardCount);
        indexData(randomIntBetween(2, 5), true, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);

        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        client().admin().cluster().restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), future);
        try {
            future.get();
        } catch (ExecutionException e) {
            // If the request goes to co-ordinator, e.getCause() can be RemoteTransportException
            assertTrue(e.getCause() instanceof IllegalStateException || e.getCause().getCause() instanceof IllegalStateException);
        }
    }

    public void testRestoreFlowNoRedIndex() throws Exception {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAME, 0, shardCount);
        Map<String, Long> indexStats = indexData(randomIntBetween(2, 5), true, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);

        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(false), PlainActionFuture.newFuture());

        ensureGreen(INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);
        verifyRestoredData(indexStats, INDEX_NAME);
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store
     * for multiple indices matching a wildcard name pattern.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8480")
    public void testRTSRestoreWithCommittedDataMultipleIndicesPatterns() throws Exception {
        testRestoreFlowMultipleIndices(2, true, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store,
     * with all remote-enabled red indices considered for the restore by default.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8480")
    public void testRTSRestoreWithCommittedDataDefaultAllIndices() throws Exception {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAMES, 1, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(2, true, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            if (ClusterHealthStatus.RED.equals(ensureRed(index))) {
                continue;
            }

            if (ClusterHealthStatus.GREEN.equals(ensureRed(index))) {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(index)));
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        restore(indices);
        ensureGreen(indices);

        for (String index : indices) {
            assertEquals(shardCount, getNumShards(index).totalNumShards);
            verifyRestoredData(indicesStats.get(index), index);
        }
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store,
     * with only some of the remote-enabled red indices requested for the restore.
     * @throws IOException IO Exception.
     */
    public void testRTSRestoreWithCommittedDataNotAllRedRemoteIndices() throws Exception {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAMES, 0, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(2, true, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            if (ClusterHealthStatus.RED.equals(ensureRed(index))) {
                continue;
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices[0], indices[1]));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(indices[0], indices[1]).restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
        ensureGreen(indices[0], indices[1]);
        assertEquals(shardCount, getNumShards(indices[0]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[0]), indices[0]);
        assertEquals(shardCount, getNumShards(indices[1]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[1]), indices[1]);
        ensureRed(indices[2], indices[3]);
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store,
     * with all remote-enabled red indices being considered for the restore
     * except those matching the specified exclusion pattern.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8480")
    public void testRTSRestoreWithCommittedDataExcludeIndicesPatterns() throws Exception {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAMES, 1, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(2, true, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            if (ClusterHealthStatus.RED.equals(ensureRed(index))) {
                continue;
            }

            if (ClusterHealthStatus.GREEN.equals(ensureRed(index))) {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(index)));
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices[0], indices[1]));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices("*", "-remote-store-test-index-*").restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
        ensureGreen(indices[0], indices[1]);
        assertEquals(shardCount, getNumShards(indices[0]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[0]), indices[0]);
        assertEquals(shardCount, getNumShards(indices[1]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[1]), indices[1]);
        ensureRed(indices[2], indices[3]);
    }

    /**
     * Simulates no-op restore from remote store,
     * when the index has no data.
     * @throws IOException IO Exception.
     */
    public void testRTSRestoreDataOnlyInTranslog() throws Exception {
        testRestoreFlow(0, true, randomIntBetween(1, 5));
    }

    public void testRateLimitedRemoteDownloads() throws Exception {
        clusterSettingsSuppliedByTest = true;
        int shardCount = randomIntBetween(1, 3);
        Path segmentRepoPath = randomRepoPath();
        Path tlogRepoPath = randomRepoPath();
        prepareCluster(
            1,
            3,
            INDEX_NAME,
            0,
            shardCount,
            buildRemoteStoreNodeAttributes(REPOSITORY_NAME, segmentRepoPath, REPOSITORY_2_NAME, tlogRepoPath, true)
        );

        // validate inplace repository metadata update
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        DiscoveryNode node = clusterService.localNode();
        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            REPOSITORY_NAME
        );
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> node.getAttributes().get(key)));
        Settings.Builder settings = Settings.builder();
        settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));
        settings.put("location", segmentRepoPath).put("max_remote_download_bytes_per_sec", 4, ByteSizeUnit.KB);

        assertAcked(client().admin().cluster().preparePutRepository(REPOSITORY_NAME).setType("fs").setSettings(settings).get());

        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            Repository segmentRepo = repositoriesService.repository(REPOSITORY_NAME);
            assertEquals("4096b", segmentRepo.getMetadata().settings().get("max_remote_download_bytes_per_sec"));
        }

        Map<String, Long> indexStats = indexData(5, false, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);
        restore(INDEX_NAME);
        assertBusy(() -> {
            long downloadPauseTime = 0L;
            for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
                downloadPauseTime += repositoriesService.repository(REPOSITORY_NAME).getRemoteDownloadThrottleTimeInNanos();
            }
            assertThat(downloadPauseTime, greaterThan(TimeValue.timeValueSeconds(randomIntBetween(3, 5)).nanos()));
        }, 30, TimeUnit.SECONDS);
        // Waiting for extended period for green state so that rate limit does not cause flakiness
        ensureGreen(TimeValue.timeValueSeconds(120), INDEX_NAME);
        // This is required to get updated number from already active shards which were not restored
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);
        assertEquals(0, getNumShards(INDEX_NAME).numReplicas);
        verifyRestoredData(indexStats, INDEX_NAME);

        // revert repo metadata to pass asserts on repo metadata vs. node attrs during teardown
        // https://github.com/opensearch-project/OpenSearch/pull/9569#discussion_r1345668700
        settings.remove("max_remote_download_bytes_per_sec");
        assertAcked(client().admin().cluster().preparePutRepository(REPOSITORY_NAME).setType("fs").setSettings(settings).get());
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            Repository segmentRepo = repositoriesService.repository(REPOSITORY_NAME);
            assertNull(segmentRepo.getMetadata().settings().get("max_remote_download_bytes_per_sec"));
        }
    }

    @AwaitsFix(bugUrl = "this test asserts on truncation of translog data")
    public void testRestoreIncompleteTranslog() throws IOException {
        // Create cluster and index
        prepareCluster(1, 3, INDEX_NAME, 0, 1);

        // Ingest 20 docs, as refresh interval is 300 secs, all these docs should be in translog only.
        for (int i = 0; i < 20; i++) {
            indexSingleDoc(INDEX_NAME);
        }

        // Stop primary to make the cluster red
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        // Delete latest 3 translog metadata files from remote
        // This is to simulate partial translog scenario
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(translogRepoPath), indexUUID, "/0/translog/metadata");
        try (Stream<Path> files = Files.list(indexPath)) {
            List<Path> allMetadataFiles = files.collect(Collectors.toList());
            allMetadataFiles.sort(Comparator.comparing(Path::getFileName));

            for (int i = 0; i < 3; i++) {
                Files.delete(allMetadataFiles.get(i));
            }
        }

        // On restore, index will turn green but the doc count will be less
        restore(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 20);
    }

    public void testRestoreSeqNoGapBetweenSegmentAndTranslog() throws Exception {
        // Create cluster and index
        prepareCluster(1, 3, INDEX_NAME, 0, 1);

        // Ingest 20 docs, as refresh interval is 300 secs, all these docs should be in translog only.
        Map<String, Long> indexStats = indexData(5, true, INDEX_NAME);
        for (int i = 0; i < 20; i++) {
            indexSingleDoc(INDEX_NAME);
        }

        // Stop primary to make the cluster red
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        // Delete latest 3 segment metadata files from remote
        // This is to simulate seq number gap between segments and translog
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(segmentRepoPath), indexUUID, "/0/segments/metadata");
        try (Stream<Path> files = Files.list(indexPath)) {
            List<Path> allMetadataFiles = files.collect(Collectors.toList());
            allMetadataFiles.sort(Comparator.comparing(Path::getFileName));

            for (int i = 0; i < 2; i++) {
                Files.delete(allMetadataFiles.get(i));
            }
        }

        // On restore, index will turn green but the doc count will be less
        restore(INDEX_NAME);

        // Added ensureRed to assertBusy to make sure none of the recovery attempts succeed.
        assertBusy(() -> ensureRed(INDEX_NAME));

        // Without the validation added to Translog.newSnapshot(), we get green index but following count fails
        // ensureGreen(INDEX_NAME);
        // assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 20);
    }

    // TODO: Restore flow - index aliases
}
